use crate::*;
use chrono::NaiveDate;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

fn model_job_id(directory: &Path, retirement_receipt: &Path) -> String {
    let name: String = directory
        .file_name()
        .unwrap_or_default()
        .as_encoded_bytes()
        .iter()
        .take(10)
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-' => char::from(*byte),
            _ => '_',
        })
        .collect();
    let mut hash = Sha256::new();
    hash.update(directory.as_os_str().as_encoded_bytes());
    hash.update(b"\0");
    hash.update(retirement_receipt.as_os_str().as_encoded_bytes());
    let digest = hex::encode(hash.finalize());
    format!("{name}-{}", &digest[..32])
}

fn walk(path: &Path, day: NaiveDate, jobs: &mut BTreeSet<String>, errors: &mut Vec<Value>) {
    let result = (|| -> Result<()> {
        let metadata = fs::symlink_metadata(path)?;
        if metadata.is_dir() {
            let mut entries = fs::read_dir(path)?.collect::<std::io::Result<Vec<_>>>()?;
            entries.sort_by_key(|e| e.file_name());
            for entry in entries {
                walk(&entry.path(), day, jobs, errors);
            }
        } else if metadata.is_file() && path.file_name().is_some_and(|n| n == "sandbox.json") {
            let data: Value = serde_json::from_slice(&fs::read(path)?)?;
            let id = model_job_id(
                Path::new(string(&data, "job_directory")?),
                Path::new(string(&data, "retirement_receipt")?),
            );
            let modified: DateTime<Utc> = metadata.modified()?.into();
            if modified.date_naive() == day {
                jobs.insert(id);
            }
        }
        Ok(())
    })();
    if let Err(e) = result {
        errors.push(json!({"path":path.to_string_lossy(),"reason":e.message}));
    }
}
#[derive(Default)]
struct Quota {
    account: String,
    state: String,
    warned: bool,
    allowed: bool,
}
pub(crate) fn run(conn: &Connection, args: &Value) -> Result<Value> {
    fields(args, &["day", "warden_state"])?;
    let day_text = string(args, "day")?;
    let day = NaiveDate::parse_from_str(day_text, "%Y-%m-%d").map_err(|_| invalid("day"))?;
    if day.to_string() != day_text {
        return Err(invalid("day"));
    }
    let path = Path::new(string(args, "warden_state")?);
    let mut warden = BTreeSet::new();
    let mut errors = vec![];
    walk(path, day, &mut warden, &mut errors);
    let start = format!("{day}T00:00:00.000Z");
    let end = format!(
        "{}T00:00:00.000Z",
        day.succ_opt().ok_or_else(|| invalid("day"))?
    );
    let core = rows(
        conn,
        "SELECT id FROM jobs WHERE created>=?1 AND created<?2 AND product='warden'",
        params![start, end],
    )?
    .into_iter()
    .map(|job| string(&job, "id").map(str::to_owned))
    .collect::<Result<BTreeSet<_>>>()?;
    let missing = warden.difference(&core).cloned().collect::<Vec<_>>();
    let extra = core.difference(&warden).cloned().collect::<Vec<_>>();
    let mut without = vec![];
    let mut refused = vec![];
    let mut accounts = BTreeSet::new();
    let mut total = 0;
    for request in rows(
        conn,
        "SELECT request_id,account,route,status,refused,usage IS NULL AS usage_missing FROM requests WHERE time>=?1 AND time<?2 ORDER BY request_id",
        params![start, end],
    )? {
        total += 1;
        accounts.insert(string(&request, "account")?.to_owned());
        if request["usage_missing"] == 1 {
            let mut entry = json!({"request_id":request["request_id"],"account":request["account"],"route":request["route"],"status":request["status"]});
            if request["refused"].is_null() {
                without.push(entry);
            } else {
                entry["refused"] = request["refused"].clone();
                refused.push(entry);
            }
        }
    }
    let unknown = rows(conn, "SELECT requests.request_id FROM requests LEFT JOIN jobs ON jobs.id=requests.job_id WHERE requests.time>=?1 AND requests.time<?2 AND jobs.id IS NULL ORDER BY requests.request_id", params![start, end])?
        .into_iter().map(|request| request["request_id"].clone()).collect::<Vec<_>>();
    let metered = rows(conn, "SELECT DISTINCT account FROM budgets", [])?
        .into_iter()
        .map(|v| string(&v, "account").map(str::to_owned))
        .collect::<Result<BTreeSet<_>>>()?;
    let unmetered = accounts.difference(&metered).cloned().collect::<Vec<_>>();
    let mut quotas = BTreeMap::<String, Quota>::new();
    let mut violations = vec![];
    // Quota history comes from the events; the day's sent requests come from the
    // `requests(time)` index with a keyed event lookup, so no other day's request is read and
    // no request payload (raw provider usage) is decoded. Both branches replay in `seq` order.
    let mut statement = conn.prepare(
        "SELECT seq, time, subject_id, kind, payload, NULL FROM events
         WHERE kind IN ('budget.reading', 'budget.warning', 'budget.exhausted', 'decision.answered')
         UNION ALL
         SELECT events.seq, events.time, events.subject_id, events.kind, '{}', requests.account
         FROM requests CROSS JOIN events
           ON events.subject_kind = 'request' AND events.subject_id = requests.request_id
          AND events.kind = 'model.request'
         WHERE requests.time >= ?1 AND requests.time < ?2 AND requests.refused IS NULL
         ORDER BY 1",
    )?;
    let mut events = statement.query(params![start, end])?;
    while let Some(event) = events.next()? {
        let time: String = event.get(1)?;
        let event_day = date(&time, "event.time")?.date_naive();
        let id: String = event.get(2)?;
        let kind: String = event.get(3)?;
        let payload: Value = serde_json::from_str(&event.get::<_, String>(4)?)?;
        let request_account: Option<String> = event.get(5)?;
        let p = &payload;
        match kind.as_str() {
            "budget.reading" => {
                let q = quotas.entry(id.clone()).or_default();
                q.account = string(p, "account")?.into();
                q.state = string(p, "state")?.into();
                if q.state == "open" {
                    q.allowed = false;
                    q.warned = false;
                }
            }
            "budget.warning" => {
                let q = quotas.entry(id.clone()).or_default();
                q.warned = true;
                q.state = "warning".into();
            }
            "budget.exhausted" => {
                let q = quotas.entry(id.clone()).or_default();
                if event_day == day && !q.warned {
                    violations.push(json!({"budget_id":id,"problem":"budget exhausted without a preceding warning since its last return to open"}));
                }
                q.state = "exhausted".into();
            }
            "decision.answered"
                if p["option_id"] == "allow_over_limit"
                    && p["state"] == "answered"
                    && p["raised_by"] == "budget.exhausted" =>
            {
                if let Some(q) = p["budget_id"].as_str().and_then(|id| quotas.get_mut(id))
                    && q.state == "exhausted"
                {
                    q.allowed = true;
                }
            }
            "model.request" => {
                let account = request_account
                    .ok_or_else(|| Error::new("store_error", "request event without its row"))?;
                for (budget, q) in &quotas {
                    if q.account == account && q.state == "exhausted" && !q.allowed {
                        violations.push(json!({"budget_id":budget,"request_id":id,"problem":"request sent while exhausted without an answered allow_over_limit decision"}));
                    }
                }
            }
            _ => {}
        }
    }
    let passed = missing.is_empty()
        && extra.is_empty()
        && without.is_empty()
        && unknown.is_empty()
        && violations.is_empty()
        && errors.is_empty();
    Ok(
        json!({"day":day_text,"passed":passed,"jobs":{"in_core":core.len(),"in_warden":warden.len(),"missing_from_core":missing,"extra_in_core":extra},
        "requests":{"total":total,"without_usage":without,"refused":refused,"unknown_job":unknown},"accounts_without_quota_meter":unmetered,"quota":{"violations":violations},"warden_errors":errors}),
    )
}
