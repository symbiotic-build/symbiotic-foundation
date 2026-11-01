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
    let mut core = BTreeSet::new();
    let mut all_jobs = BTreeSet::new();
    for job in rows(conn, "SELECT id,created FROM jobs", [])? {
        let id = string(&job, "id")?.to_owned();
        if date(string(&job, "created")?, "created")?.date_naive() == day {
            core.insert(id.clone());
        }
        all_jobs.insert(id);
    }
    let missing = warden.difference(&core).cloned().collect::<Vec<_>>();
    let extra = core.difference(&warden).cloned().collect::<Vec<_>>();
    let mut without = vec![];
    let mut unknown = vec![];
    let mut accounts = BTreeSet::new();
    let mut total = 0;
    for request in rows(conn, "SELECT * FROM requests ORDER BY request_id", [])? {
        if date(string(&request, "time")?, "time")?.date_naive() != day {
            continue;
        }
        total += 1;
        accounts.insert(string(&request, "account")?.to_owned());
        if request["usage"].is_null() && request["refused"].is_null() {
            without.push(json!({"request_id":request["request_id"],"account":request["account"],"route":request["route"],"status":request["status"]}));
        }
        if !all_jobs.contains(string(&request, "job_id")?) {
            unknown.push(request["request_id"].clone());
        }
    }
    let metered = rows(conn, "SELECT DISTINCT account FROM budgets", [])?
        .into_iter()
        .map(|v| string(&v, "account").map(str::to_owned))
        .collect::<Result<BTreeSet<_>>>()?;
    let unmetered = accounts.difference(&metered).cloned().collect::<Vec<_>>();
    let mut quotas = BTreeMap::<String, Quota>::new();
    let mut violations = vec![];
    for event in rows(conn, "SELECT * FROM events ORDER BY seq", [])? {
        let event_day = date(string(&event, "time")?, "event.time")?.date_naive();
        if event_day > day {
            continue;
        }
        let p = &event["payload"];
        let id = string(&event, "subject_id")?;
        match string(&event, "kind")? {
            "budget.reading" => {
                let q = quotas.entry(id.into()).or_default();
                q.account = string(p, "account")?.into();
                q.state = string(p, "state")?.into();
                if q.state == "open" {
                    q.allowed = false;
                    q.warned = false;
                }
            }
            "budget.warning" => {
                let q = quotas.entry(id.into()).or_default();
                q.warned = true;
                q.state = "warning".into();
            }
            "budget.exhausted" => {
                let q = quotas.entry(id.into()).or_default();
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
            "model.request"
                if p.get("refused").is_none()
                    && date(string(p, "time")?, "request.time")?.date_naive() == day =>
            {
                let account = string(p, "account")?;
                for (budget, q) in &quotas {
                    if q.account == account && q.state == "exhausted" && !q.allowed {
                        violations.push(json!({"budget_id":budget,"request_id":p["request_id"],"problem":"request sent while exhausted without an answered allow_over_limit decision"}));
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
        "requests":{"total":total,"without_usage":without,"unknown_job":unknown},"accounts_without_quota_meter":unmetered,"quota":{"violations":violations},"warden_errors":errors}),
    )
}
