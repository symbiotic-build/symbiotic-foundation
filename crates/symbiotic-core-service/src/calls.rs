use crate::*;

fn job(conn: &Connection, id: &str) -> Result<Value> {
    let mut job = one(conn, "jobs", id)?
        .ok_or_else(|| Error::new("job_not_found", format!("job {id} does not exist")))?;
    let requests = rows(conn, "SELECT usage FROM requests WHERE job_id=?1", [id])?;
    let mut cost = serde_json::Map::new();
    let mut without = 0u64;
    for request in &requests {
        if request["usage"].is_null() {
            without += 1;
        } else if let Some(normalized) = request["usage"]["normalized"].as_object() {
            for (key, value) in normalized {
                let count = value
                    .as_u64()
                    .ok_or_else(|| Error::new("store_error", "invalid stored usage"))?;
                let sum = cost
                    .get(key)
                    .and_then(Value::as_u64)
                    .unwrap_or(0)
                    .checked_add(count)
                    .ok_or_else(|| {
                        Error::new("cost_overflow", format!("job cost {key} exceeds u64"))
                    })?;
                cost.insert(key.clone(), json!(sum));
            }
        }
    }
    cost.insert("requests".into(), json!(requests.len()));
    cost.insert("requests_without_usage".into(), json!(without));
    job["cost"] = Value::Object(cost);
    Ok(job)
}
fn job_event(conn: &Connection, id: &str, kind: &str, job: &Value) -> Result<i64> {
    // Derived cost and liveness are outside the canonical job record.
    let mut payload = job.clone();
    let record = payload
        .as_object_mut()
        .ok_or_else(|| Error::new("store_error", "invalid job record"))?;
    record.remove("cost");
    record.remove("heartbeat_at");
    Ok(append(conn, "job", id, kind, &payload)?.0)
}

fn waiting(conn: &Connection, ids: &[String]) -> Result<bool> {
    let mut pending = false;
    for id in ids {
        let decision = one(conn, "decisions", id)?.ok_or_else(|| {
            Error::new(
                "decision_not_found",
                format!("decision {id} does not exist"),
            )
        })?;
        pending |= decision["state"] != "answered";
    }
    Ok(pending)
}
pub(crate) fn job_create(conn: &Connection, args: &Value) -> Result<Value> {
    fields(
        args,
        &[
            "id",
            "kind",
            "product",
            "inputs",
            "rule_revision",
            "model_route",
            "budget_id",
            "waiting_for",
        ],
    )?;
    let id = string(args, "id")?;
    let kind = string(args, "kind")?;
    let product = string(args, "product")?;
    let inputs = strings(args, "inputs")?;
    let decisions = strings(args, "waiting_for")?;
    for key in ["rule_revision", "model_route", "budget_id"] {
        optional_string(args, key)?;
    }
    // Compare create inputs to the immutable creation record, even after transitions.
    let canonical = json!({"id":id,"kind":kind,"product":product,"inputs":inputs,
        "rule_revision":args["rule_revision"],"model_route":args["model_route"],"budget_id":args["budget_id"],"waiting_for":decisions});
    if let Some((seq, original)) = event_for(conn, "job", "job.created", id)? {
        if canonical
            .as_object()
            .is_some_and(|map| map.iter().all(|(k, v)| original.get(k) == Some(v)))
        {
            return Ok(json!({"duplicate":true,"seq":seq,"job":job(conn,id)?}));
        }
        return Err(Error::new(
            "job_id_conflict",
            format!("job {id} already has different create args"),
        ));
    }
    let state = if waiting(conn, &decisions)? {
        "waiting"
    } else {
        "ready"
    };
    let time = now();
    conn.execute(
        "INSERT INTO jobs (id,kind,product,inputs,rule_revision,model_route,budget_id,state,waiting_for,result_ref,created,updated) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,NULL,?10,?10)",
        params![
            id,
            kind,
            product,
            json!(inputs).to_string(),
            args["rule_revision"].as_str(),
            args["model_route"].as_str(),
            args["budget_id"].as_str(),
            state,
            json!(decisions).to_string(),
            time.0
        ],
    )?;
    for decision in decisions {
        conn.execute(
            "INSERT OR IGNORE INTO job_decisions VALUES (?1,?2)",
            params![id, decision],
        )?;
    }
    let value = job(conn, id)?;
    let seq = job_event(conn, id, "job.created", &value)?;
    Ok(json!({"duplicate":false,"seq":seq,"job":value}))
}
pub(crate) fn job_transition(conn: &Connection, args: &Value) -> Result<Value> {
    fields(args, &["id", "to", "result_ref"])?;
    let id = string(args, "id")?;
    let to = string(args, "to")?;
    optional_string(args, "result_ref")?;
    if !["ready", "running", "done", "failed", "cancelled"].contains(&to) {
        return Err(invalid("to"));
    }
    let old = job(conn, id)?;
    let from = string(&old, "state")?;
    if ["done", "failed", "cancelled"].contains(&from) {
        return Err(Error::new(
            "job_state_terminal",
            format!("job {id} is {from}"),
        ));
    }
    if from == to {
        return Ok(json!({"duplicate":true,"seq":null,"job":old}));
    }
    if (to == "ready" && from != "waiting")
        || (["done", "failed"].contains(&to) && from != "running")
        || (to == "running" && !["waiting", "ready"].contains(&from))
    {
        return Err(Error::new(
            "job_transition_invalid",
            format!("job {id} cannot move from {from} to {to}"),
        ));
    }
    if ["ready", "running"].contains(&to) && waiting(conn, &strings(&old, "waiting_for")?)? {
        return Err(Error::new(
            "job_waiting_for_decision",
            format!("job {id} has an unanswered decision"),
        ));
    }
    conn.execute(
        "UPDATE jobs SET state=?1,result_ref=COALESCE(?2,result_ref),updated=?3 WHERE id=?4",
        params![to, args["result_ref"].as_str(), now().0, id],
    )?;
    let kind = match to {
        "ready" => "job.ready",
        "running" => "job.started",
        "done" => "job.finished",
        "failed" => "job.failed",
        _ => "job.cancelled",
    };
    let value = job(conn, id)?;
    let seq = job_event(conn, id, kind, &value)?;
    Ok(json!({"duplicate":false,"seq":seq,"job":value}))
}
pub(crate) fn job_heartbeat(conn: &Connection, args: &Value) -> Result<Value> {
    fields(args, &["id"])?;
    let id = string(args, "id")?;
    let state: String = conn
        .query_row("SELECT state FROM jobs WHERE id=?1", [id], |row| row.get(0))
        .optional()?
        .ok_or_else(|| Error::new("job_not_found", format!("job {id} does not exist")))?;
    if ["done", "failed", "cancelled"].contains(&state.as_str()) {
        return Err(Error::new(
            "job_state_terminal",
            format!("job {id} is {state}"),
        ));
    }
    if state != "running" {
        return Err(Error::new(
            "job_not_running",
            format!("job {id} is {state}"),
        ));
    }
    let time = now();
    conn.execute(
        "UPDATE jobs SET heartbeat_at=?1 WHERE id=?2",
        params![time.0, id],
    )?;
    Ok(json!({"id":id,"heartbeat_at":time.0}))
}
pub(crate) fn job_list(conn: &Connection, args: &Value) -> Result<Value> {
    fields(args, &["product", "state", "after", "limit"])?;
    for field in ["product", "state", "after"] {
        optional_string(args, field)?;
    }
    if args.get("state").is_some()
        && !["waiting", "ready", "running", "done", "failed", "cancelled"]
            .contains(&string(args, "state")?)
    {
        return Err(invalid("state"));
    }
    let limit = page_limit(args)?;
    let mut jobs = rows(
        conn,
        "SELECT * FROM jobs WHERE (?1 IS NULL OR product=?1) AND (?2 IS NULL OR state=?2) AND (?3 IS NULL OR id>?3) ORDER BY id LIMIT ?4",
        params![
            args["product"].as_str(),
            args["state"].as_str(),
            args["after"].as_str(),
            limit + 1
        ],
    )?;
    let next_after = if jobs.len() > limit as usize {
        jobs.pop();
        jobs.last()
            .map(|job| job["id"].clone())
            .unwrap_or(Value::Null)
    } else {
        Value::Null
    };
    Ok(json!({"jobs":jobs,"next_after":next_after}))
}
fn page_limit(args: &Value) -> Result<i64> {
    match args.get("limit") {
        None => Ok(1000),
        Some(v) => v
            .as_i64()
            .filter(|x| (0..=10000).contains(x))
            .ok_or_else(|| invalid("limit")),
    }
}
fn readings(meter: &Value) -> Result<&[Value]> {
    match meter.get("readings") {
        None => Ok(&[]),
        Some(value) => value
            .as_array()
            .map(Vec::as_slice)
            .ok_or_else(|| invalid("meter.readings")),
    }
}
fn validate_request(args: &Value) -> Result<()> {
    fields(
        args,
        &[
            "request_id",
            "job_id",
            "account",
            "route",
            "model",
            "status",
            "time",
            "usage",
            "meter",
            "refused",
        ],
    )?;
    for key in ["request_id", "job_id", "account", "route", "model", "time"] {
        string(args, key)?;
    }
    date(string(args, "time")?, "time")?;
    args["status"].as_i64().ok_or_else(|| invalid("status"))?;
    optional_string(args, "refused")?;
    let usage = args.get("usage").ok_or_else(|| invalid("usage"))?;
    if !usage.is_null() {
        fields(usage, &["normalized", "provider"])?;
        let normalized = usage["normalized"]
            .as_object()
            .ok_or_else(|| invalid("usage.normalized"))?;
        if !usage["provider"].is_object() {
            return Err(invalid("usage.provider"));
        }
        for (k, v) in normalized {
            if ![
                "input_tokens",
                "cached_input_tokens",
                "cache_write_tokens",
                "output_tokens",
                "reasoning_tokens",
            ]
            .contains(&k.as_str())
                || v.as_u64().is_none()
            {
                return Err(invalid(&format!("usage.normalized.{k}")));
            }
        }
    }
    let meter = args.get("meter").ok_or_else(|| invalid("meter"))?;
    fields(meter, &["readings", "provider"])?;
    let readings = readings(meter)?;
    if let Some(provider) = meter.get("provider")
        && !provider
            .as_object()
            .is_some_and(|p| p.values().all(Value::is_string))
    {
        return Err(invalid("meter.provider"));
    }
    for reading in readings {
        fields(
            reading,
            &[
                "name",
                "unit",
                "used",
                "remaining",
                "limit",
                "window_minutes",
                "resets_at",
                "read_at",
            ],
        )?;
        for key in ["name", "unit"] {
            string(reading, key).map_err(|_| invalid(&format!("meter.readings.{key}")))?;
        }
        for key in ["used", "remaining", "limit"] {
            if let Some(v) = reading.get(key)
                && v.as_f64().is_none_or(|n| !n.is_finite())
            {
                return Err(invalid(&format!("meter.readings.{key}")));
            }
        }
        if reading
            .get("limit")
            .is_some_and(|v| v.as_f64().is_none_or(|n| n <= 0.0))
        {
            return Err(invalid("meter.readings.limit"));
        }
        if let Some(v) = reading.get("window_minutes")
            && v.as_u64().is_none()
        {
            return Err(invalid("meter.readings.window_minutes"));
        }
        for key in ["read_at", "resets_at"] {
            if reading.get(key).is_some() {
                date(string(reading, key)?, key)?;
            }
        }
    }
    Ok(())
}
pub(crate) fn request_record(conn: &Connection, args: &Value) -> Result<Value> {
    validate_request(args)?;
    let id = string(args, "request_id")?;
    let account = string(args, "account")?;
    if let Some((seq, original)) = event_for(conn, "request", "model.request", id)? {
        if original != *args {
            return Err(Error::new(
                "request_id_conflict",
                format!("request {id} is already recorded with a different body"),
            ));
        }
        return Ok(json!({"duplicate":true,"seq":seq,"budgets":budgets::get(conn,account)?}));
    }
    conn.execute(
        "INSERT INTO requests VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        params![
            id,
            string(args, "job_id")?,
            account,
            string(args, "route")?,
            string(args, "model")?,
            args["status"].as_i64(),
            args["refused"].as_str(),
            if args["usage"].is_null() {
                None
            } else {
                Some(args["usage"].to_string())
            },
            args["meter"].to_string(),
            date(string(args, "time")?, "time")?.to_rfc3339_opts(SecondsFormat::Millis, true)
        ],
    )?;
    let (seq, _) = append(conn, "request", id, "model.request", args)?;
    for reading in readings(&args["meter"])? {
        budgets::apply(conn, account, reading, string(args, "time")?)?;
    }
    Ok(json!({"duplicate":false,"seq":seq,"budgets":budgets::get(conn,account)?}))
}
pub(crate) fn events_since(conn: &Connection, args: &Value) -> Result<Value> {
    fields(args, &["after", "limit"])?;
    let after = match args.get("after") {
        None => 0,
        Some(v) => v
            .as_i64()
            .filter(|x| *x >= 0)
            .ok_or_else(|| invalid("after"))?,
    };
    let limit = page_limit(args)?;
    let events = rows(
        conn,
        "SELECT * FROM events WHERE seq>?1 ORDER BY seq LIMIT ?2",
        params![after, limit],
    )?;
    let last = events
        .last()
        .and_then(|v| v["seq"].as_i64())
        .unwrap_or(after);
    Ok(json!({"events":events,"last_seq":last}))
}
