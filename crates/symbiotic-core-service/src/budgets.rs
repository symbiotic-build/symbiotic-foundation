use crate::*;
use symbiotic_core::decision::*;

pub(crate) fn get(conn: &Connection, account: &str) -> Result<Vec<Value>> {
    let budgets = rows(
        conn,
        "SELECT * FROM budgets WHERE account=?1 ORDER BY id",
        [account],
    )?;
    budgets
        .into_iter()
        .map(|b| {
            let ids = rows(
                conn,
                "SELECT id FROM decisions WHERE budget_id=?1 AND state='open' ORDER BY rowid",
                [string(&b, "id")?],
            )?
            .into_iter()
            .map(|v| v["id"].clone())
            .collect::<Vec<_>>();
            Ok(json!({
                "id": b["id"], "account": b["account"], "name": b["name"], "unit": b["unit"],
                "limit": b["limit"], "spent": b["spent"], "warn_at": b["warn_at"], "state": b["state"],
                "over_limit_allowed": b["over_limit_allowed"] == 1, "open_decision_ids": ids,
                "reading": b["meter"], "updated": b["updated"],
            }))
        })
        .collect()
}
pub(crate) fn apply(conn: &Connection, account: &str, reading: &Value, time: &str) -> Result<()> {
    let Some(limit) = reading["limit"].as_f64() else {
        return Ok(());
    };
    let spent = match (reading["used"].as_f64(), reading["remaining"].as_f64()) {
        (Some(x), _) => x,
        (None, Some(x)) => limit - x,
        _ => return Ok(()),
    };
    if !spent.is_finite() {
        return Err(invalid("meter.readings.remaining"));
    }
    let name = string(reading, "name")?;
    let id = format!("{account}/{name}");
    let read_at = reading["read_at"].as_str().unwrap_or(time);
    let old = one(conn, "budgets", &id)?;
    if let Some(old) = &old
        && date(read_at, "read_at")? < date(string(old, "read_at")?, "read_at")?
    {
        return Ok(());
    }
    let old_state = old
        .as_ref()
        .and_then(|v| v["state"].as_str())
        .unwrap_or("open");
    let warn_at = old
        .as_ref()
        .and_then(|v| v["warn_at"].as_f64())
        .unwrap_or(0.9);
    let state = if spent >= limit {
        "exhausted"
    } else if spent >= warn_at * limit {
        "warning"
    } else {
        "open"
    };
    let allowed = state != "open" && old.as_ref().is_some_and(|v| v["over_limit_allowed"] == 1);
    let mut normalized = reading.clone();
    normalized["read_at"] = json!(read_at);
    conn.execute("INSERT INTO budgets(id,account,name,unit,\"limit\",spent,meter,state,warn_at,over_limit_allowed,read_at,updated) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?11) ON CONFLICT(id) DO UPDATE SET unit=excluded.unit,\"limit\"=excluded.\"limit\",spent=excluded.spent,meter=excluded.meter,state=excluded.state,over_limit_allowed=excluded.over_limit_allowed,read_at=excluded.read_at,updated=excluded.updated", params![id,account,name,string(reading,"unit")?,limit,spent,normalized.to_string(),state,warn_at,allowed,read_at])?;
    let payload = json!({"budget_id":id,"account":account,"reading":normalized,"spent":spent,"limit":limit,"state":state});
    append(conn, "budget", &id, "budget.reading", &payload)?;
    if old_state == "open" && state != "open" {
        let (_, event) = append(
            conn,
            "budget",
            &id,
            "budget.warning",
            &json!({"budget_id":id,"account":account,"state":"warning","spent":spent,"limit":limit}),
        )?;
        open_decision(conn, &id, "warning", event)?;
    }
    if old_state != "exhausted" && state == "exhausted" {
        let (_, event) = append(
            conn,
            "budget",
            &id,
            "budget.exhausted",
            &json!({"budget_id":id,"account":account,"state":"exhausted","spent":spent,"limit":limit}),
        )?;
        open_decision(conn, &id, "exhausted", event)?;
    }
    if old_state != "open" && state == "open" {
        for mut decision in rows(
            conn,
            "SELECT * FROM decisions WHERE budget_id=?1 AND state='open' ORDER BY rowid",
            [&id],
        )? {
            let mut envelope: DecisionEnvelope =
                serde_json::from_value(decision["envelope"].clone())?;
            envelope.state = DecisionState::Withdrawn;
            decision["state"] = json!("withdrawn");
            decision["envelope"] = serde_json::to_value(&envelope)?;
            conn.execute(
                "UPDATE decisions SET state='withdrawn',envelope=?1 WHERE id=?2",
                params![decision["envelope"].to_string(), envelope.id.0],
            )?;
            append(
                conn,
                "decision",
                &envelope.id.0,
                "decision.withdrawn",
                &decision,
            )?;
        }
    }
    Ok(())
}
fn open_decision(conn: &Connection, budget_id: &str, level: &str, event: EventId) -> Result<()> {
    let kind = format!("budget.{level}");
    let already_open: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM decisions WHERE budget_id=?1 AND state='open' AND raised_by=?2)",
        params![budget_id, kind],
        |row| row.get(0),
    )?;
    if already_open {
        return Ok(());
    }
    let b = one(conn, "budgets", budget_id)?
        .ok_or_else(|| Error::new("store_error", "budget disappeared"))?;
    let n: i64 = conn.query_row(
        "SELECT count(*)+1 FROM decisions WHERE budget_id=?1 AND raised_by=?2",
        params![budget_id, kind],
        |r| r.get(0),
    )?;
    let id = format!(
        "budget-{}-{}-{level}-{n}",
        string(&b, "account")?,
        string(&b, "name")?
    );
    let exhausted = level == "exhausted";
    let options = if exhausted {
        vec![
            (
                "allow_over_limit",
                "Allow requests past the limit",
                "The gateway sends requests past the limit. On Codex this spends credits, a protected asset.",
                false,
            ),
            (
                "keep_refusing",
                "Keep refusing requests",
                "The gateway refuses requests until the budget resets or you allow requests past the limit.",
                true,
            ),
        ]
    } else {
        vec![
            (
                "continue_to_limit",
                "Continue to the limit",
                "The gateway may keep sending requests until 100 percent.",
                true,
            ),
            (
                "pause_new_jobs",
                "Pause new jobs",
                "Start no new job on this account.",
                true,
            ),
            (
                "move_to_other_route",
                "Move to another route",
                "Send new jobs to another route.",
                true,
            ),
        ]
    };
    let mut context = vec![
        format!(
            "Account {} has meter {}.",
            string(&b, "account")?,
            string(&b, "name")?
        ),
        format!(
            "Spent is {} of {} {}.",
            b["spent"],
            b["limit"],
            string(&b, "unit")?
        ),
        format!("The reading was taken at {}.", string(&b, "read_at")?),
    ];
    if let Some(reset) = b["meter"]["resets_at"].as_str() {
        context.push(format!("The meter resets at {reset}."));
    }
    let (prompt, after_answer, recommended, cost_of_delay) = if exhausted {
        (
            "Allow requests past this budget's limit?",
            "Allowing requests past the limit enables the gateway while this budget remains exhausted.",
            "keep_refusing",
            "Requests remain refused.",
        )
    } else {
        (
            "How should new jobs use this account before its limit?",
            "The core records your answer. The core makes no scheduling change.",
            "continue_to_limit",
            "The account may reach its limit.",
        )
    };
    let question = DecisionQuestion {
        id: "1".into(),
        topic: "Budget".into(),
        kind: QuestionKind::Authority,
        prompt: prompt.into(),
        context: context
            .into_iter()
            .map(|text| ContextSentence {
                label: ContextLabel::Fact,
                text,
            })
            .collect(),
        after_answer: Some(after_answer.into()),
        why_now: format!("The meter reached the {level} threshold."),
        evidence_refs: vec![],
        answer_format: AnswerFormat::Option,
        options: options
            .into_iter()
            .map(|(id, label, consequence, reversible)| DecisionOption {
                id: id.into(),
                label: label.into(),
                consequence: consequence.into(),
                reversible,
            })
            .collect(),
        recommended_option_id: Some(recommended.into()),
        confidence: None,
        severity: QuestionSeverity::Decision,
        safe_default: None,
        cost_of_delay: cost_of_delay.into(),
        resolution_trail: ResolutionTrail {
            attempts: vec![],
            escalation_reason: Some("a budget rule raises this decision directly".into()),
        },
        answer: None,
    };
    let envelope = DecisionEnvelope {
        id: DecisionId(id.clone()),
        revision: 1,
        company_id: None,
        campaign_id: None,
        work_item_id: None,
        unblock_key: None,
        questions: vec![question],
        source_events: vec![event],
        audience: vec![ActorRef {
            id: "operator".into(),
            kind: ActorKind::Human,
            display_name: "Operator".into(),
        }],
        wakes: vec![],
        decision_by: None,
        created_at: now(),
        state: DecisionState::Open,
    };
    conn.execute(
        "INSERT INTO decisions(id,envelope,state,budget_id,raised_by) VALUES (?1,?2,'open',?3,?4)",
        params![id, serde_json::to_string(&envelope)?, budget_id, kind],
    )?;
    let value = one(conn, "decisions", &id)?
        .ok_or_else(|| Error::new("store_error", "decision disappeared"))?;
    append(conn, "decision", &id, "decision.opened", &value)?;
    Ok(())
}
pub(crate) fn answer(conn: &Connection, args: &Value) -> Result<Value> {
    fields(args, &["decision_id", "revision", "answer", "answered_by"])?;
    let id = string(args, "decision_id")?;
    let revision = args["revision"]
        .as_u64()
        .and_then(|x| u32::try_from(x).ok())
        .ok_or_else(|| invalid("revision"))?;
    let answer: Answer = serde_json::from_value(args["answer"].clone())
        .map_err(|e| Error::new("args_invalid", format!("answer: {e}")))?;
    let actor: ActorRef = serde_json::from_value(args["answered_by"].clone())
        .map_err(|e| Error::new("args_invalid", format!("answered_by: {e}")))?;
    let mut value = one(conn, "decisions", id)?.ok_or_else(|| {
        Error::new(
            "decision_not_found",
            format!("decision {id} does not exist"),
        )
    })?;
    let mut envelope: DecisionEnvelope = serde_json::from_value(value["envelope"].clone())?;
    if envelope.state != DecisionState::Open {
        return Err(Error::new(
            "decision_not_open",
            format!("decision {id} is not open"),
        ));
    }
    if envelope.revision != revision {
        return Err(Error::new(
            "decision_revision_stale",
            format!("revision {revision} does not match {}", envelope.revision),
        ));
    }
    let question = envelope
        .questions
        .first_mut()
        .ok_or_else(|| Error::new("store_error", "decision has no question"))?;
    let option = match &answer {
        Answer::Option { option_id } => {
            if !question.options.iter().any(|o| o.id == *option_id) {
                return Err(Error::new(
                    "answer_option_unknown",
                    format!("option {option_id} is unknown"),
                ));
            }
            Some(option_id.clone())
        }
        Answer::MoreContext { .. } => None,
        _ => return Err(invalid("answer.kind")),
    };
    let time = now();
    question.answer = Some(AnswerRecord {
        revision,
        answer: answer.clone(),
        answered_by: actor.clone(),
        answered_at: time.clone(),
    });
    if option.is_some() {
        envelope.state = DecisionState::Answered;
    }
    let state = if option.is_some() { "answered" } else { "open" };
    conn.execute("UPDATE decisions SET state=?1,envelope=?2,answer=?3,answered_by=?4,answered_at=?5 WHERE id=?6",params![state,serde_json::to_string(&envelope)?,serde_json::to_string(&answer)?,serde_json::to_string(&actor)?,time.0,id])?;
    if option.as_deref() == Some("allow_over_limit") && value["raised_by"] == "budget.exhausted" {
        conn.execute(
            "UPDATE budgets SET over_limit_allowed=1 WHERE id=?1 AND state='exhausted'",
            [string(&value, "budget_id")?],
        )?;
    }
    value = one(conn, "decisions", id)?
        .ok_or_else(|| Error::new("store_error", "decision disappeared"))?;
    let mut payload = value.clone();
    payload["option_id"] = json!(option);
    let (seq, _) = append(conn, "decision", id, "decision.answered", &payload)?;
    value
        .as_object_mut()
        .ok_or_else(|| Error::new("store_error", "invalid decision"))?
        .remove("raised_by");
    Ok(json!({"seq":seq,"decision":value}))
}
