mod common;
use common::*;
use rusqlite::Connection;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use symbiotic_core_service::Store;

struct Generator(u64);
impl Generator {
    fn pick(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % bound as u64) as usize
    }
}

fn decisions(conn: &Connection, open_only: bool) -> Vec<Value> {
    let mut stmt = conn
        .prepare("SELECT id,state,budget_id,raised_by,envelope FROM decisions ORDER BY rowid")
        .unwrap();
    stmt.query_map([], |row| {
        Ok(json!({"id":row.get::<_,String>(0)?,"state":row.get::<_,String>(1)?,"budget_id":row.get::<_,String>(2)?,"raised_by":row.get::<_,String>(3)?,"envelope":serde_json::from_str::<Value>(&row.get::<_,String>(4)?).unwrap()}))
    }).unwrap().map(Result::unwrap).filter(|d| !open_only || d["state"] == "open").collect()
}

fn invariants(store: &mut Store, conn: &Connection) {
    let all = events(store);
    assert!(
        all.windows(2)
            .all(|w| w[0]["seq"].as_i64().unwrap() < w[1]["seq"].as_i64().unwrap()),
        "event sequence must strictly increase"
    );
    let request_count: usize = conn
        .query_row("SELECT count(*) FROM requests", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        all.iter().filter(|e| e["kind"] == "model.request").count(),
        request_count,
        "one event per request row"
    );
    let mut last_reading = BTreeMap::new();
    for event in &all {
        if event["kind"] == "budget.reading" {
            last_reading.insert(
                event["subject_id"].as_str().unwrap().to_owned(),
                event["payload"].clone(),
            );
        }
    }
    let all_decisions = decisions(conn, false);
    let mut budget_count = 0;
    for account in ["account-0", "account-1"] {
        let budgets = ok(store, "budget.get", json!({"account":account}));
        for b in budgets["budgets"].as_array().unwrap() {
            budget_count += 1;
            let spent = b["spent"].as_f64().unwrap();
            let limit = b["limit"].as_f64().unwrap();
            let expected = if spent >= limit {
                "exhausted"
            } else if spent >= 0.9 * limit {
                "warning"
            } else {
                "open"
            };
            assert_eq!(b["state"], expected, "budget {b}");
            if expected == "open" {
                assert_eq!(b["over_limit_allowed"], false, "open budget {b}");
            }
            let open = all_decisions
                .iter()
                .filter(|d| d["budget_id"] == b["id"] && d["state"] == "open")
                .collect::<Vec<_>>();
            for kind in ["budget.warning", "budget.exhausted"] {
                let count = open.iter().filter(|d| d["raised_by"] == kind).count();
                assert!(count <= 1, "multiple open {kind} decisions for {}", b["id"]);
            }
            if expected == "open" {
                assert!(open.is_empty(), "open budget has open decisions: {b}");
            }
            let last = &last_reading[b["id"].as_str().unwrap()];
            assert_eq!(
                json!([b["spent"], b["limit"], b["state"]]),
                json!([last["spent"], last["limit"], last["state"]]),
                "budget must rebuild from last reading"
            );
        }
    }
    assert_eq!(
        budget_count,
        last_reading.len(),
        "every reading has a budget row"
    );
    let mut stmt = conn
        .prepare(
            "SELECT id,waiting_for FROM jobs WHERE state IN ('ready','running','done','failed')",
        )
        .unwrap();
    for result in stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
    {
        let (job, waiting_for) = result.unwrap();
        let waiting_for: Vec<String> = serde_json::from_str(&waiting_for).unwrap();
        for id in waiting_for {
            assert!(
                all_decisions
                    .iter()
                    .any(|d| d["id"] == id && d["state"] == "answered"),
                "active or completed job {job} names unanswered decision {id}"
            );
        }
    }
}

#[test]
fn generated_operations_preserve_budget_event_and_job_invariants() {
    let seeds = match std::env::var("SEQUENCE_SEED") {
        Ok(seed) => vec![
            seed.parse::<u64>()
                .expect("SEQUENCE_SEED must be a nonzero u64"),
        ],
        Err(_) => (1..=32).collect(),
    };
    for seed in seeds {
        assert_ne!(seed, 0, "xorshift seed must be nonzero");
        let (dir, mut store) = store();
        let conn = Connection::open(dir.path().join("core.sqlite")).unwrap();
        let mut generator = Generator(seed);
        let mut requests = Vec::<Value>::new();
        let mut jobs = Vec::<String>::new();
        for step in 0..80 {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let open = decisions(&conn, true);
                let operation = match generator.pick(7) {
                    3 if requests.is_empty() => 0,
                    4 if open.is_empty() => 0,
                    6 if jobs.is_empty() => 0,
                    operation => operation,
                };
                match operation {
                    0..=2 => {
                        let mut args = record(&format!("request-{step}"), None);
                        args["account"] = json!(format!("account-{}", generator.pick(2)));
                        args["time"] = json!(
                            (chrono::DateTime::parse_from_rfc3339("2026-11-02T12:00:00Z").unwrap()
                                + chrono::Duration::seconds(step))
                            .to_rfc3339()
                        );
                        // Exercise threshold oscillation as well as the full 0..=110 range.
                        let used = if generator.pick(2) == 0 {
                            generator.pick(111)
                        } else {
                            90 + generator.pick(21)
                        };
                        let mut reading = json!({"name":format!("meter-{}", generator.pick(2)),"unit":"percent","limit":100,"read_at":args["time"]});
                        if generator.pick(2) == 0 {
                            reading["used"] = json!(used);
                        } else {
                            reading["remaining"] = json!(100_i64 - used as i64);
                        }
                        if generator.pick(5) == 0 {
                            reading.as_object_mut().unwrap().remove("limit");
                        }
                        if generator.pick(4) == 0 {
                            reading["read_at"] = json!("2026-11-01T00:00:00Z");
                        }
                        args["meter"]["readings"] = json!([reading]);
                        match generator.pick(4) {
                            0 => args["usage"] = Value::Null,
                            1 => {
                                args["usage"] = Value::Null;
                                args["refused"] = json!("quota_exhausted");
                                args["status"] = json!(402);
                            }
                            _ => {}
                        }
                        assert_eq!(
                            ok(&mut store, "request.record", args.clone())["duplicate"],
                            false
                        );
                        requests.push(args);
                    }
                    3 => {
                        let args = requests[generator.pick(requests.len())].clone();
                        let original =
                            all_request_seq(&mut store, args["request_id"].as_str().unwrap());
                        let duplicate = ok(&mut store, "request.record", args.clone());
                        assert_eq!(
                            json!([duplicate["duplicate"], duplicate["seq"]]),
                            json!([true, original])
                        );
                        let mut changed = args;
                        changed["model"] = json!("different-model");
                        assert_eq!(
                            call(&mut store, "request.record", changed)["error"]["reason"],
                            "request_id_conflict"
                        );
                    }
                    4 => {
                        let d = &open[generator.pick(open.len())];
                        let options = d["envelope"]["questions"][0]["options"].as_array().unwrap();
                        let choice = generator.pick(options.len() + 1);
                        let answer = if choice == options.len() {
                            json!({"kind":"more_context","missing":["evidence"]})
                        } else {
                            json!({"kind":"option","optionId":options[choice]["id"]})
                        };
                        let expected = if choice == options.len() {
                            "open"
                        } else {
                            "answered"
                        };
                        assert_eq!(
                            ok(
                                &mut store,
                                "decision.answer",
                                json!({"decision_id":d["id"],"revision":1,"answer":answer,"answered_by":{"id":"operator","kind":"human","displayName":"Operator"}})
                            )["decision"]["state"],
                            expected
                        );
                    }
                    5 => {
                        let existing = decisions(&conn, false);
                        let mut waiting_for = Vec::<Value>::new();
                        for _ in 0..generator.pick(3) {
                            if !existing.is_empty() {
                                waiting_for
                                    .push(existing[generator.pick(existing.len())]["id"].clone());
                            }
                        }
                        let expected = if waiting_for.iter().any(|id| {
                            existing
                                .iter()
                                .any(|d| d["id"] == *id && d["state"] != "answered")
                        }) {
                            "waiting"
                        } else {
                            "ready"
                        };
                        let id = format!("job-{step}");
                        assert_eq!(
                            ok(
                                &mut store,
                                "job.create",
                                json!({"id":id,"kind":"review","product":"test","waiting_for":waiting_for})
                            )["job"]["state"],
                            expected
                        );
                        jobs.push(id);
                    }
                    6 => {
                        let id = &jobs[generator.pick(jobs.len())];
                        let to =
                            ["ready", "running", "done", "failed", "cancelled"][generator.pick(5)];
                        let response = call(&mut store, "job.transition", json!({"id":id,"to":to}));
                        if response.get("ok").is_some() {
                            assert_eq!(response["ok"]["job"]["state"], to);
                        } else {
                            assert!(
                                [
                                    "job_state_terminal",
                                    "job_transition_invalid",
                                    "job_waiting_for_decision"
                                ]
                                .contains(&response["error"]["reason"].as_str().unwrap()),
                                "{response}"
                            );
                        }
                    }
                    _ => unreachable!("generator selects one of seven operations"),
                }
                invariants(&mut store, &conn);
            }));
            assert!(
                result.is_ok(),
                "replay with SEQUENCE_SEED={seed}; failed at step {step}"
            );
        }
    }
}

fn all_request_seq(store: &mut Store, id: &str) -> Value {
    events(store)
        .into_iter()
        .find(|e| e["kind"] == "model.request" && e["subject_id"] == id)
        .unwrap()["seq"]
        .clone()
}
