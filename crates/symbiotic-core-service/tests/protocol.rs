mod common;
use common::*;
use serde_json::{Value, json};
use symbiotic_core_service::dispatch;

#[test]
fn seeded_malformed_lines_never_panic_and_return_exactly_one_json_result() {
    let (_dir, mut store) = store();
    assert_eq!(
        ok(&mut store, "job.list", json!({})),
        json!({"jobs":[],"next_after":null})
    );
    let warden = tempfile::tempdir().unwrap();
    let mut recorded = record("request", Some(100.0));
    recorded["time"] = json!("2026-11-02T12:00:00.000Z");
    let requests = [
        json!({"call":"job.create","args":{"id":"job","kind":"review","product":"test"}}),
        json!({"call":"job.transition","args":{"id":"job","to":"running"}}),
        json!({"call":"job.heartbeat","args":{"id":"job"}}),
        json!({"call":"job.list","args":{"state":"running","limit":10}}),
        json!({"call":"request.record","args":recorded}),
        json!({"call":"budget.get","args":{"account":"account"}}),
        json!({"call":"decision.answer","args":{"decision_id":"budget-account-weekly-exhausted-1","revision":1,"answer":{"kind":"option","optionId":"keep_refusing"},"answered_by":{"id":"operator","kind":"human","displayName":"Operator"}}}),
        json!({"call":"events.since","args":{"after":0,"limit":10}}),
        json!({"call":"reconcile","args":{"day":"2026-11-02","warden_state":warden.path()}}),
    ];
    let mut seed = 0x6d61_6c66_6f72_6d65_u64;
    for index in 0..2000 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let base = &requests[index % requests.len()];
        let mut bytes = base.to_string().into_bytes();
        let position = (seed % bytes.len() as u64) as usize;
        match (index / requests.len()) % 8 {
            0 => bytes.truncate(position),
            1 => bytes[position] ^= (seed >> 32) as u8 | 1,
            2 => bytes.insert(position, (seed >> 32) as u8),
            3 => {
                bytes.remove(position);
            }
            4 => {
                let mut request = base.clone();
                let field = request["args"]
                    .as_object()
                    .unwrap()
                    .keys()
                    .nth(position % request["args"].as_object().unwrap().len())
                    .unwrap()
                    .clone();
                request["args"][field] =
                    [Value::Null, json!(false), json!(42), json!([]), json!({})]
                        [(seed % 5) as usize]
                        .clone();
                bytes = request.to_string().into_bytes();
            }
            5 => {
                let mut request = base.clone();
                request["args"]["unexpected"] = json!("extra field");
                bytes = request.to_string().into_bytes();
            }
            6 => {
                let mut request = base.clone();
                let field = request["args"]
                    .as_object()
                    .unwrap()
                    .keys()
                    .find(|key| request["args"][*key].is_string())
                    .cloned()
                    .unwrap_or_else(|| "after".into());
                request["args"][field] = json!("x".repeat(16384));
                bytes = request.to_string().into_bytes();
            }
            _ => {
                let mut nested = json!("leaf");
                for _ in 0..64 {
                    nested = json!([nested]);
                }
                let mut request = base.clone();
                request["args"] = nested;
                bytes = request.to_string().into_bytes();
            }
        }
        let line = String::from_utf8_lossy(&bytes);
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| dispatch(&mut store, &line)));
        assert!(
            result.is_ok(),
            "malformed case {index}, generator state {seed} panicked"
        );
        let response = result.unwrap();
        assert_eq!(
            response.lines().count(),
            1,
            "case {index} must return one line"
        );
        let value: Value =
            serde_json::from_str(&response).unwrap_or_else(|error| panic!("case {index}: {error}"));
        let object = value.as_object().unwrap();
        assert_eq!(object.len(), 1, "case {index}: {value}");
        assert!(
            object.contains_key("ok") ^ object.contains_key("error"),
            "case {index}: {value}"
        );
    }
}
