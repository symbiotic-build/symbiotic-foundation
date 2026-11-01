//! The committed JSON schemas match the contract types.
//!
//! Refresh them with `UPDATE_SCHEMAS=1 cargo test -p symbiotic-core --features schema --test schemas`.
#![cfg(feature = "schema")]

use schemars::{JsonSchema, schema_for};
use symbiotic_core::campaign::Campaign;
use symbiotic_core::decision::DecisionEnvelope;
use symbiotic_core::event::SystemEvent;
use symbiotic_core::incident::Incident;
use symbiotic_core::signal::Signal;

fn check<T: JsonSchema>(name: &str) {
    let path = format!("{}/schemas/{name}.json", env!("CARGO_MANIFEST_DIR"));
    let generated =
        serde_json::to_string_pretty(&schema_for!(T)).expect("schema serializes") + "\n";
    if std::env::var_os("UPDATE_SCHEMAS").is_some() {
        std::fs::write(&path, &generated).expect("schema file is writable");
        return;
    }
    let committed = std::fs::read_to_string(&path).unwrap_or_default();
    assert_eq!(
        committed, generated,
        "schemas/{name}.json is stale; refresh it with UPDATE_SCHEMAS=1"
    );
}

#[test]
fn committed_schemas_match_the_types() {
    check::<DecisionEnvelope>("decision-envelope");
    check::<SystemEvent>("system-event");
    check::<Signal>("signal");
    check::<Campaign>("campaign");
    check::<Incident>("incident");
}
