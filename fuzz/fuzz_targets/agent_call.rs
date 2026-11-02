#![no_main]

use libfuzzer_sys::fuzz_target;
use serde_json::json;

thread_local! {
    // One synthetic root for every run, so the file tools have something to read.
    static ROOT: tempfile::TempDir = {
        let root = tempfile::tempdir().expect("create fuzz root");
        std::fs::write(root.path().join("file.txt"), "synthetic file\n").expect("seed fuzz root");
        root
    };
}

fuzz_target!(|data: &[u8]| {
    let Some((&selector, arguments)) = data.split_first() else {
        return;
    };
    let name = ["read", "search", "list", "note", "note_drop", "finish"][usize::from(selector) % 6];
    let schema = json!({
        "type": "object",
        "properties": {
            "unit": {"type": "string", "minLength": 1},
            "status": {"enum": ["ok", "failed"]},
            "items": {
                "type": "array", "minItems": 1, "maxItems": 3,
                "items": {
                    "type": "object",
                    "properties": {"text": {"type": "string", "minLength": 2}},
                    "required": ["text"], "additionalProperties": false
                }
            }
        },
        "required": ["unit", "status", "items"],
        "additionalProperties": false
    });
    ROOT.with(|root| {
        symbiotic_agent::fuzz_call(
            root.path(),
            name,
            &String::from_utf8_lossy(arguments),
            &schema,
        );
    });
});
