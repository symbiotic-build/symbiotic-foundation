//! Provider bytes through the Responses stream parser and the function-call decoder.
#![no_main]

use libfuzzer_sys::fuzz_target;
use symbiotic_model::item_stream::{ItemEvent, ResponsesAdapter};
use symbiotic_model::responses::FunctionCall;

fuzz_target!(|data: &[u8]| {
    // The first byte picks where the stream splits, so records also cross feed calls.
    let split = data.first().map_or(0, |&byte| usize::from(byte) % (data.len() + 1));
    let mut adapter = ResponsesAdapter::default();
    for chunk in [&data[..split], &data[split..]] {
        let Ok(events) = adapter.feed(chunk, 1 << 16) else {
            return;
        };
        for event in events {
            if let ItemEvent::ItemDone { item, .. } = event {
                let _ = FunctionCall::from_item(&item);
            }
        }
    }
});
