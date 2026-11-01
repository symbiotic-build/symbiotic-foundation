# Fuzz targets

Coverage-guided fuzzing of entry points that parse untrusted input, with
[cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) on the pinned nightly toolchain.
A crash becomes a regression test in the crate that owns the parser.

- `responses_stream`: provider bytes through `symbiotic_model::item_stream::ResponsesAdapter`
  and `symbiotic_model::responses::FunctionCall::from_item`.

Run one target for a bounded time, seeded with the recorded Codex answer:

```sh
mkdir -p fuzz/corpus/responses_stream
cp crates/symbiotic-model/src/responses_tests/check_answer.sse fuzz/corpus/responses_stream/
cargo fuzz run --release responses_stream fuzz/corpus/responses_stream -- -max_total_time=300
```
