# Symbiotic Foundation — Agent Instructions

Base rules: [House Rules](https://github.com/jak-pan/house-rules), its `AGENTS.md` (layout and naming in its `STRUCTURE.md`). Read and follow them first. This file holds only this repository's own rules. An override may tighten or loosen a House Rules rule, and it names the rule it changes.

Reviews: Warden, our review service, reviews pull requests on request: comment `/warden review` on the pull request.

## Project references

- [README.md](README.md): workspace purpose and crate responsibilities.
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): current crate map and implementation gaps.
- [docs/architecture/ai-runtime.md](docs/architecture/ai-runtime.md): the model-call entry point and runtime behavior.
- [docs/architecture/boundary.md](docs/architecture/boundary.md): authoritative boundary contract.

## Build and verification

[CI](.github/workflows/ci.yml) uses the pinned nightly-2026-10-25 toolchain and the **debug** profile on
pull requests and pushes to `master`. Its full test matrix covers workspace
default features and `symbiotic-model` without default features.

Local commands for a touched crate (replace the placeholders):

```sh
cargo fmt --all -- --check
cargo clippy --locked -p <crate> --all-targets -- -D warnings
cargo test --locked -p <crate> <test-filter>
```

For `symbiotic-model` feature-boundary changes, also use
`--no-default-features` in its separate targeted invocation; queue backend
conformance targets use `--features conformance` on `symbiotic-queue`.
CI owns the full matrix; local full runs follow House Rules verification exceptions.
