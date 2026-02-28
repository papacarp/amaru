# pooltool_edits branch: no verification

When working on the **pooltool_edits** branch in this repo:

- **Do not run** upstream or local verification on this branch:
  - Do not run `cargo test`, `make test`, or any test suite.
  - Do not run `cargo clippy`, `cargo fmt -- --check`, or other lint/format checks unless the user explicitly asks.
  - Do not suggest or run CI-style checks (simulation tests, snapshot tests, etc.).

- All edits live only on **pooltool_edits**; main stays identical to upstream. Do not run verification that would apply upstream standards to these edits.

- Only run a build (`cargo build` / `cargo run`) when the user needs to run or build the binary.
