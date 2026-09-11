# Project

Rust account-management CQRS/ES playground using PostgreSQL, Tokio, and Axum. Preserve the `domain`, `application`, `presentation`, and `infrastructure` boundaries defined by `$clean-architecture`.

## Project Idioms

- Use Rust 2024 modules without `mod.rs`; keep submodules in `<module>/` beside `<module>.rs`.
- Keep domain fields private, enforce invariants in constructors, and model meaningful primitives as value objects.
- Pass time and other nondeterministic inputs into domain code; keep framework and I/O types outside `domain`.
- Prefer typed errors, idiomatic naming, and tests next to the behavior they cover.

## Definition of Done

- The change respects `$clean-architecture` boundaries and includes tests for changed behavior.
- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test` pass.
- Infrastructure configuration is validated when changed; use `podman compose config` for `compose.yaml`.
- Documentation and agent guidance are updated when conventions or behavior change.
