# Account Management Service

Rust 2024 workspace using CQRS/event sourcing with `cqrs-es`, PostgreSQL,
Axum, and Tokio. The workspace contains two sibling crates:
`account-managements-service` owns the business system and `cqrs-runtime`
owns reusable CQRS/outbox infrastructure. Canonical PostgreSQL schemas live
under `cqrs-runtime/sql/`.

## Mandatory instructions

Before running a shell command, read `/state/home/.codex/RTK.md` and prefix the
command with `rtk`.

Load the following mandatory repository-local skills before any coding,
dependency-wiring, or infrastructure-configuration task:

@.codex/skills/clean-architecture/SKILL.md
@.codex/skills/ddd-directory-layout/SKILL.md

The skills are the source of truth for architecture boundaries, CQRS/ES, and
DDD directory rules. Update the applicable skill when changing a lasting
convention.

## Project map

```text
account-managements-service/
  src/domain/         Business model
  src/application/    Use cases and ports
  src/presentation/   Inbound adapters
  src/infrastructure/ Service-specific outbound adapters
  src/startup/        Executable composition and service resource ownership
  src/main.rs         Configuration → Service::prepare → Service::run entry point
cqrs-runtime/         Generic command, outbox, PostgreSQL, and worker runtime
```

## Verification

- Required Rust version: `1.98.1` (`rust-toolchain.toml`).
- For code changes: `cargo fmt --all --check`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings`, and
  `cargo test --workspace --all-features`.
- After every change to Rust production code or tests, run
  `cargo llvm-cov --workspace --all-features --color never --ignore-filename-regex '(^|/)main\.rs$'
  --fail-under-lines 90`. The minimal entry point
  `account-managements-service/src/main.rs` is intentionally excluded from the
  report; all other production code remains in scope. The
  command fails automatically when total line coverage is below `90%`; add
  tests until it passes. If the report cannot be generated, state the exact
  environmental blocker in the handoff.
- For `compose.yaml` changes: also run `podman compose config`.
- Report an environmental blocker precisely; do not weaken these requirements.
- When extending or changing service configuration, update
  `account-managements-service/.env.example` with every supported variable and
  a safe local value. Update the workspace-root `.env` as needed for local
  manual runs; keep this local file uncommitted.
