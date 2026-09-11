# Account Management Service

Account-management service written in Rust. The project uses CQRS and event
sourcing with `cqrs-es`, PostgreSQL, Axum, and Tokio. It is in an early stage:
`UserAccount` is the first domain model and the PostgreSQL event-store schema
is already available.

## Prerequisites

- [Rustup](https://rustup.rs/). The repository selects Rust `1.98.1` and
  installs `rustfmt` and `llvm-tools-preview` through `rust-toolchain.toml`.
- [`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov) for coverage:

  ```sh
  cargo install cargo-llvm-cov --locked
  ```

- Podman and Podman Compose to run PostgreSQL locally.

## Local PostgreSQL

Start the event-store database:

```sh
podman compose up -d postgres
podman compose ps
```

The Compose configuration initializes the local `account_management` database
with `deploy/postgres/001-event-store.sql`. It creates the `events` and
`snapshots` tables expected by `postgres-es`.

Stop the local service when it is no longer needed:

```sh
podman compose down
```

## Development checks

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

## Test coverage

For every Rust code or test change, run the following command. It fails when
total line coverage is below `90%`:

```sh
cargo llvm-cov --color never --fail-under-lines 90
```

Add tests until the check passes before completing the work.

## Project layout

```text
src/domain/         Business model and invariants
src/application/    Use cases and ports
src/presentation/   Inbound adapters
src/infrastructure/ Outbound adapters
src/main.rs         Dependency composition and application startup
```

Architecture and DDD conventions for contributors are defined in
[AGENTS.md](AGENTS.md) and its mandatory local skills.
