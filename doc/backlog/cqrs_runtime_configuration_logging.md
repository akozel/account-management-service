# Этап 5: конфигурация, логгер и безопасные diagnostics

Связанное [review](../reviews/cqrs_runtime_reuse.md#этап-5-конфигурация-logging-диагностика-и-небольшие-helpers).
Статус: выполнено 2026-10-11; реализация и все проверки завершены.

## Цель и согласованные границы

Собрать startup-конфигурацию сервиса, перенести настройку логгера в runtime,
заменить panic-based подключение к PostgreSQL и улучшить безопасные diagnostics.
По решению пользователя подключается только logger на существующих tracing и
tracing-subscriber. Feature/module telemetry, metrics, distributed tracing и
exporters отложены.

Принятые рекомендованные defaults: BIND_ADDRESS — numeric IPv4/IPv6 + port,
валидируемый SocketAddr; отсутствие .env допустимо, ошибки чтения/синтаксиса
возвращаются. Pool limit остается 10. Новых env variables и миграций нет.
Retry ownership, CommandGatewayError categories, budgets и shutdown policy
сохраняются. EnvFilter, pool tuning, console sender и replay-stream fix вне этапа.

## ServiceConfig и startup

Приватный binary config модуль владеет ServiceConfig с закрытыми полями/getters:
PgConnectOptions, SocketAddr, LoggingConfig, OutboxRuntimeConfig и
PostgresOutboxQueueConfig. from_env snapshots 10 поддерживаемых keys, отклоняет
non-Unicode значения безопасной ошибкой с именем настройки, затем делегирует
from_lookup. DATABASE_URL и BIND_ADDRESS обязательны. URL scheme postgres:// или
postgresql:// проверяется до SQLx parsing. Парсинг options выполняется до logger
installation и DB connection; стандартное SQLx поведение PG environment/pgpass
сохранено. Вложенные configs сохраняют defaults, units, ranges и lease validation.

Dotenv loading остается действием executable: только not-found игнорируется.
Controlled loader tests не меняют environment. Config/startup errors typed:
dotenv, configuration, logging, database, outbox preparation, HTTP bind, Host.
Original errors сохраняются как sources. Display/Debug/logging скрывают URL,
пароль/PgConnectOptions, dotenv lines и arbitrary SQLx/I/O source text.
Host diagnostics сохраняют исходную безопасную обработку JoinError.

Порядок: dotenv → полная config validation → logger → DB → business wiring →
outbox preparation → HTTP bind → host → pool close → logging/return результата.
Startup expect заменяются error propagation; проверенные constant defaults могут
сохранить expect. После connection вся оставшаяся работа находится внутри
with_pool_cleanup(&PgPool, work_future): await work, pool.close().await, original
result. Preparation/bind/host errors также закрывают pool до reporting.
Normal await возвращается после cleanup; Drop/abort не гарантирует DB cleanup.
Новых host/pool deadlines нет. Main сохраняет явные бизнес-зависимости и HTTP wiring.

## Runtime APIs и features

Optional feature `logging = ["dep:tracing-subscriber"]`; default features остаются
postgres и worker. Сервис явно включает host + logging. Tracing — base dependency
для структурированных events без установки subscriber; tracing-subscriber
optional для logger setup и dev dependency тестов.

```rust
impl LoggingConfig {
    pub fn new(level_filter: LevelFilter) -> Self;
    pub fn from_lookup(
        lookup: impl FnMut(&str) -> Option<String>,
    ) -> Result<Self, ParseLevelFilterError>;
    pub fn from_env() -> Result<Self, ParseLevelFilterError>;
    pub fn level_filter(&self) -> LevelFilter;
    pub fn try_init(&self) -> Result<(), LoggingInitError>;
}
```

Default INFO. New/default/read config не устанавливают logger. Сохранить весь
LevelFilter::from_str: off/error/warn/info/debug/trace, case-insensitive names,
0–5 aliases и empty input ERROR. Missing RUST_LOG = INFO; directives rejected.
Try_init сохраняет fmt setup: target false, thread names true, остальные defaults
и log bridge. Duplicate subscriber/logger дает LoggingInitError; TryInitError
сохраняется как source и не форматируется автоматически небезопасным способом.

```rust
pub async fn connect_pool(options: PgConnectOptions) -> Result<PgPool, sqlx::Error>;
```

Helper использует PgPoolOptions::new().max_connections(10).connect_with(options).
Остальные SQLx defaults прежние; входные options готовы, dotenv/parsing/retry
policy helper не добавляет. Source SQLx error возвращается unchanged.
Удалить service LogingConfig без compatibility wrapper; удалить production
postgres-es/tracing-subscriber dependencies; SQLx перенести из dev в production.
Централизация versions остается этапу 6.

## Diagnostics и private helpers

Command executor логирует terminal technical failure до преобразования в
Unavailable: operation execute, sanitized aggregate type, framework category
connection/deserialization/unexpected и доступную SQLx classification через source
chain. Domain rejection и optimistic conflict не являются technical error events;
число retries и mapping сохраняются.

PostgreSQL pool/queue фиксируют SQLx cause до erasure: operations connect/claim/
settle/recover_expired. Fixed categories: io, tls, database, pool_timeout,
pool_closed, decode, configuration, other. Только валидный пятисимвольный SQLSTATE
из uppercase ASCII letters/digits допускается в logs. Aggregate type допускает
ASCII letters/digits/underscore, иначе unknown в diagnostics без изменения IDs.
Не форматировать command, task payload/metadata, aggregate ID/email, URL,
raw error text или database message/detail/query/constraint. Worker summaries
сохраняются отдельно от source diagnostics; public errors unchanged.

Private read_u64 объединяет повторяющийся integer parsing. Units, defaults,
validation, error variants и existing parsing semantics принадлежат configs.
Публичных utils, объединения backoff или новых retries нет.

## Проверки и приемка

- Config: required settings; IPv4/IPv6; invalid URI/address; defaults; Unicode,
  integer boundaries, lease budget; safe formatting и сохраненные sources.
- Dotenv controlled success/missing/read/syntax failures без env mutation.
- Logger parser matrix и explicit/duplicate installation в isolated integration
  executable; constructors не меняют global state.
- PostgreSQL: connection, max 10, original authentication error без panic;
  persisted suites и прежние rollback/lease semantics.
- Cleanup: success и preparation/bind/host errors; pool closes after work before
  return, original result preserved.
- Captured diagnostics: fixed categories/context; secret sentinels отсутствуют в
  logs/Debug/Display; terminal command failure one event; domain/conflict behavior
  и retry attempts сохраняются; invalid SQLSTATE omitted.

Обязательные checks: fmt, workspace Clippy -D warnings, workspace tests включая
PostgreSQL, llvm-cov ≥90% с прежним исключением main.rs. Дополнительно compile base,
worker, postgres, host, logging по отдельности без defaults; base/logging-only lib
tests. Global logger integration также запускается с logging-only features.
Все команды через RTK согласно AGENTS.md.

Синхронизировать runtime guides, startup diagrams, clean-architecture skill,
review и .env.example. Новые defaults и behavior numeric bind/dotenv отражены в
документации. Этап завершен после прохождения всех checks.

## Результаты проверок

Итоговая реализация проверена с Rust 1.98.1:

| Проверка | Результат |
| --- | --- |
| `cargo fmt --all --check` | Passed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Passed |
| `cargo test --workspace --all-features` | Passed: 200 unit/integration + 1 compile-fail doctest = 201; PostgreSQL suites и Axum shutdown включены |
| `cargo llvm-cov --workspace --all-features --color never --ignore-filename-regex '(^|/)main\.rs$' --fail-under-lines 90` | Passed: 97.54% line coverage (7191 lines, 177 missed) |
| `cargo test -p cqrs-runtime --no-default-features --lib` | 33 passed |
| `cargo test -p cqrs-runtime --no-default-features --features logging --lib --test logging` | 34 lib + 1 isolated logger test passed |
| `cargo check -p cqrs-runtime --no-default-features` | Passed |
| `cargo check -p cqrs-runtime --no-default-features --features worker` | Passed |
| `cargo check -p cqrs-runtime --no-default-features --features postgres` | Passed |
| `cargo check -p cqrs-runtime --no-default-features --features host` | Passed |
| `cargo check -p cqrs-runtime --no-default-features --features logging` | Passed |
| `git diff --check` | Passed |

Все команды выполнялись через RTK. Compact `rtk cargo test` выдавал E0463 при
запуске workspace rustdoc после успешных 200 unit/integration tests; отдельные
doctests проходили. Итоговый полный `rtk proxy cargo test --workspace --all-features`
прошел вместе с doctests, без исключений или ослабления требований. Feature matrix
и итоговый Clippy также запускались через RTK proxy. Блокеров не осталось.

PostgreSQL fixtures используют runtime connect_pool и реальные testcontainers
через доступный Podman socket. Connection test проверяет max=10, SQL query и
возврат исходной authentication error (SQLSTATE 28P01) без panic. Config/cleanup
и safe diagnostics tests проходят с controlled inputs; global logger init проверен
в изолированном integration executable. Base и logging-only tests подтверждают
независимость от postgres/worker/host.

ServiceConfig и ServiceError остаются private binary API; main сохраняет business
wiring. Service logging module удален; runtime facade logging и connect_pool
используются напрямую. Private env/diagnostic helpers сохраняют error mapping,
retry attempts и budgets. Локальный .env имеет numeric bind и не требовал изменения.
Новых env variables, SQL migrations или telemetry dependencies/features нет.
Guides, Mermaid startup diagram, skill, review, known issue и .env.example обновлены.
