# Этап 1: готовый командный путь PostgreSQL

Статус: выполнено. Дата: 2026-10-11.
Дорожная карта: [review и этапы](../reviews/cqrs_runtime_reuse.md).

## Цель и границы

Убрать повторяющуюся repository/store/framework сборку из бизнес-сервиса и
generic bounds из его gateways. Первый этап предоставляет готовую factory
для `Aggregate<Services = ()>` и пустого списка queries. Custom stores, services
и queries доступны через существующий low-level constructor executor.

Outbox workers, lifecycle, logging, pool connection helper и workspace dependency
management относятся к следующим этапам. Новых env variables, features,
SQL migrations и persisted formats в первом этапе нет.

## API и реализация

- `command::CommandExecutor<A>: Send + Sync` — async-trait boundary с одним
  методом `execute(&self, aggregate_id: &str, command: A::Command,
  tasks: Vec<NewOutboxTask>) -> Result<(), CommandGatewayError<A::Error>>`.
- `CqrsCommandExecutor` реализует trait, сохраняя constructor с custom framework
  factory, существующий optimistic-conflict retry и error mapping. Commands
  достаточно быть `Clone + Send`; `Sync` не требуется.
- `postgres::PostgresCommandConfig` хранит private `NonZeroUsize` snapshot interval
  и conflict retry policy; `new(usize, policy)` возвращает typed validation error
  при нуле, getters доступны. Defaults: interval=25, существующая retry policy.
- `postgres::command_executor::<A>(PgPool, PostgresCommandConfig)` возвращает
  `Arc<dyn CommandExecutor<A>>` напрямую: проверенная сборка не выполняет I/O.
  Aggregate имеет unit services, commands — `Clone + Send + 'static`,
  domain errors — `Send + 'static`, aggregate — `'static`.
- Per execute создается новый repository с immutable task list, snapshot store
  и framework; framework используется всеми conflict attempts этого вызова.
  Разные вызовы разделяют pool, не task buffer. Retry closure владеет исходной
  command, поэтому Send future не требует Command: Sync.

## Интеграция

Два gateways принимают `Arc<dyn CommandExecutor<СоответствующийАгрегат>>` и
содержат выбор aggregate ID и делегирование. `main` явно строит два executors
с общим command config, затем gateways и бизнес-services.

Обычные PostgreSQL test fixtures используют новую factory с существующими
interval=3/1. `MemStore`, `ConflictOnceStore` и `GatedStore` fixtures сохраняют
custom framework construction внутри low-level runtime executor.

Обновляются runtime README, architecture diagram, portable task-processing
guide и clean-architecture skill. Pool пока подключается прежним helper,
поэтому production dependency сервиса на `postgres-es` остается до этапа 5.

## Критерии и тесты

- Config defaults/override и отклонение interval=0.
- Existing runtime retry/error scenarios выполняются через dyn executor:
  один framework на вызов, unchanged command/tasks, reload после conflict,
  технические ошибки не получают fast retry.
- Отдельный тест запускает executor с Send, но не Sync command через Tokio task.
- Реальный PostgreSQL: event/snapshot/tasks публикуются вместе; domain rejection
  не добавляет записи; ошибка duplicate task ID откатывает event и snapshot.
- Events-only вызов после outbox вызова не переиспользует прежний список tasks.
- Шестнадцать параллельных вызовов общего executor сохраняют собственные
  aggregate IDs, event data и immutable task IDs/payloads.
- Interval=2: первый event без snapshot, второй создает snapshot, третий читает
  его, четвертый читает snapshot+tail и обновляет snapshot.
- Existing service gateway, registration, snapshots и race tests проходят.

Обнаруженное upstream ограничение snapshot apply описано отдельно в
[known issue](../known_issues/cqrs_snapshot_event_reapplication.md) и runtime API.
State assignment в существующих агрегатах совместимо; изменение механизма
snapshots не включено в этот этап.

## Проверки завершения

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo llvm-cov --workspace --all-features --color never \
  --ignore-filename-regex '(^|/)main\.rs$' --fail-under-lines 90
cargo check -p cqrs-runtime --no-default-features
cargo check -p cqrs-runtime --no-default-features --features postgres
cargo check -p cqrs-runtime --no-default-features --features worker
```

Все проверки прошли. Workspace tests: **159 passed**, включая реальные
PostgreSQL suites и doctest; ignored tests отсутствуют. Workspace line coverage:
**97.97%** при пороге 90%; `command/executor.rs` и `postgres/command_factory.rs`
имеют 100% line coverage. Независимые no-default/postgres-only/worker-only
compile checks также прошли. Environmental blockers отсутствуют.
