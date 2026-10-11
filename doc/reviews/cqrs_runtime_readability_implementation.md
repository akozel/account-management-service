# Реализация review cqrs-runtime

Дата: 2026-10-11. Основание: [исходный review](cqrs_runtime_readability.md).

## Результат

Исправления P1–P3 реализованы в runtime и consumers workspace. Публичные API
обновлены сразу, без compatibility aliases. Durable task formats, SQL schema,
env names, retry policy и lifecycle budgets сохранены. Non-Unicode env handling
остаётся отдельной задачей по выбранному плану; текущее поведение описано в guide.

## Диагностика

`HostError::Debug` для I/O ошибок выводит только вариант и ErrorKind. Оригинальные
sources доступны программно. Registry error Display/Debug используют общую
`diagnostics::safe_type`; duplicate format сохраняется в error data.
Эта же функция используется typed adapter и worker failure logging.
Regression tests проверяют host, вторичные shutdown failures и ServiceError.

## API и ошибки

| API | Новый контракт |
| --- | --- |
| `outbox_task_coordinator_with_config` | Возвращает coordinator без Result |
| `PostgresOutboxQueue::with_config` | Возвращает Self без Result |
| `worker::prepare`, `postgres::prepare_outbox_runtime` | `Result<PreparedOutboxRuntime, WorkerRegistryError>` |
| `PreparedOutboxRuntime::run`, `worker::run` | `Result<(), OutboxWorkerPoolError>` |
| `TaskHandlerRegistry::formats` | Заимствованный `&[TaskFormat]` |

`WorkerRegistryError::{Build, FormatsMismatch}` обозначает ошибки реально общей
операции подготовки/restart registry. `OutboxWorkerPoolError::{Configuration,
Registry}` отделяет actual-budget validation от registry failures. Удалены
`OutboxPreparationError` и `PostgresOutboxPreparationError`; ServiceError получает
WorkerRegistryError напрямую. InvalidFormat сохраняет conversion и source.

Конфиги проверяют локальные ограничения при создании; OutboxRuntimeConfig проверяет
совместный budget. Низкоуровневый worker run сохраняет проверку фактического budget
custom coordinator. Defaults и env fallbacks используют общие приватные constants.

QueueRequestError относится только к queue parameters. InvalidDelivery и
InvalidExecutionBudget перенесены непосредственно в OutboxError. Timeout,
Unavailable и LeaseBudgetExhausted остаются разными причинами.

## Читаемость и функциональные границы

Registry collections и параметры названы registry/registries; notify обозначает
capacity_available; host явно работает с outbox supervisor. PermanentRejection
описывает permanent outcome и business handler, и format/decode adapter.
Внутренние модули delivery_worker и outbox_runtime_factory отражают назначение.

Handler result → SettlementRequest и request/status → settlement parameters/result
вынесены в чистые функции. Случайный fallback delay вычисляется в async оболочке
только для retry без explicit delay. SQL rows декодируются в приватные ClaimRow и
RecoveryRow; адаптер логирует SQLx failures до потери source. Command executor
разделяет inspect_err diagnostics и map_err classification.

Preparation собирает ровно C registries через iterator, удерживает первый для
worker zero и перемещает config parts. Formats, leases, deadlines и подтверждения
commit сохраняются. Supervisor и recovery остаются последовательными.

## Проверки

- `cargo fmt --all --check` — успешно.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — успешно.
- `cargo test --workspace --all-features` — 206 passed, включая PostgreSQL suites.
- `cargo llvm-cov --workspace --all-features --color never --ignore-filename-regex
  '(^|/)main\.rs$' --fail-under-lines 90` — line coverage 97.75%.
- Feature matrix — base, postgres, worker, host, logging и все features проходят
  `cargo check --locked -p cqrs-runtime --no-default-features` с соответствующими features.

Runtime README, architecture/task-processing guides и clean-architecture skill
обновлены вместе с API. Исходный review сохранён как описание исходного состояния.
