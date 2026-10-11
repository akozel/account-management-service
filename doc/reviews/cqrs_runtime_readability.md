# Review cqrs-runtime: читаемость, названия, типы и ошибки

Дата: 2026-10-11. Исходная ревизия: `85726aa`.
Это review текущей реализации после последних рефакторингов; Rust-код,
зависимости, SQL и persisted formats не изменены.

Исправления этого review реализованы позднее; актуальные API и результаты
проверок описаны в [отчёте реализации](cqrs_runtime_readability_implementation.md).
Наблюдения и названия ниже относятся к исходной ревизии.

## Вывод

Границы runtime уже достаточно ясные. Основной выигрыш даст упрощение сборки
из проверенных компонентов, уточнение названий runtime/registry/delivery и
отделение преобразований данных от асинхронного исполнения. Переписывать
supervisor или вводить общую иерархию задач/ошибок не требуется.

Два места нарушают принятую политику безопасной диагностики. Помимо этого,
проверенные конфиги порождают лишние fallible APIs, а ошибки некорректного
ответа хранилища классифицируются как некорректный запрос.

Приоритеты ниже: P1 — исправление поведения диагностики; P2 — существенное
упрощение или исправление семантики API; P3 — локальное улучшение.

## Устройство и удачные решения

| Компонент | Фактическая ответственность | Оценка |
| --- | --- | --- |
| `command` | Один framework на вызов; повтор неизменной команды только при optimistic conflict | Простая граница; сохранить |
| `outbox/task`, `delivery` | Публикуемая работа, отдельная попытка доставки, платформенный lease | Полезное разделение типов; сохранить |
| `outbox/registry`, `typed_handler` | Маршрутизация по type/version и декодирование DTO | Ответственности разделены правильно |
| `outbox/coordinator` | Общий budget claim, invocation deadline и settlement | Сложность в основном необходимая |
| `worker` | Подготовка registries, bounded admission, перезапуски, drain | Уточнить имена и ошибки подготовки |
| `postgres` | Атомарная публикация и отдельно управление доставкой | Не объединять два адаптера |
| `host` | Сигналы, cancellation, приоритет ошибок и ожидание компонентов | Сохранить явные lifecycle states |
| `logging`, `diagnostics`, `env` | Явная установка logger, безопасные категории, parsing | Небольшие модули оправданы |

`NewOutboxTask` описывает новую durable операцию, `TaskDelivery` — конкретную
попытку её исполнения, `ClaimedTask` — доставку вместе с lease. Это разные
понятия, а не дублирующие DTO. Бизнес-handler правильно получает только
`&TaskDelivery`. `SettlementRequest` отделён от подтверждённого
`SettlementResult`; scheduled retry не приравнен к завершению.

`ExecutionPlan` и `WritePlan` уже дают хороший функциональный образец:
сначала вычисляется решение, затем выполняются эффекты. Приватные поля и
checked constructors ограничивают невалидные состояния. `Send` без лишнего
`Sync`/`Clone` у бизнес-handler и DTO подтверждён тестами. Trait objects
нужны для внедряемых портов и heterogeneous registries; заменять их общей
generic-иерархией ради стиля не стоит.

## P1. HostError раскрывает произвольный текст I/O-ошибки

Место: [`host/error.rs`](../../cqrs-runtime/src/host/error.rs), строки 30–55;
[`startup.rs`](../../account-managements-service/src/startup.rs), строки 35–38.

`HostError::Display` скрывает исходный текст, но его ручной `Debug` для
`Inbound` и `ShutdownSignal` вызывает `.field(error)`. `HostError::log`
пользуется `?self`, а `ServiceError::Debug` делегирует host-ошибке. Поэтому
произвольное содержимое `io::Error` попадает в логи и termination report.
Защита `JoinError` уже реализована; для I/O источников аналогичной защиты нет.

Минимальное воспроизведение:

```rust
let error = HostError::Inbound(std::io::Error::other("private_io_sentinel"));
assert!(format!("{error:?}").contains("private_io_sentinel"));
```

Предложение: сохранять исходную ошибку через `source()`, а `Debug`/structured
log формировать из категории host-ошибки и `io::ErrorKind`. Не форматировать
произвольный source. Добавить regression cases для `Inbound`, `ShutdownSignal`,
host logging и обёртки `ServiceError`.

## P1. Registry errors обходят sanitization task type

Место: [`outbox/registry.rs`](../../cqrs-runtime/src/outbox/registry.rs),
строки 8–15; [`worker/supervisor.rs`](../../cqrs-runtime/src/worker/supervisor.rs),
строки 26–35; [`host/error.rs`](../../cqrs-runtime/src/host/error.rs), строка 35.

`TaskFormat::new` разрешает любое непустое имя, включая переводы строк и
произвольные данные. `DuplicateFormat(TaskFormat)` печатает это имя через
`Display`, а derive `Debug` также сохраняет его в выводе.
`WorkerPoolError::RegistryBuild` включает source в `Display`; `HostError`
делегирует его `Debug`. При ошибке restart factory этот путь доступен в host
diagnostics. Безопасный вывод type в typed adapter и `log_failed` здесь не помогает.
Стандартные фиксированные имена бизнес-DTO проблему не проявляют, но публичный
контракт runtime допускает её воспроизведение.

Предложение: error formatting оставляет категорию, worker index, безопасный
type и version. Сам `TaskFormat` можно сохранить для программного разбора.
Использовать существующий приватный `diagnostics::safe_type`; не ограничивать
задним числом допустимые persisted names. Проверять `Display`, `Debug` и host
log на дублирующемся формате с sentinel и переводом строки.

## P2. Проверенные конфиги повторно проверяются при сборке

Места:

- [`coordinator/config.rs`](../../cqrs-runtime/src/outbox/coordinator/config.rs), строки 23–45;
- [`coordinator.rs`](../../cqrs-runtime/src/outbox/coordinator.rs), строки 57–64;
- [`worker/runtime_config.rs`](../../cqrs-runtime/src/worker/runtime_config.rs), строки 22–28;
- [`outbox_queue/config.rs`](../../cqrs-runtime/src/postgres/outbox_queue/config.rs), строки 23–45;
- [`outbox_queue.rs`](../../cqrs-runtime/src/postgres/outbox_queue.rs), строки 35–41;
- [`preparation.rs`](../../cqrs-runtime/src/worker/preparation.rs), строки 14–20;
- [`worker_factory.rs` (теперь `outbox_runtime_factory.rs`)](../../cqrs-runtime/src/postgres/outbox_runtime_factory.rs), исходные строки 7–13.

У `OutboxTaskCoordinatorConfig` и `PostgresOutboxQueueConfig` приватные поля.
Их `new` уже проверяет все локальные ограничения, а defaults допустимы.
Повторные `validate()` у consumers проверяют неизменившийся объект. Поэтому
configuration variants в `OutboxPreparationError` и
`PostgresOutboxPreparationError` на штатном публичном пути недостижимы.

Получается цепочка обёрток:

```text
PostgresOutboxPreparationError::Preparation(
    OutboxPreparationError::Worker(
        WorkerPoolError::RegistryBuild { worker_index, source }
    )
)
```

Предложение:

1. Локальные ограничения проверять при создании соответствующего config.
2. Совместный lease budget проверять в `OutboxRuntimeConfig::new`.
3. Сборку coordinator/queue из проверенного config сделать infallible.
4. `worker::prepare` возвращает непосредственно ошибки сборки/mismatch registry;
   PostgreSQL helper возвращает тот же результат без своей обёртки.

Можно выделить один `WorkerRegistryError::{Build, FormatsMismatch}` с
`worker_index`; подготовка возвращает его, низкоуровневый run оборачивает
его вместе с ошибкой совместного budget. Это отдельная ошибка реально общей
операции, а не дополнительная обёртка для каждой factory.

Проверку `worker::run` против `coordinator.storage_timeout()` сохранить:
низкоуровневый API принимает произвольный coordinator, и его фактический
budget неизвестен при создании worker config. Это необходимая cross-component
проверка. Проверенные configs можно дополнять приватным consuming `into_parts`,
чтобы preparation перемещал coordinator/worker config вместо клонирования getters.

## P2. InvalidRequest смешивает запрос, исполнение и ответ хранилища

Место: [`outbox/error.rs`](../../cqrs-runtime/src/outbox/error.rs), строки 2–18;
[`lease_budget.rs`](../../cqrs-runtime/src/outbox/coordinator/lease_budget.rs),
строки 20–40; [`outbox_queue.rs`](../../cqrs-runtime/src/postgres/outbox_queue.rs),
строки 119, 168–170, 223.

`QueueRequestError::InvalidDelivery` означает некорректные данные, пришедшие из
очереди, но преобразуется в `OutboxError::InvalidRequest`. Это обвиняет caller
в ошибке ответа адаптера. `InvalidExecutionBudget` также относится к invocation
coordinator, а не запросу очереди. Enum не соответствует своему названию и границе.

Предложение: сохранить `QueueRequestError` для limit/lease/retry delay;
в `OutboxError` отдельно выразить `InvalidDelivery` и `InvalidExecutionBudget`.
Отдельные одноэлементные error enums для каждого случая не нужны.

`LeaseBudgetExhausted` оставить отдельным исходом, требующим recovery:
он не означает storage timeout и не даёт основания запускать handler.
`TimedOut` оставить отличным от подтверждённого отказа операции: результат
storage может быть неизвестен.

## P2. Названия путают registry, worker, runtime и delivery

| Сейчас | Предложение | Причина |
| --- | --- | --- |
| `PreparedOutboxWorker` | `PreparedOutboxRuntime` | Объект владеет coordinator, factory и C registries; запускает весь пул |
| `prepare_outbox_worker` | `prepare_outbox_runtime` | PostgreSQL helper собирает весь runtime |
| `WorkerPoolError` | `OutboxWorkerPoolError` | Согласовать с `OutboxWorkerPoolConfig` и обозначить подсистему |
| `checked_handler` | `build_checked_registry` | Вызывает factory и проверяет целый registry |
| `handler: TaskHandlerRegistry` | `registry` | Registry маршрутизирует несколько handlers |
| `handlers: Vec<TaskHandlerRegistry>` | `registries` | Список registries, по одному на worker |
| `worker_task.rs` | `delivery_worker.rs` | Здесь цикл доставки, а не durable task DTO |
| `run_worker` | `run_delivery_worker` | Исполняет доставки до остановки worker |
| `available: Notify` | `capacity_available` | Уведомление об освобождении локальной очереди |
| `indices: HashMap<Id, usize>` | `worker_indices` | Отображение Tokio task ID в индекс worker |
| `WorkerExit` в host | `OutboxSupervisorExit` | Завершение supervisor, а не отдельного worker |
| `worker` в host | `outbox_supervisor` | JoinHandle всего outbox supervisor |
| `task: &TaskDelivery` | `delivery` | Конкретная попытка, без lease ownership |
| `TaskFailureCause::HandlerPermanent` | `PermanentRejection` | Permanent может вернуть и format/decode adapter |
| `error` в SQL settlement tuple | `last_error` | Это текст для persisted column, не ошибка исполнения SQL |
| `prefer` | `preserve_primary_failure` | Выбирает primary и логирует secondary; имя отражает эффект |

Локальные переименования имеют непосредственную пользу. Публичные имена требуют
согласованного обновления consumers, rustdoc, guides и архитектурного skill;
compatibility aliases в этом внутреннем workspace только продлят путаницу.

Термин `Task` оставить для durable работы и бизнес-DTO; `delivery` использовать
для попытки, `worker` для постоянного потребителя, `supervisor` для управления
пулом, `tokio_task_id` только для идентичности Tokio task. `TaskPayload`,
`TaskFormat`, `TaskHandlerRegistry`, `CommandExecutor`, `CommandGatewayError`,
`PostgresEventRepositoryWithOutbox` уже отражают назначение. Длина последнего
имени оправдана его специальной атомарной записью.

Переименование Rust symbols не должно менять `TASK_TYPE`, `TASK_VERSION`,
serde fields, env names или SQL schema.

## P2. Выделить преобразования в чистые функции

Первый кандидат — [`coordinator.rs`](../../cqrs-runtime/src/outbox/coordinator.rs),
строки 169–181. Отделить `Result<(), TaskHandlerError>` → `SettlementRequest`
от `select!`. Функция получает outcome и лениво вычисляемый fallback delay;
явный `retry_after` сохраняется, случайный backoff вычисляется только при
необходимости. Timeout остаётся явной веткой выбора, использующей то же
преобразование в retry request.

Второй кандидат — [`outbox_queue.rs`](../../cqrs-runtime/src/postgres/outbox_queue.rs),
строки 133–171. Сейчас один request дважды разбирается на `failure_cause` и
tuple `(status, delay, error)`. Чистая функция может одним match строить
небольшую приватную структуру параметров settlement с именованными полями.
Преобразование возвращённого DB status в `SettlementResult` также отделить
от transaction. Не создавать публичную иерархию SQL plans ради трёх вариантов.

Третий кандидат — [`command/executor.rs`](../../cqrs-runtime/src/command/executor.rs),
строки 52–68. `map_err` одновременно логирует и преобразует ошибку.
Использовать `inspect_err` для диагностики, затем `map_err(map_aggregate_error)`;
приватный диагностический helper получает framework error по ссылке.
Количество logs и retry classification сохраняются.

Четвёртый кандидат — декодирование `PgRow` в queue. Небольшие приватные row
типы либо helpers отделят `try_get`/структурную проверку от claim/recovery SQL
и сократят повторяющиеся `map_err`. Сохранять safe operation diagnostics и
возврат deliveries только после commit. Не менять transaction granularity.

Supervisor/recovery loops оставить последовательными: порядок cancellation,
partial claim results, commit и drain имеет значение. Async combinators и
параллельные SQL calls здесь не являются автоматическим улучшением.

## P3. Небольшие упрощения

- `diagnostics::safe_type` уже существует, но typed adapter и `log_failed`
  повторяют sanitization. Использовать одну приватную функцию в трёх местах.
- `TaskHandlerRegistry::formats` может возвращать `&[TaskFormat]`: registry
  уже владеет стабильным списком. Клонировать только при передаче владения
  coordinator. Trait `TaskHandler::formats` сначала можно оставить с `Vec`;
  его изменение затронет все пользовательские handlers и требует отдельного
  обоснования. У coordinator список formats тоже стабилен, но custom trait
  implementations не стоит менять только ради небольшой экономии allocations.
- Defaults повторяются в `Default` и `from_lookup` у worker/coordinator/queue.
  Использовать приватные constants с единицами или значения `Default` как
  fallback. Оба способа сохраняют независимость overrides C/Q.
- `worker::prepare` может собрать registries через `once(Ok(first)).chain(...)`
  и `collect::<Result<Vec<_>, _>>()`. Это локальное функциональное улучшение;
  сохранить ровно C factory calls, порядок и первый registry для worker zero.
- `TaskHandlerRegistryError::InvalidFormat` нужен при построении typed adapter
  внутри factory, а не из-за невалидного `TaskFormat` внутри registry. Оставить
  conversion, уточнить rustdoc; не считать этот вариант мёртвым.
- Общий `TaskValidationError` сейчас охватывает и публикацию, и `TaskDelivery`.
  Разделение на два enum даст мало пользы при текущем небольшом наборе причин.
  Назначение лучше уточнить документацией и локальными названиями.

Дополнительная семантическая особенность: runtime `from_env` loaders используют
`std::env::var(name).ok()` и принимают non-Unicode value за отсутствие значения.
ServiceConfig уже отдельно отвергает такие значения, поэтому текущий binary
защищён. Для переносимой библиотеки стоит явно выбрать контракт: либо сохранить
и описать текущую трактовку, либо добавить общий приватный env reader и typed
ошибку non-Unicode. Это отдельное изменение поведения, не часть rename.

## Целевая система ошибок

| Граница | Ошибки, которые здесь действительно нужны |
| --- | --- |
| Команда приложения | `CommandGatewayError<D>`: domain rejection / exhausted conflict / unavailable |
| Создание task/format/delivery | Структурная валидация; без transport/storage |
| Бизнес-handler | Retry или permanent; optional retry delay |
| Queue/coordinator | Invalid queue request / invalid delivery / invalid execution budget / unavailable / timeout / exhausted lease budget |
| Config loading | Parse/local validation и проверка совместных budgets |
| Registry construction | Build failure или несовпадение поддерживаемых formats, с worker index |
| Низкоуровневый worker run | Config cross-check и registry startup/restart failures |
| Host | Signal/inbound/supervisor failure, unexpected exit, supervisor join failure |

`Ok(())` бизнес-handler означает удовлетворение контракта операции, а не
подтверждение settlement. Это хороший контракт. Технические источники,
которые намеренно скрываются за `Unavailable`, предварительно классифицируются
в diagnostics; добавлять их во все публичные ошибки не требуется. На lifecycle
границе сохранённые sources полезны, но их formatting должен быть безопасным.

## Порядок изменений и проверки

1. Исправить две диагностические утечки и добавить regression tests.
2. Упростить checked-config assembly и цепочки preparation errors.
3. Уточнить классификацию queue/coordinator errors и названия компонентов.
4. Выделить чистые преобразования, убрать sanitization/defaults дублирование.

Для будущего рефакторинга сохранить atomic event/snapshot/tasks, один framework
на вызов, immutable command/tasks между retries, ordered format checks, ровно C
startup registries, исходные lease deadlines, bounded single-producer admission,
cancellation перед channel close, H+S drain и приоритет host failures.
Изменения Rust должны проходить все проверки AGENTS.md, включая workspace
Clippy/tests и coverage >=90%; при изменении lasting convention обновляется skill.

В рамках review выполнены:

- `cargo fmt --all --check` — успешно.
- `cargo test --locked -p cqrs-runtime --all-features --lib` — 99 passed.
- `cargo test --locked -p cqrs-runtime --all-features --test logging` — 1 passed.
- `cargo test --locked -p cqrs-runtime --all-features --test postgres` — 26 passed
  с реальным PostgreSQL через testcontainers.
- `cargo test --locked -p cqrs-runtime --all-features --doc` — 1 passed.
- `cargo check --locked -p cqrs-runtime --no-default-features` для base,
  отдельно `postgres`, `worker`, `host`, `logging` и всех четырёх features — успешно.

Всего прошли 127 тестов. Обе проблемы с error formatting дополнительно
воспроизведены временной программой, использующей публичные API runtime:
`HostError::Debug` содержит `private_io_sentinel`, а supervisor error
содержит `private_format_sentinel` в `Debug` и `Display`. Временная программа
не добавлена в workspace; её исходники удалены после запуска.

Coverage в этом review не запускался: Rust production code и tests не менялись.
Проверки подтверждают текущую реализацию, а не предлагаемые изменения.
