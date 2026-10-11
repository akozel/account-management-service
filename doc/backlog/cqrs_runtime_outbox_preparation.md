# Этап 2: подготовка outbox до запуска HTTP

Статус: выполнено. Дата: 2026-10-11.
Дорожная карта: [review и этапы](../reviews/cqrs_runtime_reuse.md).
Предыдущий этап: [командный путь PostgreSQL](cqrs_runtime_command_factory.md), выполнен.

## Цель и текущая проблема

Бизнес-сервис явно выбирает handlers и их зависимости, а runtime согласует
queue, coordinator, storage/lease budgets и registries. До открытия HTTP
listener все startup registries должны быть созданы и проверены.

До этапа 2 `main` создавал registry только ради `formats()` и выбрасывал его.
После открытия listener `worker::run` создавал еще C registries. Получались
C+1 factory invocations, а ошибка второго или последующего registry выявлялась
уже после открытия HTTP. Combined budget проверялся вручную в main и повторно
в supervisor; worker settings читались после подключения к БД.

Результат этапа: один checked runtime config и один вызов подготовки outbox
в composition root; ровно C startup registries, включая первый, затем явный
запуск prepared object с CancellationToken.

## Согласованные границы

- Реализовать общий `worker::prepare` и PostgreSQL convenience helper в этом этапе.
- Сохранить публичный low-level `worker::run(coordinator, factory, config, shutdown)`
  для custom coordinators и существующих тестов. Оба входа используют один engine.
- Сохранить существующие low-level configs, их constructors, lookup APIs и
  `WorkerPoolError`. Новые ошибки конфигурации/подготовки не объединять с runtime exits.
- Новых env variables, dependencies, features, SQL/schema и persisted formats нет.
- Host lifecycle, startup error enum сервиса, typed tasks, pool connection helper,
  logging и force shutdown относятся к следующим этапам. Существующий startup
  `expect` стиль main пока сохраняется; ошибки библиотечных APIs типизированы.

## Публичные API

### Общая конфигурация

Добавить в `worker` facade `OutboxRuntimeConfig` и `OutboxRuntimeConfigError`.
Приватная реализация — `worker/runtime_config.rs`.

```rust
pub fn new(
    coordinator: OutboxTaskCoordinatorConfig,
    worker: OutboxWorkerPoolConfig,
) -> Result<Self, OutboxRuntimeConfigError>;

pub fn from_env() -> Result<Self, OutboxRuntimeConfigError>;
pub fn from_lookup(
    lookup: impl FnMut(&str) -> Option<String>,
) -> Result<Self, OutboxRuntimeConfigError>;

pub fn coordinator(&self) -> &OutboxTaskCoordinatorConfig;
pub fn worker(&self) -> &OutboxWorkerPoolConfig;
```

Fields private; `Clone`, `Debug`, checked `Default`. Constructor проверяет
coordinator config и вызывает существующий
`worker.validate_for_storage_timeout(coordinator.storage_timeout())`.
Использовать прежнюю формулу с checked arithmetic, без новой оценки lease:

```text
lease > 2*S + H + 5s + ceil(Q/C)*(H+S)
```

Defaults: C=128, Q=128, poll=1s, H=15s, S=6s, lease=60s.
`Default` не читает env. `from_lookup` сначала читает coordinator, затем worker
через их существующие методы с одним передаваемым mutable lookup, после чего
вызывает checked `new`. `from_env` делегирует lookup с текущей семантикой env reads.

Ошибки: `OutboxRuntimeConfigError::Coordinator(OutboxTaskCoordinatorConfigError)`
и `::Worker(OutboxWorkerPoolConfigError)`, с сохранением source. Ошибка combined
budget относится к `Worker`; parser и диапазоны значений не переписываются.

PostgreSQL statement timeout остается отдельным `PostgresOutboxQueueConfig`:
не добавлять PostgreSQL settings в общий runtime config. Default SQL cap=4s;
не вводить новое обязательное сравнение SQL cap и storage timeout.

### Подготовка и запуск

Приватная реализация — `worker/preparation.rs`; типы и функция экспортируются
через `worker` facade.

```rust
pub fn prepare(
    queue: Arc<dyn OutboxQueue>,
    factory: HandlerRegistryFactory,
    config: OutboxRuntimeConfig,
) -> Result<PreparedOutboxWorker, OutboxPreparationError>;

impl PreparedOutboxWorker {
    pub async fn run(self, shutdown: CancellationToken) -> Result<(), WorkerPoolError>;
}
```

`PreparedOutboxWorker` владеет coordinator, factory, worker config и Vec из C
готовых registries. Fields private; тип не реализует Clone и потребляется run.
Он должен быть Send, и его run future должен подходить для `tokio::spawn`;
Sync для mutable handlers не требуется. Дополнительные публичные getters,
runtime handles, start flags и shared registry mutex не вводить.

`OutboxPreparationError` различает
`CoordinatorConfiguration(OutboxTaskCoordinatorConfigError)` и
`Worker(WorkerPoolError)`; source сохраняется. Для startup registry errors
использовать существующие `RegistryBuild { worker_index, source }` и
`HandlerFormatsMismatch { worker_index }` внутри варианта Worker.

### PostgreSQL helper

Добавить приватный `postgres/worker_factory.rs` и facade exports только при
одновременных features `postgres` и `worker`:

```rust
pub fn prepare_outbox_worker(
    pool: PgPool,
    factory: HandlerRegistryFactory,
    queue_config: PostgresOutboxQueueConfig,
    runtime_config: OutboxRuntimeConfig,
) -> Result<PreparedOutboxWorker, PostgresOutboxPreparationError>;
```

Helper создает `PostgresOutboxQueue::with_config`, оборачивает queue в Arc и
вызывает `worker::prepare`. Ошибки:
`PostgresOutboxPreparationError::QueueConfiguration(PostgresOutboxQueueConfigError)`
и `::Preparation(OutboxPreparationError)`, с source. Здесь нет connect, SQL,
миграций, env reads, регистрации business handlers или background spawn.
Feature `postgres` не начинает неявно включать `worker`.

## Алгоритм подготовки и единый supervisor

1. Runtime config уже проверен constructor/loader до вызова prepare.
2. Вызвать factory для worker index=0; failure вернуть как RegistryBuild(0).
3. Взять cached ordered `formats()` первого registry, не сортировать и не
   преобразовывать к unordered set. Registry уже проверяет empty/duplicate formats.
4. Собрать coordinator с этой очередью, format list и coordinator config.
5. Сохранить первый registry в Vec и создать registries для indices 1..C−1.
   Каждый проверять существующей проверкой ordered formats.
6. Вернуть prepared object. На первой ошибке прекратить сборку и освободить
   уже созданные registries. Factory вызывается ровно C раз при успешной подготовке.

`prepare` синхронен, не создает Tokio tasks/channel/poller и не обращается к
queue storage. Он не требует Tokio runtime сам по себе; пользовательская
factory должна конструировать handlers быстро и не начинать их business effects.
Отмена не прерывает blocking factory; новый cancellation protocol не вводится.

Выделить текущую часть supervisor после проверки/создания startup registries
в одну приватную async функцию запуска. Она принимает готовые registries,
coordinator, factory, worker config и token. Сохранить текущие poller, JoinSet,
capacity, restart, recovery и drain loops; не создавать второй engine.

`PreparedOutboxWorker::run` использует этот engine без повторной startup factory
invocation. `worker::run` остается тонким low-level входом: создает DropGuard,
проверяет budget против фактического `coordinator.storage_timeout()`, проверяет
предварительную отмену, собирает C registries по format list данного coordinator
и запускает тот же engine. Общую проверку registry formats переиспользовать.

## Владение, ошибки и cancellation

- Registry index=0 действительно передается worker 0; ни один startup registry
  не выбрасывается в пользу заново созданного. Каждый mutable registry и sender
  принадлежит одной постоянной Tokio task.
- До run в prepared object нет выполняющихся задач и CancellationToken. Его
  обычный Drop освобождает registries, не запускает shutdown/drain и не отменяет host.
- При первом poll run future устанавливает DropGuard до fallible runtime work. Если token
  уже отменен, возвратить Ok, освободить registries, не делать claims/recovery,
  spawn или дополнительных factory invocations. Регистры уже были созданы в prepare.
- Low-level run сохраняет прежнее поведение: предварительно отмененный token
  не приводит к созданию registries; error/panic startup отменяет token через guard.
- Panic одной worker task заменяется новым registry из factory с проверкой formats.
  Успешный restart вызывает factory ровно один дополнительный раз.
- Restart factory error/mismatch сначала отменяет token, затем drains начатую
  работу и возвращает исходный WorkerPoolError. Factory/supervisor panic сохраняет
  нынешнюю unwind/DropGuard семантику; новый catch_unwind не вводится.
- Сохранить cancel-before-channel-close, остановку dequeue/claims, общий H+S drain,
  abort/join, recovery ожидающих claims, child-token isolation и safe JoinError logs.
  Cancellation не race-ится с активным handler/settlement. Claim deadlines не сбрасываются.

Defaults, независимые C/Q, recovery batch min(C,32), единственный producer и
claim limit Q−sender.len() сохраняются. At-least-once, unknown commit outcomes,
task ordering и бизнес-классификация результата не меняются.

## Изменение composition root

До подключения pool загрузить `OutboxRuntimeConfig::from_env()` и отдельный
`PostgresOutboxQueueConfig::from_env()`. Combined config тем самым проверяется
до подключения БД. После сборки business services оставить явный handler factory.

Перед `TcpListener::bind` вызвать:

```rust
let outbox = postgres::prepare_outbox_worker(
    pool.clone(), handler_factory, postgres_queue_config, outbox_config,
).expect("valid outbox preparation");
```

Убрать из main ручные queue/coordinator creation, пробный `factory().formats()`,
late worker-config loading и ручной combined-budget check.
После listener и общего token запускать `tokio::spawn(outbox.run(shutdown.clone()))`.
Получаемый JoinHandle имеет прежний результат `Result<(), WorkerPoolError>`;
сервисный process runtime и порядок HTTP/outbox/DB shutdown остаются текущими.

Никакого generic HTTP bootstrap или нового host lifecycle в этом этапе нет.
Startup factories должны полностью завершиться до bind listener. Prepared
startup гарантирует проверку конфигурации/registries, но не здоровье БД и внешних
receivers: проверка эффектов начинается при run.

## Порядок реализации и тесты

1. Добавить combined config, typed config errors и inline unit tests:
   defaults, independent overrides C/Q, invalid integers, границы lease>53s,
   дополнительная queued wave и arithmetic overflow. Existing parser tests сохранить.
2. Добавить preparation и общий supervisor engine. Inline tests preparation:
   ровно C вызовов factory; отсутствие storage calls до run; первый registry
   используется и сохраняет mutable state; empty/duplicate formats, failure на
   worker k, различие format/version/order возвращают typed indexed error.
   При failure и Drop prepared все ранее созданные handlers освобождаются.
3. Проверить prepared run на предварительно отмененном token; успешный restart
   использует один новый registry. Error/mismatch restart отменяет host до drain;
   abort начавшего выполняться supervisor отменяет token и не оставляет detached workers.
   Использовать controlled futures/Notify/counters, не time-based performance asserts.
4. Existing low-level supervisor tests с нестандартными coordinators оставить
   для проверки фактического storage timeout, stall, panic и cancelled startup.
   Behavioral tests mutable-state ownership, bounded capacity и 1000 successful
   deliveries перевести на prepared path; сохраняются прежние behavioral assertions.
5. Добавить PostgreSQL helper и перевести обе реальные worker integration tests
   на подготовку до spawn: два пула на одной queue обрабатывают work, несколько
   supported formats маршрутизируются, malformed payload становится Failed,
   unknown format остается Pending с attempts=0. После prepare и до run строки
   остаются pending: helper не выполняет claims.
6. Перевести main на новый API. Startup ordering тестировать в runtime preparation
   с controlled factory/storage; не добавлять brittle unit tests main.

## Документация и приемка

При реализации обновить runtime README, public rustdoc, architecture diagram,
portable task-processing guide и clean-architecture skill. Новая граница:
composition root перечисляет handlers; runtime создает/проверяет registries и
согласует coordinator/budgets. Описать отдельно prepare без token и начавшийся run
с DropGuard, а также сохраненный low-level API.

В `.env.example` существующие settings/defaults сохраняются; пояснить combined
validation до подключения БД. SQL cap остается отдельным. Process lifecycle,
HTTP/DB deadlines и известные snapshot/console limitations не пересматривать.

Обязательные проверки реализации:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo llvm-cov --workspace --all-features --color never \
  --ignore-filename-regex '(^|/)main\.rs$' --fail-under-lines 90
cargo check -p cqrs-runtime --no-default-features
cargo check -p cqrs-runtime --no-default-features --features worker
cargo check -p cqrs-runtime --no-default-features --features postgres
```

После каждого изменения Rust выполнять repository coverage requirement.
Проверки должны включать реальные PostgreSQL suites; blocker указать точно,
не ослабляя требования. Статус «Выполнено» ставить после реализации и проверок.

Критерий завершения: main использует ready outbox, до HTTP проверены все C
registries и combined budget, при run используются сохраненные handlers,
runtime lifecycle contracts и low-level regression tests проходят,
workspace line coverage ≥90%. Изменение не требует миграции данных.

## Результат реализации

Добавлены `worker::OutboxRuntimeConfig`, `worker::prepare`,
`PreparedOutboxWorker` и PostgreSQL helper. Main проверяет combined settings
до подключения БД, готовит все registries до bind HTTP и запускает ready outbox.
Low-level API сохранен; оба входа используют единственный supervisor engine.
В документации и архитектурном skill закреплены новые границы; `.env.example`
поясняет раннюю проверку без изменения variables/defaults.

Все обязательные проверки прошли:

- fmt и workspace Clippy с `-D warnings`;
- workspace tests: **166 passed**, включая PostgreSQL и doctest, без ignored tests;
- workspace line coverage: **97.97%**, при пороге 90%;
- no-default, worker-only и postgres-only compile checks.

Подтверждены ровно C startup invocations и освобождение handlers на ошибке/drop;
обработка первого registry и сохранение mutable state; indexed format/version/order
errors; prepared cancellation, restart/mismatch и abort; bounded C/Q, 1000 deliveries;
PostgreSQL Pending/attempts=0 до run, включая failed preparation, затем routing,
Completed/Failed и quarantine неизвестного формата. Environmental blockers нет.

Первоначальный план выше сохранен как контракт реализованного этапа.
