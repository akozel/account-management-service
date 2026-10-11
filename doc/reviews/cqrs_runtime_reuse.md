# Review небизнес-кода и переиспользование cqrs-runtime

Дата: 2026-10-11. Статус: этапы 1–2 выполнены; этапы 3–6 — backlog.

Под «сборкой» здесь рассматриваются composition root и запуск приложения.
Cargo/CI рассмотрены в этапе 6. Бизнес-инварианты не являются предметом
этого review. API этапов 1–2 реализованы; примеры этапов 3–5 остаются эскизами.

## Вывод

Выделение `cqrs-runtime` уже проведено по правильной границе: command executor,
outbox, PostgreSQL и worker supervisor не зависят от account management.
Следующий полезный шаг — дать этим компонентам небольшие готовые функции
сборки и перенести общий host lifecycle. Сейчас каждый новый сервис должен
повторять технические детали, которые уже определены контрактами runtime.

Рекомендуемая последовательность: PostgreSQL command factory → prepared outbox
worker → process host → typed task adapter. Логирование и мелкие внутренние
helpers можно перенести после этих изменений. Composition root остается явным:
из него видно, какие агрегаты, gateways, сервисы, handlers и HTTP router выбраны.

## Этапы реализации

Работы выполняются последовательно. Полный план и результат первого этапа:
[готовый командный путь PostgreSQL](../backlog/cqrs_runtime_command_factory.md).
План и результат второго этапа:
[подготовка outbox до запуска HTTP](../backlog/cqrs_runtime_outbox_preparation.md).

| Этап | Результат и критерий завершения | Статус |
| --- | --- | --- |
| 1. Командный путь PostgreSQL | Checked config, готовая factory, CommandExecutor interface, два простых gateways; сохраняются retries, snapshot replay и atomic event/tasks. 159 tests passed; line coverage 97.97%; все обязательные проверки прошли. | Выполнено |
| 2. Подготовка outbox | Checked OutboxRuntimeConfig, prepared registries и PostgreSQL helper; все C registries проверяются до HTTP и используются workers. 166 tests passed; line coverage 97.97%; обязательные и feature checks прошли. | Выполнено |
| 3. Lifecycle процесса | [План и реализация](../backlog/cqrs_runtime_host_lifecycle.md): optional host владеет signals/token/spawn/drain; lifecycle tests в runtime, Axum test в сервисе. 176 tests passed; line coverage 97.79%; обязательные и feature checks прошли. | Выполнено |
| 4. Типизированные tasks | [План и реализация](../backlog/cqrs_runtime_typed_task_adapter.md): boxed typed adapter, три бизнес-handler и borrowed sender context. 187 tests passed; line coverage 97.91%; обязательные и feature checks прошли. | Выполнено |
| 5. Конфигурация и диагностика | ServiceConfig, fallible pool connection, optional telemetry, private env helpers и safe storage categories; настройки валидируются до подключения БД. | Backlog |
| 6. Workspace и CI | Общие dependency versions с сохранением features/optional; compile matrix покрывает независимые runtime features. | Backlog |

Этап 3 использует prepared outbox этапа 2; этап 5 объединяет появившиеся runtime
configs, а этап 6 проверяет итоговые feature combinations. Детальная реализация
поздних этапов уточняется перед началом каждого этапа. Статус «Выполнено»
проставляется после реализации и обязательных проверок, не после одного дизайна.

## Наблюдения по приоритету

Это наблюдения исходного review до этапа 1, с исходными номерами строк;
приоритеты относятся к рефакторингу, а не к критичности дефектов.

| Приоритет | Место | Наблюдение и последствие |
| --- | --- | --- |
| P1 | [`main.rs`](../../account-managements-service/src/main.rs), строки 47–67 | Дважды повторяется repository → snapshot store → framework. Snapshot interval `25` задан дважды. Каждый следующий агрегат потребует такой же технической сборки. |
| P1 | [`main.rs`](../../account-managements-service/src/main.rs), строки 70–86; [`supervisor.rs`](../../cqrs-runtime/src/worker/supervisor.rs) | Host вручную согласует queue, registry formats, coordinator и worker budgets. Проверочный registry создается и выбрасывается, затем supervisor создает еще C registries. Обязательные шаги интеграции runtime распределены между host и supervisor. |
| P1 | `runtime.rs` (исходный сервисный модуль; перенесен в [`host`](../../cqrs-runtime/src/host.rs)), строки 1–151; [`main.rs`](../../account-managements-service/src/main.rs), строки 114–128 | Управление signal/token/component exit, drain и приоритетом ошибок не использует бизнес-типы. Это готовый кандидат на перенос вместе с lifecycle-тестами. |
| P2 | [`create_account.rs`](../../account-managements-service/src/application/user_account/tasks/create_account.rs), [`record_account_created.rs`](../../account-managements-service/src/application/user_account/tasks/record_account_created.rs), [`send_verification_code.rs`](../../account-managements-service/src/application/email_reservation/tasks/send_verification_code.rs) | Три handlers повторяют регистрацию формата, проверку версии, JSON decode и permanent error. Бизнес-операция занимает лишь часть каждого обработчика. |
| P2 | [`email_reservation_gateway.rs`](../../account-managements-service/src/infrastructure/cqrs/email_reservation_gateway.rs), [`user_account_gateway.rs`](../../account-managements-service/src/infrastructure/cqrs/user_account_gateway.rs) | Сервисные gateways несут `BuildFramework`, `EventStore` и связанные bounds. Их собственная ответственность гораздо меньше: выбрать aggregate ID и делегировать команду. |
| P2 | [`main.rs`](../../account-managements-service/src/main.rs), строки 38–45, 83–89 | Некоторые настройки проверяются после подключения БД, startup failures обрабатываются через `expect`. В локальном исходнике `postgres-es::default_postgress_pool` также обнаружен `expect` и фиксированные `max_connections(10)`. |
| P2 | [`executor.rs`](../../cqrs-runtime/src/command/executor.rs), строки 52–58; [`outbox_queue.rs`](../../cqrs-runtime/src/postgres/outbox_queue.rs), функция `storage_error` | Технические причины сводятся к `Unavailable` без сохранения диагностической категории. Это безопасно для внешнего API, но затрудняет различение недоступной БД, ошибки сериализации и внутренней ошибки записи. |
| P3 | [`logging.rs`](../../account-managements-service/src/infrastructure/logging.rs) | Настройка tracing полностью универсальна, но принадлежит бизнес-crate. `LogingConfig` — struct без данных с опечаткой в имени; установка subscriber происходит через `.init()`. |

## Этап 1. PostgreSQL: готовая сборка командного пути

Реализация находится в `postgres` facade и приватном
`postgres/command_factory.rs`. Runtime предоставляет `command::CommandExecutor<A>`
с одним async методом:

```rust
async fn execute(
    &self,
    aggregate_id: &str,
    command: A::Command,
    tasks: Vec<NewOutboxTask>,
) -> Result<(), CommandGatewayError<A::Error>>;
```

`CqrsCommandExecutor` реализует этот trait и продолжает владеть conflict retry.
Gateways принимают `Arc<dyn CommandExecutor<A>>`, вычисляют aggregate ID и
делегируют исходную command/tasks; factory/retry/store generic bounds в них нет.

```rust
let executor = postgres::command_executor::<EmailReservation>(
    pool.clone(), PostgresCommandConfig::default(),
);
let gateway = Arc::new(CqrsEmailReservationGateway::new(executor));
```

`PostgresCommandConfig::new(interval, policy)` проверяет положительный interval,
хранит private `NonZeroUsize` и предоставляет getters. Defaults: interval=25 и
существующая conflict policy. Factory не выполняет I/O и не читает env,
поэтому возвращает executor напрямую без `Result`.

Согласованный scope: `Aggregate<Services = ()>` и пустые queries. Custom stores,
services и queries доступны через сохраненный low-level `CqrsCommandExecutor::new`.
Он используется в MemStore/управляемых race fixtures; обычные PostgreSQL
fixtures используют новую factory с прежними intervals=3/1.

Сохраняются один framework на execute, unchanged command/tasks при conflict
retries, state reload на каждую попытку, command-local immutable task list,
atomic event/snapshot/tasks и ограничение «не более одного event на command».
Shared mutable task buffer отсутствует; технические ошибки не получают fast retry.

Snapshot integration test выявил повторный `apply` при snapshot construction
в upstream `cqrs-es 0.5.0`. Текущие агрегаты используют state assignment и
совместимы с ним; для increment/append моделей нужен отдельный snapshot-contract
fix. Ограничение описано в [known issue](../known_issues/cqrs_snapshot_event_reapplication.md)
и публичной документации factory. Первый этап сохраняет механизм dependency.

Полный список тестов и проверок: [план этапа 1](../backlog/cqrs_runtime_command_factory.md).

## Этап 2. Outbox: checked configuration и prepare до запуска

Подробный план: [подготовка outbox](../backlog/cqrs_runtime_outbox_preparation.md).
Реализация завершена; следующие API доступны через runtime facades.

Реализован `worker::OutboxRuntimeConfig`, объединяющий существующие coordinator
и worker configs. Он загружает настройки через `from_env`/`from_lookup` и
проверяет combined lease budget. PostgreSQL statement timeout остается частью
PostgreSQL-конфигурации; он не становится обязательным для других queue adapters.

Общий API сборки:

```rust
let outbox = worker::prepare(queue, handler_factory, outbox_config)?;
// outbox.run(shutdown) запускается только после успешной подготовки.
```

Короткий PostgreSQL helper входит в этап; доступен при features postgres+worker:

```rust
let outbox = postgres::prepare_outbox_worker(
    pool.clone(), handler_factory, postgres_queue_config, outbox_config,
)?;
```

Helper только собирает queue и передает ее в `worker::prepare`. Он не запускает
background tasks, не читает env и не регистрирует бизнес-handlers сам.

`OutboxRuntimeConfig` проверяет combined budget при создании/загрузке.
`prepare` проверяет registry formats и строит coordinator.
Первый registry используется как
registry первого worker. Остальные C−1 registries тоже создаются и проверяются
до открытия HTTP listener; ordered formats должны совпадать. При restart
проверка сохраняется. `PreparedOutboxWorker` владеет registries и factory,
не клонируется и потребляется методом `run(self, shutdown)`.

Существующий low-level `worker::run` сохраняется для custom coordinators и
использует тот же supervisor engine. Подготовка не принимает CancellationToken
и не начинает storage work; cancellation/DropGuard относятся к начавшемуся run.
Combined runtime config загружается и проверяется до подключения БД.

Бизнес-код по-прежнему явно перечисляет handlers и создает отдельный mutable
sender для каждого worker. Не заменять factory общим registry под mutex и не
клонировать один `Box<dyn TaskHandler>` на все workers.

Подготовка C registries и очередь Q остаются разными ограничениями. Сохраняются
defaults C=128/Q=128/H=15s/S=6s/lease=60s, исходные deadlines, checked arithmetic,
unknown claim outcomes, replacement workers и bounded H+S drain.

## Этап 3. Host lifecycle: перенести готовую реализацию

[Сохраненный план и результаты реализации](../backlog/cqrs_runtime_host_lifecycle.md).
Lifecycle теперь принадлежит runtime; main передает outbox future без внешнего
spawn и закрывает pool после host независимо от результата. Default features
сохранены; `host` явно включен сервисом. Описанный ниже перенос реализован.

`cqrs-runtime::host` под feature `host`, включающей `worker` и `tokio/signal`,
владеет наблюдением signals, общим CancellationToken и координацией inbound
future/outbox. Существующий `runtime.rs` почти целиком переносится сюда.
Внутренние имена `HttpServer` можно обобщить до `Inbound`, не меняя алгоритм.

Начальная версия сохраняет future с `io::Result<()>`: она уже не зависит от
Axum. Generic inbound error имеет смысл, когда появится второй transport.
Не нужен новый supervisor для произвольного графа компонентов.

API должен принимать server future, outbox future и signal future; outbox
spawn-ится внутри host, после подготовки. Это сокращает работу со вложенными
`JoinHandle<Result<...>>` в сервисе. Production helper `shutdown_signal()`
покрывает SIGINT/SIGTERM; основная функция остается тестируемой через fake signal.

Сохранить раннюю отмену HTTP при fatal worker error, DropGuard, приоритет
requested/unexpected failures, логирование secondary failures и безопасное
форматирование JoinError без panic payload. `pool.close().await` остается после
ожидания обоих компонентов и выполняется также при runtime error.

HTTP router и `axum::serve(...).with_graceful_shutdown(...)` остаются в сервисе.
Для этого переноса runtime не нужна production dependency на Axum. Тест реального
Axum drain можно оставить в сервисной integration suite, остальные lifecycle
tests перенести в host.

Перенос сохраняет текущую политику: HTTP drain и DB close без host deadline.
Принудительное завершение уже отдельно отложено в
[`http_shutdown.md`](../known_issues/http_shutdown.md). Добавление watchdog,
таймаутов или force cancellation не входит в этот рефакторинг.

## Этап 4. Typed task adapter

[Сохраненный план и результаты реализации](../backlog/cqrs_runtime_typed_task_adapter.md).
Public helper возвращает `Result<Box<dyn TaskHandler>, TaskFormatError>`;
конкретный adapter приватный. Три сервисных factories сохраняют сигнатуры и
используют typed handlers. По согласованному расширению этапа sender-порт теперь
принимает `&TaskDelivery`, а owned `DeliveryContext` удален без compatibility alias.

В `outbox` добавить адаптер `typed_task_handler::<P, H>(handler)`, где
`P: TaskPayload + DeserializeOwned + Send + 'static`. Он реализует существующий
`TaskHandler`, регистрирует проверенный `TaskFormat::of::<P>()`, проверяет format
и десериализует payload перед вызовом typed business handler.

Пример ответственности бизнес-handler:

```rust
#[async_trait]
impl TypedTaskHandler<CreateAccountTaskV1> for CreateAccountTaskHandler {
    async fn handle(
        &mut self,
        payload: CreateAccountTaskV1,
        _delivery: &TaskDelivery,
    ) -> Result<(), TaskHandlerError> {
        self.service.create_account(payload).await.map_err(delivery_error)
    }
}
```

Использовать trait с обычным async method, совместимый с текущим async-trait.
Начальная версия не требует сложных closure bounds для futures, заимствующих
`&mut self`. Existing `TaskHandler` остается escape hatch для handlers нескольких
версий; v1/v2 также можно регистрировать как разные адаптеры.

Adapter возвращает `Permanent` при несовпадении формата или malformed payload;
reason содержит безопасные type/version, но не payload и не текст serde error.
Дополнительные поля старого DTO продолжают приниматься. Бизнес-проверка семи
цифр остается в verification handler, conversion v1 → текущий input тоже
принадлежит приложению. `delivery_error` и классификация exact repeat остаются
бизнес-решениями. В адаптер не добавляются retry, settlement или successor tasks.

Контекст handler и sender — исходный `&TaskDelivery`; ID, attempt и metadata
доступны через getters на протяжении async invocation. Metadata не клонируется
для передачи sender. Console сохраняет прежний output и write/flush error mapping.
Application handler tests и их фейки перенесены в service integration suite;
общие format/decode tests принадлежат runtime.

## Этап 5. Конфигурация, logging, диагностика и небольшие helpers

У сервиса появляется небольшой `ServiceConfig`: database URL, bind address и
вложенные runtime configs. Загрузка dotenv остается действием executable.
Runtime не должен автоматически читать `.env` или устанавливать subscriber.
Все настройки валидируются до подключения к БД; constructors без `from_env`
продолжают использовать явные defaults.

В `postgres` полезен fallible connection helper с явными pool options,
использующий SQLx вместо upstream panic helper. Сначала сохранить limit 10;
изменение pool sizing — отдельное решение, оно не следует автоматически из C=128.
Startup ошибки возвращаются типизированно, без `expect` на пользовательских
настройках. Constant checked defaults могут сохранять `expect`.

Логирование переносится в optional `telemetry`: `LoggingConfig` с данными,
`from_lookup` и `try_init`. Сохраняются поддерживаемые сейчас значения RUST_LOG
(`off/error/warn/info/debug/trace`) и default info. Полный EnvFilter — отдельное
расширение контракта, а не побочный эффект переноса. `try_init` возвращает ошибку,
если subscriber уже установлен; библиотека не изменяет global state неявно.

Диагностику storage failures улучшать там, где известна исходная причина:
safe category и aggregate type/operation, без raw commands, task payload,
aggregate ID-email, connection URL и произвольного error text. Внешний
`CommandGatewayError` остается трехкатегорийным; retry ownership не меняется.

Повторяющийся parse integer env можно собрать в приватный helper runtime.
Ограничения единиц/диапазонов и boundary error types остаются у configs.
Backoff command/outbox пока стоит оставить раздельным: у них разные бюджеты
и публичные контракты. Общий публичный `utils` модуль здесь не нужен.

## Что остается в бизнес-сервисе

| Код | Основание |
| --- | --- |
| Выбор aggregate ID в gateway | Canonical email для reservation и UUID для account отражают разные бизнес-границы. |
| Task DTOs, accepted data, handler operation и outcome classification | Определяют устойчивый бизнес-протокол и семантику exact redelivery. |
| RegistrationMaterialGenerator и его random adapter | Семь цифр, 256-bit token и UserAccountId выражают конкретный контракт регистрации. Здесь мало кода, перенос не упростит workflow. |
| RegistrationClock/SystemRegistrationClock | Порт определяет, где workflow наблюдает время; системный adapter сейчас однострочный. Общая clock hierarchy добавит больше кода, чем уберет. |
| ConsoleVerificationCodeSender | Содержит бизнес-message и специальный development effect. Блокирующий stdout — отдельная существующая задача №11; перенос не устраняет ее. |
| HTTP DTOs, маршруты, URL и ошибки | Даже одинаковый JSON envelope не делает HTTP contract частью CQRS. `presentation/http.rs` уже содержит общий primitive. Retry-After helper можно выделить локально при необходимости. |
| `main`: выбор и соединение бизнес-зависимостей | Список сервисов и handlers должен читаться прямо, без DI-container, reflection и регистрационных macros. |

## Как будет читаться composition root

Предлагаемые APIs используются последовательно: config → logging → DB → два
executors → два бизнес-gateways → бизнес-services → handler factory → prepared
outbox → router/listener → host → DB close. В `main` остаются явные бизнес-имена.
Технические repository/store/framework closures, budgets, signal select,
registry format extraction и worker JoinHandle больше не повторяются.

Ориентир — сократить нынешние 129 строк main примерно до 50–70 без учета imports;
это оценка, а не измеренный результат. Не стоит превращать его в вызов
`Application::auto_run()`: пользователь runtime должен видеть wiring и порядок
владения ресурсами.

## Этап 6. Cargo и CI

Cargo dependency versions повторяются в двух manifests. Их можно перенести в
`[workspace.dependencies]`, сохранив features/default-features/optional у consumers.
Это изменение workspace, не задача runtime library. После factory этапа 1 и fallible pool helper этапа 5
прямая production dependency сервиса на `postgres-es` станет не нужна;
`cqrs-es` остается нужна domain и gateway tests.

CI живет в [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) и проверяет
all-features. Для переносимости runtime полезна дополнительная compile matrix:
no-default-features, postgres only, worker only, затем host/telemetry после их
добавления. Cargo/CI helpers остаются на уровне repository tooling. Отдельный
build framework или `build.rs` для этой цели не требуется.

## Общие проверки этапов

Clean-architecture skill обновлен вместе с этапом 1: main вызывает техническую
factory, gateways получают ready executor. Этап 2 закрепляет runtime-owned
preparation и раннюю проверку budgets/registries. Этап 3 закрепляет
runtime host ownership, first-poll guard/spawn и самостоятельный outbox drain при
Drop/abort. DDD domain layout не меняется.

Каждый этап меняющий Rust проходит fmt, workspace Clippy, workspace tests и
llvm-cov с line coverage ≥90%, как требует AGENTS.md. Новые env settings отражаются
в `.env.example` и portable runtime guide. Перенос не требует миграции PostgreSQL
schema, event formats или task payloads.

При исходном review, до реализации этапа 1, production code и tests не менялись.
Тогда были выполнены:

- `cargo test --locked --workspace --all-features --lib --bins`: 104 passed;
- `cargo check --locked -p cqrs-runtime --no-default-features`;
- `cargo check --locked -p cqrs-runtime --no-default-features --features postgres`;
- `cargo check --locked -p cqrs-runtime --no-default-features --features worker`.

При исходном review PostgreSQL suites и llvm-cov не запускались. Эти исходные
checks не подтверждают реализацию; актуальные результаты реализации записаны в документах
[этапа 1](../backlog/cqrs_runtime_command_factory.md),
[этапа 2](../backlog/cqrs_runtime_outbox_preparation.md),
[этапа 3](../backlog/cqrs_runtime_host_lifecycle.md) и
[этапа 4](../backlog/cqrs_runtime_typed_task_adapter.md).
