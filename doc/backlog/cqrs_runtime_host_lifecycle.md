# Этап 3: перенос lifecycle процесса в cqrs-runtime::host

Связанное [review](../reviews/cqrs_runtime_reuse.md#этап-3-host-lifecycle-перенести-готовую-реализацию).
Статус: выполнено 2026-10-11; реализация и все обязательные проверки завершены.

## Цель и границы

Перенести signals, cancellation и завершение компонентов из сервисного runtime
в runtime library. Main создает бизнес-зависимости и HTTP adapter, вызывает host,
закрывает БД независимо от результата, затем логирует и возвращает HostError.
Удалить приватный сервисный runtime без compatibility wrapper.

Сохранить policy: outbox самостоятельно ограничивает drain бюджетом H+S;
HTTP drain и pool.close не имеют host timeout. Watchdog и force termination
остаются [known issue #10](../known_issues/http_shutdown.md). Новых env variables
и миграций данных нет; DTOs и бизнес-поведение не меняются.

## API и feature

Optional `host = ["worker", "tokio/signal"]`; defaults остаются postgres+worker.
Сервис явно включает host, runtime не зависит от Axum. Facade экспортирует:

```rust
pub async fn run<Inbound, Outbox, Signal>(
    inbound: Inbound,
    outbox: Outbox,
    shutdown: CancellationToken,
    signal: Signal,
) -> Result<(), HostError>
where
    Inbound: Future<Output = io::Result<()>>,
    Outbox: Future<Output = Result<(), WorkerPoolError>> + Send + 'static,
    Signal: Future<Output = io::Result<()>>;

pub async fn shutdown_signal() -> io::Result<()>;
```

Host spawn-ит outbox внутри Tokio task. Inbound и signal исполняются внутри host
future без дополнительных Send/'static ограничений. Приватные модули: lifecycle,
error, signal. HostError категории: ShutdownSignal, Inbound, InboundStopped,
OutboxSupervisor, OutboxSupervisorTask, OutboxSupervisorStopped. Debug и log
отображают JoinError через task ID и panic/cancellation flags без panic payload;
оригинал сохраняется как source. Обычный panic hook независим от этих diagnostics.

## Lifecycle и сигналы

На первом poll установить cancellation DropGuard **до spawn outbox**. Простое
создание host future ничего не запускает; Drop future до первого poll token не
отменяет. Biased-приоритет: signal → token cancellation → inbound exit → outbox
exit. После stop request отменить общий token и дождаться inbound/outbox.
Первый неожиданный exit остается основной ошибкой. При signal/token приоритет
ошибок: signal observation → outbox → inbound; вторичные ошибки логируются.
Fatal outbox failure отменяет общий token до завершения собственного drain,
позволяя inbound начать shutdown немедленно.

Drop/abort host future после первого poll отменяет token и отпускает JoinHandle без
немедленного abort supervisor. Начатая outbox работа может самостоятельно drain
в работающем Tokio runtime. Host при Drop не может дождаться компонентов или
гарантировать DB cleanup; inbound future уничтожается. Normal await возвращается
после обоих компонентов, до начала cleanup.

Unix helper наблюдает ctrl_c и SIGTERM; другие платформы — ctrl_c. Закрытие
SIGTERM stream остается io::Error. Приватный signal multiplexer принимает
controlled futures; тесты не отправляют OS signals процессу.

## Интеграция и документация

Main передает `outbox.run(shutdown.clone())` в host::run без внешнего spawn.
HTTP использует штатный Axum graceful shutdown с тем же token. После await
pool.close выполняется независимо от результата. Unit lifecycle tests перенесены
в runtime и принимают worker futures. Реальный Axum shutdown test перенесен в
сервисную integration suite. Runtime guides, архитектурная Mermaid-диаграмма,
clean-architecture skill и ссылки HTTP shutdown обновляются вместе с API.

## Тесты и приемка

Сохранить signal/error/token shutdown, неожиданный успешный exit, inbound
failure, outbox failure/panic, раннюю отмену inbound до конца outbox drain,
одновременные события, приоритет ошибок и безопасные diagnostics.

Добавить проверки нового владения: unpolled host ничего не запускает; Drop/abort
после первого poll отменяет token и позволяет закончить начатую outbox работу;
normal await ждет обеих компонент до cleanup. Проверить borrowed non-Send
inbound/signal. Axum integration test подтверждает остановку TCP admission,
завершение активного запроса и последующий cleanup. Controlled signal tests
покрывают interrupt success/error и terminate success/closed stream.

Обязательные checks: fmt; workspace Clippy с -D warnings; workspace tests включая
PostgreSQL; llvm-cov с line coverage ≥90% (main исключен согласно AGENTS.md).
Дополнительно check runtime без default features, с worker, с postgres, с host.

Этап завершен, когда lifecycle принадлежит runtime, main использует новый API,
согласованная семантика сохраняется и все проверки проходят.

## Результаты проверок

Проверено на итоговой реализации (Rust 1.98.1):

| Проверка | Результат |
| --- | --- |
| `cargo fmt --all --check` | Passed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Passed |
| `cargo test --workspace --all-features` | 176 passed, включая PostgreSQL suites и реальный Axum drain |
| `cargo llvm-cov --workspace --all-features --color never --ignore-filename-regex '(^|/)main\.rs$' --fail-under-lines 90` | Passed: 97.79% line coverage (6326 lines, 140 missed) |
| `cargo check -p cqrs-runtime --no-default-features` | Passed |
| `cargo check -p cqrs-runtime --no-default-features --features worker` | Passed |
| `cargo check -p cqrs-runtime --no-default-features --features postgres` | Passed |
| `cargo check -p cqrs-runtime --no-default-features --features host` | Passed |
| `git diff --check` | Passed |

Все команды выполнялись через обязательный RTK proxy. PostgreSQL tests используют
существующий testcontainers fixture и доступный Podman socket; suites не пропускались.
22 host tests также прошли 20 последовательных запусков с параллельным test runner.

При повторной проверке обнаружилась гонка инициализации tracing callsite в тесте
secondary logs: соседний тест без subscriber мог первым закэшировать disabled
interest. Перед сбором лога тест теперь инициализирует общий callsite и обновляет
interest внутри своего scoped subscriber. Production diagnostics и error priority
сохранены; итоговые workspace tests и coverage повторно прошли.

Lifecycle находится в `cqrs-runtime/src/host/`, API экспортируется через
`cqrs-runtime/src/host.rs`; сервисный runtime удален. Main вызывает host и закрывает
pool после результата. HTTP adapter и integration test принадлежат сервису.
Runtime guides, Mermaid architecture, clean-architecture skill, review и known issue
синхронизированы. Shutdown policy остается прежней: H+S только у outbox,
HTTP/pool closure без host timeout, Drop/abort без гарантий DB cleanup.
