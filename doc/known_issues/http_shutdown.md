# №10: HTTP shutdown и закрытие пула БД

Статус: открыто; перенесено из backlog 2026-10-11. HTTP force, process controller,
watchdog и host shutdown deadlines откачены по решению пользователя. Политику
принудительного завершения нужно обсудить до реализации.

## Текущее поведение

Асинхронный composition root использует штатный Tokio runtime. SIGTERM/SIGINT
наблюдаются через Tokio; `cqrs_runtime::host` отменяет общий `CancellationToken` и
ожидает HTTP/outbox. Ошибка supervisor отменяет токен до аварийного drain;
DropGuard владельца lifecycle тоже сохраняется.

HTTP использует `axum::serve(...).with_graceful_shutdown(...)`. Отмена токена
останавливает прием новых TCP-соединений и запускает graceful shutdown
существующих соединений. Текущие запросы, включая чтение тела и handlers,
продолжают выполняться без shutdown timeout или force-отмены.

Outbox сохраняет bounded drain H+S=21s при defaults H=15s/S=6s. Его supervisor
сам abort/join оставшихся workers после drain deadline; дополнительного host
abort/observation timer нет. SQL timeout=4s, lease=60s, C=Q=128, poll=1s и
safety margin=5s сохранены; lease >53s.

После завершения HTTP/outbox main вызывает `pool.close().await`, логирует результат
и возвращается. Закрытие пула и teardown Tokio также не имеют host timeout.
`controller.rs`, `config.rs`, системные signal/cancellation/watchdog threads,
`_exit(124)` и `shutdown_timeout` удалены. Настройки `HTTP_SHUTDOWN_GRACE_SECS`,
`SHUTDOWN_FORCE_TIMEOUT_SECS`, `DB_CLOSE_TIMEOUT_SECS` больше не поддерживаются.
Цель 28 секунд для выхода всего приложения пока не обеспечена host deadlines.

## Проблема и пример

Handler, бесконечно ожидающий внешний сервис, или клиент, не закончивший передачу
тела запроса, может удерживать HTTP graceful shutdown неопределенно долго.
Например, `std::future::pending::<()>().await` внутри handler не завершится
из-за отмены общего токена: handler этот токен не наблюдает.

Outbox уже может быть остановлен, но host продолжает ждать HTTP; до закрытия
БД выполнение не доходит. Даже после успешного HTTP drain `pool.close().await`
может ждать удерживаемое соединение. Блокирующая работа также может задержать
исполнение Tokio или его teardown. Watchdog отсутствует: завершение возможно
только после окончания работы либо внешнего принудительного завершения процесса.

Timeout только вокруг server future не доказывает остановку отдельно
запущенных connection/request tasks. Закрытие соединения или принудительный
выход процесса не доказывают rollback: SQL или внешний эффект могли выполниться.
При потере ответа commit outcome может оставаться неизвестным; повтор HTTP
сам по себе не становится идемпотентным.

## Что нужно решить перед реализацией

- Выбрать политику для текущих запросов после grace: продолжать ждать,
  кооперативно отменять работу или принудительно уничтожать futures.
- Определить обработку partial bodies, HTTP/1, HTTP/2, streaming/upgrades и
  отдельно запущенных фоновых задач; подтвердить фактическое завершение работы.
- Согласовать момент закрытия БД с выбранной HTTP-политикой и общий process
  budget. Не закрывать pool admission во время обычного drain.
- Решить, нужен ли независимый process watchdog, или достаточно внешнего
  лимита orchestrator, и учесть блокировку Tokio/logging/teardown.
- Сохранить unknown commit semantics, первичную ошибку и безопасные diagnostics.
- Проверить hang, повторные сигналы, committed operation без ответа и границы
  каждого согласованного дедлайна интеграционными тестами.

Согласованы H=15s/S=6s/SQL=4s/lease=60s. Плановый envelope для цели 28s:
max(HTTP drain, H+S) <=21s, observation 2s, DB cleanup 3s и teardown/reserve 2s.
При Kubernetes grace=30s остаются еще 2s внешнего запаса, если нет длительного
preStop и TERM доставлен своевременно. HTTP/observation/DB/teardown budgets пока
не реализованы как host deadlines; это план для обсуждения решения №10.
См. [timeline в README](../../README.md#graceful-shutdown-timeline).

Область: [composition root](../../account-managements-service/src/main.rs),
[runtime host](../../cqrs-runtime/src/host.rs) и
[portable runtime guide](../../cqrs-runtime/doc/task_processing.md).
Перенос host и согласованная семантика Drop/abort описаны в
[этапе 3](../backlog/cqrs_runtime_host_lifecycle.md): Drop отменяет token и позволяет
outbox самостоятельно drain в работающем Tokio runtime, но не гарантирует
ожидание HTTP и закрытие БД. №11 (блокирующий console sender) остается отдельной задачей.
