# CQRS runtime: единая терминология и границы компонентов

Дата: 2026-10-12. План реализован без compatibility aliases.

Полное актуальное дерево компонентов, назначение каждого и правила именования:
[Components and terminology](../../cqrs-runtime/doc/components.md).

## Результат

Бизнес-операция реализует `task::TaskHandler<P>::handle`. Сериализованный вход
проходит через `TaskAdapter::invoke`; стандартный приватный `JsonTaskAdapter`
проверяет формат и декодирует JSON. `TaskDispatcher::dispatch` выбирает adapter
по type/version. Dispatcher больше не реализует интерфейс adapter или handler.

`TaskExecutor` ограничивает и подтверждает одну leased попытку. Из прежнего
coordinator выделен приватный runtime Poller: он выполняет rotating claims и
recovery, сохраняя partial results и общий deadline batch. Отдельного публичного
polling trait нет. Supervisor остаётся единственным producer, а постоянные
workers сохраняют собственные mutable dispatchers и бизнес-state между попытками.

`runtime::prepare` и `postgres::prepare_task_runtime` возвращают готовый
`TaskRuntime`; low-level `runtime::run` получает queue и executor отдельно,
проверяет фактический storage budget и использует тот же lifecycle engine.
Queue wiring стандартного runtime связывает poller и executor с одной очередью.

Данные операции и платформенный контекст теперь называются `NewTask`,
`TaskAttempt`, `TaskLease`, `LeasedTask`. `LeasedTask::attempt().number()`
однозначно обозначает номер claim. Хранение реализуется через `TaskQueue` и
`PostgresTaskQueue`. Public facades: command, task, runtime, postgres, host, logging.

Ошибки/configs/factories обновлены вместе с consumers. Бизнес-factories имеют
имена `*_task_adapter`. Host диагностирует task runtime; original sources и
безопасное форматирование ошибок сохранены. Skills, README, guides, диаграммы и
комментарии env example отражают новые границы.

## Сохранённые контракты

- Cargo features, env names, SQL schema/assets и production SQL blocks не изменены.
- TASK_TYPE/TASK_VERSION и payload formats бизнес-задач не изменены.
- Atomic event/snapshot/tasks, immutable command retries и replay сохраняются.
- Ровно C startup dispatchers; первый удерживается для worker zero и formats.
- Один producer, bounded DispatchBuffer, исходные leases и token/expiry fencing.
- Cancellation перед закрытием channel, H+S drain и приоритет host failures.
- UnitOfWork не вводится: handler effect и settlement имеют отдельные commits.

## Проверки

- Workspace fmt и Clippy с `-D warnings` проходят.
- Workspace all-features tests: 208 passed, включая PostgreSQL suites и doctests.
- Line coverage: 97.90%; обязательный порог 90% выполнен.
- All-targets feature matrix: base, postgres, worker, host, logging, все features.
- Workspace rustdoc собирается; все пять Rust examples README проходят doctests
  во временном внешнем crate без изменения workspace.
- Canonical SQL, production SQL blocks и task type/version constants сравнены
  с исходной ревизией и совпадают.

Polling deadline tests перенесены к Poller; executor tests проверяют только одну
попытку. PostgreSQL timeout suite отдельно моделирует cancellation durable queue
await, сохраняя проверки lost replies и original lease clock. Dispatcher/adapter
граница дополнительно закреплена compile-fail doctest.
