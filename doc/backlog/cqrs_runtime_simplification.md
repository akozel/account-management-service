# Упрощение cqrs-runtime

Статус: backlog. Последняя проверка рабочего дерева: 2026-10-11.
Задачи №1 и №2 выполнены и прошли обязательные проверки.
Согласованы независимые process-wide defaults C=128/Q=128 и
H=15s/S=6s/SQL=4s/lease=60s. №10 перенесена в known issues.

## Задачи

| № | Задача | Статус | Ожидаемый результат |
| --- | --- | --- | --- |
| 1 | [JoinSet, постоянные workers и независимые ограничения очереди/конкурентности](cqrs_runtime_join_set_workers.md) | Выполнено | Handler registry принадлежит одной Tokio task; bounded queue управляет ожиданием; JoinSet управляет завершением, panic и abort. |
| 2 | Общая процедура shutdown и drain при ошибках supervisor; CancellationToken | Выполнено | Общий токен HTTP/outbox отменяется до аварийного drain; начатые операции получают H+S, исходная ошибка сохраняется, сопутствующие ошибки логируются безопасно. |
| 3 | Оценить обычную SQLx-транзакцию вместо динамического CTE публикации | Backlog | Более простой SQL для event + snapshot + tasks при сохранении атомарности и optimistic lock; измерить цену дополнительных round trips. |
| 4 | Batch recovery просроченных leases | Backlog | Заменить SELECT + N UPDATE пакетным обновлением, сохранив блокировки, индивидуальный jitter, attempts и подтвержденные failures после commit. |
| 5 | Проверить целесообразность готового PostgreSQL runner | Backlog: исследование | Прототип sqlxmq или Apalis: атомарное enqueue с event-store, версии DTO, attempts, failure, lease/recovery и миграция существующих записей. |
| 6 | Оценивать streams и дополнительные примитивы по сокращению конкретного кода | Отложено | tokio-stream, buffer_unordered и Semaphore вводить при явной необходимости. Они сами по себе не заменяют durable queue, supervision и settlement. |
| 7 | Удалить локальный perf-harness после выбора очереди | Выполнено | Модуль performance, smoke-тест его измерений, инструкции, отчеты и CSV удалены. Оставлены поведенческие unit-тесты runtime. |
| 8 | Миграция на async-channel 2.5.0 | Выполнено | Клонируемые receivers, один producer, независимые C/Q; старые реализации, адаптеры, тесты и сравнительные бенчмарки удалены. |
| 9 | Проверка успешного выполнения 1000 задач | Выполнено | Реальные supervisor, async-channel, registry и coordinator с тестовым storage; каждый ID обработан один раз и получил Completed. |
| 11 | Заменить или изолировать блокирующий console sender | Backlog; отдельно от №2 | Зависший stdout не блокирует кооперативный runtime и завершение процесса; подтверждать успешную отправку только после выполнения обязательного эффекта. |

## Границы изменений

- Сохранить разделение command / outbox / postgres / worker и независимость
  runtime от бизнес-приложения.
- Сохранить атомарную публикацию event, optional snapshot и explicit tasks.
- Сохранить at-least-once, версии task payload, обработку exact redelivery
  приложением и независимость бизнес-последовательности от settlement.
- Сохранить lock-token и expiry fencing, исходный monotonic lease deadline,
  storage deadlines и различие requested/confirmed settlement.
- Ошибка или timeout commit оставляют неизвестный результат; новая реализация
  не должна интерпретировать их как доказанный rollback.
- Не менять ограничение «один event на команду» в задачах 1–4.

## Исходные точки

- [Supervisor](../../cqrs-runtime/src/worker/supervisor.rs).
- [Permanent worker](../../cqrs-runtime/src/worker/worker_task.rs).
- [Coordinator](../../cqrs-runtime/src/outbox/coordinator.rs).
- [Atomic publication](../../cqrs-runtime/src/postgres/event_repository_with_outbox/write.rs).
- [PostgreSQL queue](../../cqrs-runtime/src/postgres/outbox_queue.rs).
- [Task processing contract](../../cqrs-runtime/doc/task_processing.md).

## Проверка реализации

Для изменений Rust выполнить обязательные repository checks: fmt, clippy,
workspace tests и llvm-cov с порогом 90%. При новой настройке обновить
`.env.example`, переносимый runtime guide и соответствующий rustdoc.
Задачу считать выполненной только после реализации и проверок; завершенный
дизайн отдельно не означает завершенный рефакторинг.

Миграция очереди (№8) сохраняет публичные интерфейсы, defaults C=128/Q=128/lease=120s,
исходные deadlines и bounded drain. Единственный poller берет claim limit из
Q - sender.len(); consumers могут только освобождать место во время claim.
Тесты проверяют отмену notified receive без потери доставки и освобождение
очереди во время claim без второго параллельного запроса.

Локальные perf-эксперименты помогли выбрать очередь и удалены после миграции.
В unit-тесте supervisor проверяются 1000 уникальных доставок: выполнение каждой
ровно один раз и успешный Completed settlement для каждого ID. C=16/Q=32
проверяют несколько batch/refill циклов; часовой polling interval позволяет
проверить refill через Notify. Idle shutdown остается отдельным unit-тестом
без измерительной инфраструктуры.

После удаления perf-модуля обязательные проверки прошли: fmt, workspace Clippy
с `-D warnings`, workspace tests (142 passed, включая PostgreSQL и doctests;
без ignored тестов) и llvm-cov с порогом 90%. Line coverage workspace: 97.89%.

## Проверка graceful shutdown и границы задачи №2

Согласована прямая миграция worker::run на CancellationToken без watch-адаптера.
Host передает общий токен HTTP, supervisor и process runtime. Ошибка startup/restart
отменяет его до возврата/drain; отдельный panic worker приводит к restart.
Для изолированного пула можно передать child_token. DropGuard владельца lifecycle
заменяет сигнал закрытия watch sender; обычный Drop token handle не отменяет токен.
Начатые handler/settlement завершаются в общем H+S; ожидающие claims остаются для
lease recovery. Supervisor abort отменяет токен и JoinSet workers без async drain.

HTTP и outbox останавливаются одновременно. Завершающийся HTTP-запрос может
опубликовать durable tasks после остановки workers; они ждут следующего запуска.
Ошибка первого неожиданно завершившегося компонента сохраняется. При сигнале/токене
приоритет ошибок: signal observation, supervisor, HTTP. Сопутствующие ошибки
логируются отдельно; JoinError не форматируется вместе с panic payload.

Задача [№10: HTTP shutdown и закрытие БД](../known_issues/http_shutdown.md)
перенесена в known issues. HTTP ожидает текущие запросы без дедлайна;
закрытие БД начинается после HTTP/outbox и тоже не имеет timeout.
Незавершенный commit остается неизвестным.
Для №11 перенос в spawn_blocking сам по себе не отменяет начатый синхронный write/flush.
Нельзя подтверждать Completed только за постановку сообщения в память вместо
подтверждения обязательного эффекта. Worker drain ограничен кооперативным исполнением;
временной предел для всего процесса пока не гарантирован.

После реализации №2 обязательные проверки прошли: fmt, workspace Clippy с
`-D warnings`, workspace tests (152 passed, включая PostgreSQL и doctests;
без ignored тестов) и llvm-cov с порогом 90%. Line coverage workspace: 97.95%.
Проверены ранняя отмена HTTP до завершения аварийного drain, общий токен во время
pending claim, restart-factory ошибки и несовпадение форматов, supervisor panic/abort,
приоритет одновременных событий и сохранение committed claim/settlement в PostgreSQL.
