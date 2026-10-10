# Упрощение cqrs-runtime

Статус: backlog. Анализ выполнен по текущему рабочему дереву 2026-10-10.
Первая задача выполнена и прошла обязательные проверки.
Согласованы независимые process-wide defaults C=128/Q=128 и lease=120 секунд.

## Задачи

| № | Задача | Статус | Ожидаемый результат |
| --- | --- | --- | --- |
| 1 | [JoinSet, постоянные workers и независимые ограничения очереди/конкурентности](cqrs_runtime_join_set_workers.md) | Выполнено | Handler registry принадлежит одной Tokio task; bounded queue управляет ожиданием; JoinSet управляет завершением, panic и abort. |
| 2 | Общая процедура shutdown и drain при ошибках supervisor | Drain реализован; CancellationToken отложен | Ошибка factory при restart проходит через согласованную процедуру завершения остальных workers. При необходимости рассмотреть CancellationToken вместо watch<bool>. |
| 3 | Оценить обычную SQLx-транзакцию вместо динамического CTE публикации | Backlog | Более простой SQL для event + snapshot + tasks при сохранении атомарности и optimistic lock; измерить цену дополнительных round trips. |
| 4 | Batch recovery просроченных leases | Backlog | Заменить SELECT + N UPDATE пакетным обновлением, сохранив блокировки, индивидуальный jitter, attempts и подтвержденные failures после commit. |
| 5 | Проверить целесообразность готового PostgreSQL runner | Backlog: исследование | Прототип sqlxmq или Apalis: атомарное enqueue с event-store, версии DTO, attempts, failure, lease/recovery и миграция существующих записей. |
| 6 | Оценивать streams и дополнительные примитивы по сокращению конкретного кода | Отложено | tokio-stream, buffer_unordered и Semaphore вводить при явной необходимости. Они сами по себе не заменяют durable queue, supervision и settlement. |
| 7 | Удалить локальный perf-harness после выбора очереди | Выполнено | Модуль performance, smoke-тест его измерений, инструкции, отчеты и CSV удалены. Оставлены поведенческие unit-тесты runtime. |
| 8 | Миграция на async-channel 2.5.0 | Выполнено | Клонируемые receivers, один producer, независимые C/Q; старые реализации, адаптеры, тесты и сравнительные бенчмарки удалены. |
| 9 | Проверка успешного выполнения 1000 задач | Выполнено | Реальные supervisor, async-channel, registry и coordinator с тестовым storage; каждый ID обработан один раз и получил Completed. |

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

Миграция сохраняет публичные интерфейсы, defaults C=128/Q=128/lease=120s,
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
