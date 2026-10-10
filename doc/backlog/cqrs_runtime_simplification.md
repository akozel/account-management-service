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
| 7 | [Performance test текущей очереди и bounded MPMC](../../cqrs-runtime/doc/queue_performance.md) | Выполнено | Настоящий run_worker как baseline; 128/1000 workers, 1/4 runtime threads, fast/yield/I/O, 168 samples с latency percentiles и raw CSV. |
| 8 | Расширить benchmark на Flume и Kanal | Выполнено | Четыре backend в одном release-прогоне, восемь сбалансированных повторений, 384 samples; прежние результаты архивированы. |
| 9 | [Имитация HTTP 5 мс на четырех runtime threads](../../cqrs-runtime/doc/queue_performance_results_io5ms.md) | Выполнено | Четыре очереди, C=Q=128/1000, 100 доставок на worker, 64 release samples; отдельные report/CSV. |

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

Проверка первой задачи: `cargo fmt --all --check`, workspace Clippy с `-D warnings`,
workspace tests (138 passed, включая PostgreSQL и doctests) и обязательный
`cargo llvm-cov` с порогом 90% прошли. Line coverage workspace: 97.83%.
Есть проверка 1000 одновременно ожидающих I/O workers на одном потоке Tokio;
это функциональный тест конкурентности, не throughput benchmark реального HTTP.

Performance comparison (задача 7) выполнен отдельно в release; production queue
не переключена на MPMC. [Результаты](../../cqrs-runtime/doc/queue_performance_results.md)
и [raw CSV](../../cqrs-runtime/doc/queue_performance_results.csv) сохранены.
После добавления benchmark workspace checks прошли: 139 тестов, один manual
benchmark ignored в обычном запуске, line coverage 96.97%.

Расширение comparison (задача 8): четыре backend заново измерены в одном release
прогоне, 384 samples. Порядок сбалансирован по позициям и парам; проверки доставки
прошли. Старый двухканальный прогон сохранен отдельным архивом, без смешивания
samples. Workspace checks прошли: 141 тест, один manual benchmark ignored;
line coverage 97.16%. Актуальные таблицы и CSV доступны по ссылкам выше.
