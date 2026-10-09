# Review `PostgresEventRepositoryWithOutbox`

Дата: 2026-09-20

Область review:

- [`cqrs-runtime/src/postgres/event_repository_with_outbox.rs`](../../cqrs-runtime/src/postgres/event_repository_with_outbox.rs)
- схема `transactional_outbox`;
- постановка, claim, retry и подтверждение outbox-задач;
- взаимодействие с snapshot-механизмом `cqrs-es`.

## Итог

Атомарная часть outbox реализована корректно. Это настоящий transactional
outbox, а не dual-write: domain events, snapshot и outbox tasks записываются
одним PostgreSQL statement в одной неявной транзакции.

При этом весь механизм доставки предоставляет семантику **at-least-once без
гарантии порядка**, а не exactly-once. Контракт последовательности принят
2026-10-10: её обеспечивает машина состояний aggregate, tasks могут выполняться
параллельно; см. [контракт](../task_processing_contract.md). Перед
production-эксплуатацией необходимо обеспечить
идемпотентность внешних обработчиков и добавить retention для завершённых
записей.

## Найденные проблемы

### Resolved: multi-event и snapshot-only writes отклоняются

Статус: исправлено при выделении `cqrs-runtime`. Регрессионный тест находится в
[`cqrs-runtime/tests/postgres/persistence.rs`](../../cqrs-runtime/tests/postgres/persistence.rs).

В `persist_batch` snapshot всегда получает sequence последнего события batch:

```rust
let last_sequence = event_sequences.last().copied().unwrap_or_default();
```

При этом `cqrs-es` может построить snapshot только по префиксу текущего batch.
Например, при snapshot interval `2` и трёх новых событиях snapshot payload
отражает состояние после sequence `2`, но repository сохранит для него
`last_sequence = 3`. При следующей загрузке событие с sequence `3` будет
пропущено, и состояние aggregate окажется неполным.

Все aggregate-команды сервиса генерируют не более одного события. Runtime
теперь проверяет этот safety contract до выполнения SQL:

- batch из нескольких событий отклоняется;
- snapshot без события отклоняется;
- outbox tasks без события отклоняются;
- пустой batch без snapshot и tasks остаётся допустимым no-op.

Для поддерживаемого one-event batch snapshot получает sequence именно этого
события, а event, snapshot и tasks фиксируются одним statement. Поддержка
multi-event команд по-прежнему требует отдельного изменения контракта, чтобы
repository получал фактическую sequence snapshot.

### Resolved by contract: задачи одного aggregate могут выполняться не по порядку

Статус: архитектурный контракт принят 2026-10-10. Очередь допускает параллельное
исполнение; последовательность процесса обеспечивают состояния aggregate и
создание следующей task с событием, подтверждающим её предпосылку. Изменение
claim на per-aggregate FIFO не требуется. См. [контракт](../task_processing_contract.md).

Repository сохраняет `aggregate_sequence` и правильно связывает задачи с
последним событием batch. Однако queue выбирает задачи только по
`available_at, id`, а worker pool исполняет их параллельно. Поле
`aggregate_sequence` при claim не используется.

Возможный сценарий для verification codes:

1. отправка старого кода завершается временной ошибкой и уходит в retry;
2. новый код создаётся и успешно отправляется;
3. старая задача становится доступной позднее и отправляет уже невалидный код.

Если этапы зависимы, aggregate должен разрешать следующий этап только после
подтверждённого результата предыдущего; уже поставленную внешнюю task состояние
aggregate не останавливает. Для verification codes может лучше подойти
семантика supersede/latest-wins, чтобы новая отправка не блокировалась за
повторными попытками устаревшей задачи.

### Resolved: классификация PostgreSQL `23505`

Статус: исправлено. Регрессионный тест находится в
[`cqrs-runtime/tests/postgres/persistence.rs`](../../cqrs-runtime/tests/postgres/persistence.rs).

`map_sqlx_error` превращает любое нарушение unique constraint в
`PersistenceError::OptimisticLockError`:

```rust
if database_error.code().as_deref() == Some("23505") {
    return PersistenceError::OptimisticLockError;
}
```

Это корректно для `events_pkey` и конкурентного создания snapshot, но неверно
для `transactional_outbox_pkey`. Повторный `outbox.id` будет показан как
конфликт версии aggregate и может запустить бессмысленные CQRS retries.

Теперь adapter проверяет `database_error.constraint()`: только `events_pkey` и
`snapshots_pkey` считаются optimistic conflict. `transactional_outbox_pkey` и
неизвестные unique constraints возвращаются как unexpected/unavailable, а
единый statement полностью откатывается.

### P2, эксплуатационное: отсутствует retention/cleanup

Записи со статусами `completed` и `failed` остаются в
`transactional_outbox` навсегда. Partial indexes защищают claim-запросы от
сканирования завершённых записей, но таблица, backups и объём WAL будут
неограниченно расти.

Нужен периодический batch purge или archive по `finished_at` с явно заданным
сроком хранения.

## Что реализовано корректно

- Snapshot lock, единственное event и outbox связаны через `RETURNING` CTE.
- При stale snapshot ни events, ни outbox tasks не вставляются.
- Ошибка вставки event, snapshot или outbox task откатывает весь statement.
- Outbox task нельзя поставить без зафиксированного domain event.
- Повторный task ID не проглатывается через `ON CONFLICT DO NOTHING`.
- До SQL проверяются one-event invariant, наличие event для snapshot/tasks,
  переполнение event sequence, duration и лимит bind parameters PostgreSQL.
- Claim использует `FOR UPDATE SKIP LOCKED`, позволяя нескольким worker-ам
  безопасно разбирать очередь.
- Lease и `lock_token` fencing не позволяют старому владельцу подтвердить
  задачу после потери lease.
- Ограничения таблицы согласованно проверяют статусы, attempts, lock-поля и
  `finished_at`.

Использование modifying CTE здесь корректно: PostgreSQL выполняет каждый
data-modifying CTE ровно один раз в рамках одного statement. Зависимость
`written_snapshot -> written_events -> written_tasks` передаётся через
`RETURNING`, поэтому sibling CTE не требуется видеть изменения таблиц друг
друга.

## Гарантии доставки

Exactly-once delivery не обеспечивается. Handler сначала выполняет внешний
side effect, а затем отдельным запросом переводит task в `completed`. Падение
процесса или потеря соединения между этими операциями приведёт к повторной
доставке после истечения lease.

Это нормальная модель at-least-once. Для её корректного использования:

- все внутренние continuation handlers должны быть идемпотентными;
- внешний verification-code sender должен использовать стабильный `task_id`
  как idempotency key либо явно допускать повторную отправку;
- нельзя интерпретировать успешную постановку task как завершение внешнего
  действия.

Текущий интерфейс передаёт `task_id` в `DeliveryContext`, а внутренние
continuation handlers обрабатывают точное повторение как успех, поэтому база
для идемпотентной доставки уже есть.

## Проверка

Выполнены точечные интеграционные тесты с реальным PostgreSQL:

```text
cargo test --test outbox persistence
cargo test: 5 passed, 9 filtered out
```

Существующие тесты хорошо покрывают атомарный commit/rollback, stale snapshot,
duplicate event sequence и валидацию outbox task.

Не хватает регрессионных тестов для:

- поведения конкретного workflow при out-of-order выполнении tasks; общий
  запрет такого исполнения не входит в принятый контракт;
- ~~отдельной классификации дубликата `transactional_outbox.id`~~ — исправлено,
  покрыто регрессионным тестом;
- cleanup/retention завершённых задач.
