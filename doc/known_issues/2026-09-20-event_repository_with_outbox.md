# Review `PostgresEventRepositoryWithOutbox`

Дата: 2026-09-20

Область review:

- [`src/infrastructure/postgres/event_repository_with_outbox.rs`](../../src/infrastructure/postgres/event_repository_with_outbox.rs)
- схема `transactional_outbox`;
- постановка, claim, retry и подтверждение outbox-задач;
- взаимодействие с snapshot-механизмом `cqrs-es`.

## Итог

Атомарная часть outbox реализована корректно. Это настоящий transactional
outbox, а не dual-write: domain events, snapshot и outbox tasks записываются
одним PostgreSQL statement в одной неявной транзакции.

При этом весь механизм доставки предоставляет семантику **at-least-once без
гарантии порядка**, а не exactly-once. Перед production-эксплуатацией необходимо
определить требования к порядку задач одного aggregate, обеспечить
идемпотентность внешних обработчиков и добавить retention для завершённых
записей.

## Найденные проблемы

### P1 при появлении multi-event команд: snapshot получает неверный `last_sequence`

В `persist_batch` snapshot всегда получает sequence последнего события batch:

```rust
let last_sequence = event_sequences.last().copied().unwrap_or_default();
```

При этом `cqrs-es` может построить snapshot только по префиксу текущего batch.
Например, при snapshot interval `2` и трёх новых событиях snapshot payload
отражает состояние после sequence `2`, но repository сохранит для него
`last_sequence = 3`. При следующей загрузке событие с sequence `3` будет
пропущено, и состояние aggregate окажется неполным.

Сейчас aggregate-команды сервиса генерируют по одному событию, поэтому дефект
фактически не проявляется. Тем не менее generic repository небезопасен для
multi-event команд.

Возможные решения:

- исправить или форкнуть интеграцию с `cqrs-es`, чтобы вместе со snapshot
  передавался его фактический event sequence;
- отказаться от snapshot store до исправления контракта;
- как временное ограничение явно закрепить и проверить invariant «одна команда
  создаёт не более одного события».

Смежный случай: прямой вызов `persist([], Some(snapshot))` разрешён и сохранит
`last_sequence = 0`. Такой вызов следует отклонять, если repository не может
получить фактическую sequence snapshot.

### P1/P2: задачи одного aggregate могут выполняться не по порядку

Repository сохраняет `aggregate_sequence` и правильно связывает задачи с
последним событием batch. Однако queue выбирает задачи только по
`available_at, id`, а worker pool исполняет их параллельно. Поле
`aggregate_sequence` при claim не используется.

Возможный сценарий для verification codes:

1. отправка старого кода завершается временной ошибкой и уходит в retry;
2. новый код создаётся и успешно отправляется;
3. старая задача становится доступной позднее и отправляет уже невалидный код.

Если порядок внутри aggregate является частью бизнес-контракта, необходима
per-aggregate сериализация. Для verification codes может лучше подойти
семантика supersede/latest-wins, чтобы новая отправка не блокировалась за
повторными попытками устаревшей задачи.

### Resolved: классификация PostgreSQL `23505`

Статус: исправлено. Регрессионный тест находится в
[`tests/outbox/persistence.rs`](../../tests/outbox/persistence.rs).

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

- Snapshot lock, events и outbox связаны через `RETURNING` CTE.
- При stale snapshot ни events, ни outbox tasks не вставляются.
- Ошибка вставки event, snapshot или outbox task откатывает весь statement.
- Outbox task нельзя поставить без зафиксированного domain event.
- Повторный task ID не проглатывается через `ON CONFLICT DO NOTHING`.
- Перед записью проверяются переполнение event sequence, duration и лимит bind
  parameters PostgreSQL.
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

- snapshot на границе interval при batch из нескольких событий;
- out-of-order выполнения двух задач одного aggregate;
- ~~отдельной классификации дубликата `transactional_outbox.id`~~ — исправлено,
  покрыто регрессионным тестом;
- cleanup/retention завершённых задач.
