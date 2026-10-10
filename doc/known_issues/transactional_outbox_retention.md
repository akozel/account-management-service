# Transactional outbox: retention

Статус: открыто. Автоматический purge/archive в runtime не реализован.

Записи `completed` и `failed` остаются в `transactional_outbox`. Partial
indexes ограничивают стоимость claim/recovery, но не рост таблицы, backups и
WAL при продолжительной работе.

Нужно выбрать сроки хранения terminal tasks и реализовать периодический batch
purge либо archive по `finished_at`. Сохранение failed tasks должно учитывать
ручное восстановление, расследование ошибок и период дедупликации получателя.
Pending/processing tasks не должны удаляться такой процедурой.

Область: [схема outbox](../../cqrs-runtime/sql/002-transactional-outbox.sql),
[queue](../../cqrs-runtime/src/postgres/outbox_queue.rs) и
[контракт эксплуатации](../../cqrs-runtime/doc/task_processing.md#integrating-another-application).
