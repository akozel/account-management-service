# PostgreSQL event replay: ошибки фонового stream

Статус: открыто. Относится к делегированному replay, а не к очереди tasks.

[PostgresEventRepositoryWithOutbox](../../cqrs-runtime/src/postgres/event_repository_with_outbox.rs)
делегирует `stream_events`/`stream_all_events` в `postgres-es` 0.5.0.
Upstream producer читает SQL stream в фоновой Tokio task и вызывает `unwrap()`
на результате `try_next()`. Ошибка SQL может завершить producer panic и закрыть
канал без передачи `PersistenceError` потребителю. Конец `ReplayStream` поэтому
сам по себе не доказывает успешное завершение replay.

Нужно обеспечить передачу ошибок чтения и отличать их от нормального EOF,
через исправление upstream либо adapter. Проверка должна покрывать отказ
соединения/SQL во время replay и запрет успешного завершения неполной проекции.
Существующие [persistence tests](../../cqrs-runtime/tests/postgres/persistence.rs)
покрывают успешный stream, но не снимают это ограничение.

Обычные command reads через `get_events`/`get_snapshot`, атомарные writes и
task-queue deadlines имеют отдельные контракты; исправления worker не изменяют
этот путь upstream replay.
