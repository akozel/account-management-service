# Повторное применение события при создании snapshot

Статус: открыто. Обнаружено в интеграционном тесте command factory 2026-10-11.
Область: snapshot path `cqrs-es 0.5.0`; первый этап выделения runtime сохраняет
существующее поведение dependency.

## Причина и воспроизведение

В установленном исходнике `cqrs-es 0.5.0`:

1. `src/event_sink.rs`, `EventSink::write` применяет event к mutable aggregate
   до сохранения.
2. `src/cqrs.rs`, `execute_with_metadata` передает измененный aggregate context
   в `PersistedEventStore::commit`.
3. `src/persist/event_store.rs`, `update_snapshot_with_events` еще раз применяет
   accepted event к тому же context при достижении snapshot interval.

Если event handler добавляет элемент в Vec, snapshot содержит его дважды.
Тестовая последовательность с interval=2 давала `[first, second, second]`
вместо `[first, second]`. На interval=1 следующий command тоже наблюдал лишний
элемент и отвергался проверкой expected state. Это подтверждено реальным
PostgreSQL-тестом в ходе реализации, а не предположением о behavior dependency.

Текущие `EmailReservation` и `UserAccount` присваивают принятые значения state;
повторное применение их одного event не дублирует эффект. Их snapshot/race
integration tests продолжают проходить. Новый factory test использует такие же
repeat-tolerant assignments и проверяет snapshot плюс последующие events.

## Ограничение переиспользования и отдельная задача

`postgres::command_executor` использует snapshot store. Перед применением к
другому aggregate необходимо учитывать повторное `apply`: инкременты и append
без защиты сейчас несовместимы с этим snapshot path. Для event-store-only
сборки доступен low-level `CqrsCommandExecutor::new` с custom framework factory;
это не исправляет snapshots.

Отдельная задача — исправить snapshot contract upstream либо адаптировать его
так, чтобы snapshot строился из state до accepted event или сериализовал уже
полностью примененное state без повторного apply. Для сохраненного ограничения
«не более одного event на command» оба варианта требуют явных regression tests
с неидемпотентным apply, replay и optimistic conflict.

Исправление должно отдельно определить совместимость уже сохраненных snapshots.
Первый этап не меняет SQL/schema, не переписывает данные и не скрывает это
ограничение в factory API.
