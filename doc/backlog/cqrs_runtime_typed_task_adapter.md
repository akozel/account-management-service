# Этап 4: typed task adapter и упрощение sender-порта

Связанное [review](../reviews/cqrs_runtime_reuse.md#этап-4-typed-task-adapter).
Статус: выполнено 2026-10-11; реализация и все проверки завершены.

## Цель и согласованные границы

Перенести регистрацию формата, проверку type/version и десериализацию task payload
в cqrs-runtime. Перевести три сервисных handlers на typed inputs. По выбору
пользователя заменить owned DeliveryContext в sender-порте на &TaskDelivery.
Публичный helper возвращает готовый Box<dyn TaskHandler>; конкретный adapter
остается приватным. Service factories сохраняют прежние сигнатуры.

Бизнес-handlers сохраняют операции, проверку семизначного кода, преобразование
DTO и классификацию результатов повторной доставки. Существующий TaskHandler
остается для custom multi-version handlers. Новых features, dependencies,
env variables и миграций нет. Persisted DTO type/version и serde policy,
retry/settlement и lifecycle не меняются. Тексты format/decode reasons становятся
общими. Изменения конфигурации, logging и блокирующего stdout вне этого этапа.

## Runtime API и поведение

Приватный модуль: cqrs-runtime/src/task/json_adapter.rs. Facade outbox экспортирует:

```rust
#[async_trait]
pub trait TypedTaskHandler<P>: Send
where
    P: TaskPayload + DeserializeOwned + Send + 'static,
{
    async fn handle(
        &mut self,
        payload: P,
        delivery: &TaskDelivery,
    ) -> Result<(), TaskHandlerError>;
}

pub fn typed_task_handler<P, H>(
    handler: H,
) -> Result<Box<dyn TaskHandler>, TaskFormatError>
where
    P: TaskPayload + DeserializeOwned + Send + 'static,
    H: TypedTaskHandler<P> + 'static;
```

Helper проверяет TaskFormat::of::<P>() при создании; formats() возвращает этот
единственный формат. На delivery сначала сравнивает формат, затем десериализует
owned P непосредственно из &serde_json::Value без clone всего JSON, затем
вызывает business handler с исходным &TaskDelivery.

Несовпадение format и decode failure возвращают Permanent без вызова business
handler. Reason templates: `unsupported task format for {type} v{version}` и
`invalid {type} v{version} task payload`. Указывается ожидаемый DTO; type разрешен
в diagnostics только из ASCII letters/digits/underscore, иначе `unknown`, как в
worker diagnostics. Payload, metadata, фактический произвольный type и текст
serde error не форматируются. Routing key при sanitization не меняется.

Success и business errors передаются без изменений, включая reason и retry_after.
Один mutable handler сохраняется между deliveries; дополнительных Clone/Sync
ограничений нет. Serde policy принадлежит DTO; текущие v1 принимают дополнительные
поля. Отдельные adapters v1/v2 регистрируются отдельно; duplicate format остается
ошибкой. Retry, settlement и successor task publication в adapter не добавляются.

## Сервисная интеграция

CreateAccountTaskHandler, RecordAccountCreatedTaskHandler и SendVerificationCodeTaskHandler
реализуют TypedTaskHandler<соответствующий V1>. Публичные factories делегируют
runtime helper и сохраняют Result<Box<dyn TaskHandler>, TaskFormatError>.
Локальные format fields, format checks и JSON decode удалены. delivery_error,
exact-repeat classification и conversion verification v1 → VerificationCodeDelivery
остаются в приложении.

Sender-порт:

```rust
async fn send(
    &mut self,
    message: VerificationCodeDelivery,
    delivery: &TaskDelivery,
) -> Result<(), TaskHandlerError>;
```

DeliveryContext удаляется без compatibility alias. Console и тестовые senders
используют getters для ID/attempt/metadata. Borrow действует на протяжении async
invocation; данные для хранения после вызова sender должен явно взять во владение.
Console output и write/flush error mapping сохраняются. Metadata clone для передачи
context исчезает.

Application handler tests и фейки переносятся в
account-managements-service/tests/task_handlers.rs. Console unit tests используют
локальную delivery fixture вместо application test_support. Runtime unit tests
используют dummy DTO/handlers в inline tests модуля typed_handler; registry tests
проверяют routing нескольких typed versions. Общие format/decode tests не требуют
PostgreSQL или service dependency.

Обновить runtime README/task guide, архитектурные Mermaid-диаграммы,
clean-architecture skill и review вместе с API. Сохранить здесь фактические
результаты реализации и проверок.

## Тесты и приемка

- Runtime: invalid format construction; successful decode; extra fields; неверные
  type/version; malformed payload; format check до decode; отсутствие business
  вызова при отказе; safe reasons для arbitrary expected/actual type и serde error;
  исходный delivery context через async await; success/Retry/Permanent и backoff;
  mutable state между deliveries; Send payload/handler без Sync, payload без Clone.
- Registry: adapters v1/v2 маршрутизируются отдельно; duplicate format отклоняется.
- Service: правильный вызов каждого handler; семизначные границы кода; доставка
  frozen inputs с extra fields; borrowed original context через async sender;
  неизменный business error mapping. Console output и ошибки write/flush.
- Существующие PostgreSQL suites: exact redelivery, accepted data, отсутствие
  повторных events/successor tasks и сохраненные rollback/lease semantics.

Обязательные checks: fmt; workspace Clippy с -D warnings; workspace tests включая
PostgreSQL; llvm-cov line coverage ≥90%, с прежним исключением main.rs.
Дополнительно runtime lib tests без default features и compile checks отдельно
для base, worker, postgres, host. Все команды через RTK согласно AGENTS.md.

Этап завершен после реализации и прохождения всех проверок.

## Результаты проверок

Проверено на итоговой реализации с Rust 1.98.1:

| Проверка | Результат |
| --- | --- |
| `cargo fmt --all --check` | Passed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Passed |
| `cargo test --workspace --all-features` | 187 passed; 13 suites, включая PostgreSQL и Axum shutdown |
| `cargo llvm-cov --workspace --all-features --color never --ignore-filename-regex '(^|/)main\.rs$' --fail-under-lines 90` | Passed: 97.91% line coverage (6541 lines, 137 missed) |
| `cargo test -p cqrs-runtime --no-default-features --lib` | 30 passed |
| `cargo check -p cqrs-runtime --no-default-features` | Passed |
| `cargo check -p cqrs-runtime --no-default-features --features worker` | Passed |
| `cargo check -p cqrs-runtime --no-default-features --features postgres` | Passed |
| `cargo check -p cqrs-runtime --no-default-features --features host` | Passed |
| `git diff --check` | Passed |

Все команды выполнены через RTK. PostgreSQL suites использовали существующие
реальные testcontainers fixtures через доступный Podman socket; suites не пропускались.
Typed adapter module имеет 97.53% line coverage. 5 новых runtime adapter tests,
registry version-routing test, 5 service handler integration tests и console
output regression test покрывают согласованные сценарии. Прежний verification
unit test заменен расширенными service integration checks.

Runtime владеет checked format/decode; service factories используют boxed helper,
business operations и error mapping сохранены. Sender получает исходный delivery
без production metadata clone, а DeliveryContext и application test_support
удалены. Runtime guides, Mermaid diagrams, clean-architecture skill и review
обновлены. Cargo manifests, SQL schemas, persisted DTOs и env settings не менялись.
