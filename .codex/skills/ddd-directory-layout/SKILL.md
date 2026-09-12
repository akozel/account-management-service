---
name: ddd-directory-layout
description: Organize Rust domain aggregates, entities, value objects, commands, events, and errors in the Account Management Service.
---

# DDD directory layout for Account Management Service

Use this skill before adding or reorganizing code under `src/domain/`.

## Layout

Group code by aggregate or bounded-context concept, not in global `entities/`
or `value_objects/` directories. A root module is the stable public facade:

```text
src/domain/
  user_account.rs
  user_account/
    aggregate.rs
    commands.rs
    events.rs
    errors.rs
    id.rs
    value_objects.rs
```

`user_account.rs` defines the aggregate struct, its state, constructor, and
state accessors, then declares private child modules and re-exports their
public domain types.
Keep that public API stable when moving internals.
Use Rust 2024 file modules; do not introduce `mod.rs`. Create only the
components an aggregate needs.

## Responsibilities

- `{aggregate}.rs` contains the aggregate struct, its state, constructors,
  and state accessors.
- `aggregate.rs` contains only the `Aggregate` trait implementation for an
  event-sourced aggregate, including event application and command dispatch.
  Its `Aggregate::handle` implementation only matches a command and delegates
  to the corresponding handler. Omit `aggregate.rs` when no such implementation
  exists; keep constructor tests in the root module or a `tests.rs` child.
- `commands.rs` contains command types expressing intent toward that aggregate
  and their handlers. Handlers validate the current aggregate state and emit
  the resulting domain events.
- `events.rs` contains immutable past-tense facts produced by accepted commands.
- `errors.rs` contains aggregate-level rejection errors.
- `id.rs` contains a dedicated aggregate identifier when it differs from other
  domain values. If an existing value object is already the canonical aggregate
  key (for example, `Email` for `EmailReservation`), use that type directly;
  do not add a wrapper solely to make aggregate layouts look alike.
- `value_objects.rs` contains small aggregate-scoped value objects and their
  parsing/validation errors. Split it into a `value_objects/` directory only
  once several independently meaningful types make that clearer.

Create an `entities/` child directory only for a genuine child entity: it has
an identity and life cycle inside the aggregate, but is not an independently
consistent aggregate.

## Schematic example

Adapt this division to the aggregate and include only needed components:

```rust
// invoice.rs: stable public facade, aggregate state, constructor and accessors
mod aggregate;
mod commands;
mod errors;
mod events;
mod id;
mod value_objects;

pub use commands::InvoiceCommand;

pub struct Invoice { status: InvoiceStatus }
impl Invoice {
    pub fn new() -> Self { Self { status: InvoiceStatus::Draft } }
    pub const fn status(&self) -> InvoiceStatus { self.status }
}

// aggregate.rs: dispatch and event application
impl Aggregate for Invoice {
    async fn handle(&mut self, command: Self::Command, _: &(), sink: &EventSink<Self>) -> Result<(), Self::Error> {
        match command {
            InvoiceCommand::Approve { invoice_id } => self.handle_approve(invoice_id, sink).await,
        }
    }

    fn apply(&mut self, event: Self::Event) {
        if let InvoiceEvent::InvoiceApproved { .. } = event {
            self.status = InvoiceStatus::Approved;
        }
    }
}

// commands.rs: intent and the command-specific transition rule
pub enum InvoiceCommand { Approve { invoice_id: InvoiceId } }

impl Invoice {
    pub(super) async fn handle_approve(&mut self, invoice_id: InvoiceId, sink: &EventSink<Self>) -> Result<(), InvoiceError> {
        if self.is_approved() {
            return Err(InvoiceError::AlreadyApproved);
        }
        sink.write(InvoiceEvent::InvoiceApproved { invoice_id }, self).await;
        Ok(())
    }
}

// events.rs: immutable fact required to replay the state change
pub enum InvoiceEvent { InvoiceApproved { invoice_id: InvoiceId } }

// errors.rs: rejected command
pub enum InvoiceError { AlreadyApproved }

// id.rs: typed aggregate identity
pub struct InvoiceId(Uuid);

// value_objects.rs: validated domain value
pub struct InvoiceNumber(String);

// entities/line_item.rs: child with an identity and life cycle inside Invoice
pub struct LineItem { id: LineItemId, amount: Money }
```

## Event sourcing rules

Commands are intentions; events are facts and must carry the data required for
replay. Apply/rebuild logic must not call I/O or observe the current clock.
Current domain events return `"1"` from `DomainEvent::event_version()`; the
pre-release service has no older event formats to support.
Cross-aggregate coordination is performed by an application reaction to an
event, not by one aggregate mutating another directly.
When follow-up work is needed, the aggregate records a pending phase that
rejects conflicting commands until a later completion command. The accepted
event and the follow-up outbox Task are committed together by application and
infrastructure; the aggregate does not perform outbox I/O. Each Task performs
one operation: a domain Task commands one aggregate, while an I/O Task may
perform one external effect. Follow the critical Task convention in
`.codex/skills/clean-architecture/SKILL.md`.
An already-applied command may return a distinct exact-repeat domain rejection;
the TaskHandler interprets it for its own Task. The aggregate must distinguish
that case from an existing record with different identity-defining data.

Do not introduce placeholder domain types or modules solely to fill the layout.
