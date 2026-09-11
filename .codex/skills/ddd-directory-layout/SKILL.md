---
name: ddd-directory-layout
description: Organize Rust domain aggregates, entities, value objects, commands, events, and errors in the Account Management Service.
---

# DDD directory layout for Account Management Service

Use this skill before adding or reorganizing code under `src/domain/`.

## Organize by aggregate

Group code by aggregate or bounded-context concept, not in global `entities/`
or `value_objects/` directories. A root module acts as the stable public facade:

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

`user_account.rs` declares private child modules and explicitly re-exports the
domain types that callers may use. Keep this public API stable when moving
internals. Use Rust 2024 file modules; do not introduce `mod.rs`.

## Assign responsibilities deliberately

- `aggregate.rs` contains the aggregate root and its state-transition rules.
- `commands.rs` contains only command types expressing intent toward that
  aggregate.
- `events.rs` contains immutable past-tense facts produced by accepted commands.
- `errors.rs` contains aggregate-level rejection errors.
- `id.rs` contains the aggregate identifier, a value object with type safety.
- `value_objects.rs` contains small aggregate-scoped value objects and their
  parsing/validation errors. Split it into a `value_objects/` directory only
  once several independently meaningful types make that clearer.

An aggregate root is itself an entity. Create an `entities/` child directory
only when the aggregate gains a genuine child entity: it has an identity and
life cycle within the aggregate, but it is not an independently consistent
aggregate.

## CQRS/ES rules for domain layout

Commands are intentions; events are facts and must carry the data required for
replay. Apply/rebuild logic must not call I/O or observe the current clock.
Cross-aggregate coordination is performed by an application reaction to an
event, not by one aggregate mutating another directly.

Avoid creating placeholder domain types solely to fill the layout. Empty module
files are acceptable while a planned aggregate evolves, but introduce command,
event, and child-entity types only when their behavior is defined.
