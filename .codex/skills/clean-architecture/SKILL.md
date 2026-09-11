---
name: clean-architecture
description: Preserve the Account Management Service's domain, application, presentation, and infrastructure boundaries when changing code or wiring.
---

# Clean architecture for Account Management Service

Use this skill for Rust source changes, dependency wiring, and infrastructure
configuration in this repository.

## Boundary rules

- `domain` owns business language, invariants, aggregate state, commands, and
  domain events. Keep it deterministic and free of transport, persistence,
  clocks, network clients, and framework-specific I/O types.
- `application` coordinates a use case through domain types and ports. It owns
  workflows, event reactions, and calls to injected nondeterministic services.
- `presentation` maps inbound protocols and DTOs to application requests. It
  must not bypass application code or encode domain rules.
- `infrastructure` implements ports and contains PostgreSQL, `postgres-es`,
  external email, and other I/O integrations. Do not expose those types through
  domain APIs.
- `main` only composes dependencies and starts the application.

## CQRS/ES placement

Keep event reconstruction and business decisions deterministic. Generate domain
events from validated intent; persist them through an infrastructure event-store
adapter; execute external effects in application reactions after persistence.

An event stream is authoritative. A snapshot is disposable derived state, so a
feature must remain correct when rebuilding solely from events.

## Change review

Before completing a change, inspect its imports and public signatures. Move
outward dependencies toward an adapter or application port if they would make a
domain type depend on I/O, transport, persistence, or a system clock.
