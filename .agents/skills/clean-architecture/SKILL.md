---
name: clean-architecture
description: Preserve layer boundaries when changing this service's structure or behavior.
---

# Clean Architecture

- `domain`: business model and invariants; no project-layer or framework dependencies.
- `application`: use cases and ports; depends only on `domain`.
- `presentation`: translates external input into application calls; contains no business rules.
- `infrastructure`: implements application ports and external integrations.
- `main.rs`: composition root that wires the layers.

Dependencies point inward; do not bypass a layer boundary.
