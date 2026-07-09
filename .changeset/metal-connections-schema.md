---
'@backstage/connections': minor
---

Changed connection types to expose generated JSON Schema objects via `schema` instead of Zod schemas, and removed the Zod-backed `configSchema` values from the public connection type shape.
