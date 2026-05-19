---
'@backstage/backend-defaults': patch
---

Reduced scheduler database polling overhead by introducing a shared `TaskStatePoller` that batches per-task readiness checks into a single query per poll cycle. Previously, each scheduled task independently queried the database every few seconds; now all tasks registered by a plugin share one poll query.
