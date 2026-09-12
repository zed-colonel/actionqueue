# actionqueue-runtime

Embedded execution, host-authenticated controls, and structural inspection for
AQ-CONT-1. Configure `HostControlContext` on the engine before admissions,
signals, cancellation or inspection. Missing context fails closed.

`BootstrappedEngine::ensure_task` and `admit_signal` use the same typed service as
HTTP v2. `Inspector` and the engine getters return redacted DTOs from a single
projection revision. `control` exposes task/run/wait cancellation and explicit
wait resolution. `store` exports verified offline inspect, backup and restore.

See [AQ-12 APIs](../../docs/aq-12-apis.md) for schemas, authorization, disclosure,
query bounds, metric semantics and operating modes.
