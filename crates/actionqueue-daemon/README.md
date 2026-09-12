# actionqueue-daemon

HTTP v2 hosting for AQ-CONT-1. Object inspection requires a host authentication
hook. Mutations additionally require `enable_control`; bootstrap rejects that
setting without authentication. Health/readiness, aggregate statistics and
metrics are separate from object inspection.

Bootstrap retains exclusive store ownership. `actionqueue daemon` binds the
router and serves until graceful shutdown. Continuation maintenance runs with
default features; actor/platform adapters are feature-gated and use `/api/v2`.

See [AQ-12 APIs](../../docs/aq-12-apis.md) for routes, redacted views, error codes,
backpressure, recovery readiness and telemetry.
