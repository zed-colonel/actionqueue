# actionqueue-daemon

HTTP daemon with REST API and Prometheus metrics for the ActionQueue task queue engine.

## Overview

This crate provides the daemon infrastructure:

- **HTTP API** -- Axum-based REST endpoints for task/run introspection and control
- **Metrics** -- Prometheus metrics endpoint (`/metrics`)
- **Bootstrap** -- Daemon startup and configuration validation

Read-only introspection endpoints are enabled by default. Control endpoints (cancel, pause, resume) require explicit enablement.

### API Surface

- `/healthz`, `/ready` -- Health and readiness probes
- `/api/v1/stats`, `/api/v1/tasks`, `/api/v1/runs` -- Introspection
- `/api/v2/actors/*` -- Actor registration and heartbeat (actor feature)
- `/api/v2/tenants`, `/api/v2/ledger` -- Platform operations (platform feature)

## Part of the ActionQueue workspace

See the [workspace root](https://github.com/zed-colonel/actionqueue) for full documentation.

## License

Apache-2.0
