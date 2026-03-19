# actionqueue-platform

Multi-tenant isolation, RBAC, and append-only ledgers for the ActionQueue task queue engine.

## Overview

Pure in-memory data structures for the platform layer. No I/O, no tokio, no storage dependencies.

- **TenantRegistry** -- Tenant registration and lookup
- **RbacEnforcer** -- Role-based capability enforcement (Operator, Auditor, Gatekeeper, Custom)
- **AppendLedger** -- Generic append-only ledger with secondary indexes by key and tenant

Requires the `platform` feature flag at the workspace level.

## Part of the ActionQueue workspace

See the [workspace root](https://github.com/zed-colonel/actionqueue) for full documentation.

## License

Apache-2.0
