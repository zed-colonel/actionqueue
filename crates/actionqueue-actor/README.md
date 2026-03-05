# actionqueue-actor

Remote actor registration, heartbeat monitoring, and capability routing for the ActionQueue task queue engine.

## Overview

Pure in-memory data structures for managing remote actors. No I/O, no tokio, no storage dependencies.

- **ActorRegistry** -- Registered actor state with secondary tenant index
- **HeartbeatMonitor** -- Per-actor heartbeat tracking and timeout detection
- **CapabilityRouter** -- Stateless capability intersection matching
- **DepartmentRegistry** -- Actor-to-department grouping with bidirectional index

Requires the `actor` feature flag at the workspace level.

## Part of the ActionQueue workspace

See the [workspace root](https://github.com/zed-colonel/actionqueue) for full documentation.

## License

MIT
