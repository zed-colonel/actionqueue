# actionqueue-workflow

Workflow primitives for ActionQueue: DAG dependencies, task hierarchy, cron scheduling, and dynamic submission.

## Overview

This crate extends ActionQueue with workflow capabilities:

- **DependencyGate** -- DAG task dependencies with cycle detection and failure propagation
- **HierarchyTracker** -- Parent-child cascade cancellation and completion gating
- **SubmissionChannel** -- Dynamic child task submission from coordinator handlers
- **ChildrenSnapshot** -- Child state visibility for coordinator handlers

Requires the `workflow` feature flag at the workspace level.

## Part of the ActionQueue workspace

See the [workspace root](https://github.com/zed-colonel/actionqueue) for full documentation.

## License

MIT
