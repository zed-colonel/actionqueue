# actionqueue-workflow

Release 0.2.0 implements AQ-CONT-1. See the [release and compatibility notes](../../docs/releases/0.2.0.md).

Workflow primitives for ActionQueue: DAG dependencies, task hierarchy, cron scheduling, and transactional child admission.

## Overview

This crate extends ActionQueue with workflow capabilities:

- **DependencyGate** -- DAG task dependencies with cycle detection and failure propagation
- **HierarchyTracker** -- Parent-child cascade cancellation and completion gating
- **Compound child admission** -- Coordinator handlers return child proposals with an awaiting disposition
- **ChildrenSnapshot** -- Child state visibility for coordinator handlers

Child admission, child waits, DAG eligibility, and hierarchy are available in the base runtime.
The `workflow` feature adds cron scheduling.

Use `child_admission::child` to build required or detached child proposals and
`child_admission::awaiting_children` to return a checkpoint and child wait in the
same disposition. Storage commits all child admissions and the parent wait in one
synced WAL frame. Keep child IDs and local keys stable across retries; store batch
progress in the checkpoint. Keys are scoped to the parent task and run.

`WaitSpec::children` accepts 1–64 direct-child IDs. `AllTerminal` returns every
terminal result; `AllSucceededOrAnyFailed` returns all successes or the lowest-ID
terminal failure/cancellation witness. Handlers read historical evidence from
`ResumeContext::wake`; `ChildrenSnapshot` is an inspection view.

Required children gate parent completion and receive parent cancellation. Detached
children do neither; explicit DAG dependencies and tenant checks still apply.
A parent may complete after a required child fails or is canceled. The coordinator
chooses its own final result. Ongoing cron tasks remain nonterminal between windows.

The target uses task/admission WAL schema 3, wait/disposition schema 2, and snapshot /
projection version 9. Earlier stores are refused by the manifest boundary; there is
no in-place migration.

## Part of the ActionQueue workspace

See the [workspace root](https://github.com/zed-colonel/actionqueue) for full documentation.

## License

Apache-2.0
