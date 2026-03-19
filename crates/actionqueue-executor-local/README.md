# actionqueue-executor-local

Local executor with dispatch queue, retry, backoff, and timeout for the ActionQueue task queue engine.

## Overview

This crate provides the execution infrastructure for running task attempts:

- **ExecutorHandler** trait -- implement to define task execution logic (must be `Send + Sync`)
- **AttemptRunner** -- request/response pipeline with timeout enforcement
- **TimeoutGuard** -- watchdog-based timeout with panic-safe cleanup
- **BackoffStrategy** -- pluggable delay computation (fixed, exponential)
- **RetryDecision** -- cap-enforced retry logic with no N+1 paths
- **DispatchQueue** -- FIFO intake with backpressure

## Part of the ActionQueue workspace

See the [workspace root](https://github.com/zed-colonel/actionqueue) for full documentation.

## License

Apache-2.0
