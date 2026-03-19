# actionqueue-cli

Command-line interface for the ActionQueue task queue engine.

## Overview

This crate provides the CLI binary for operating ActionQueue:

- `daemon` -- Start the HTTP server with runtime configuration
- `submit` -- Submit a new task specification
- `stats` -- Print task/run statistics

Supports JSON and text output formats. Exit codes: 0 (success), 2 (usage), 3 (validation), 4 (runtime), 5 (connectivity).

## Part of the ActionQueue workspace

See the [workspace root](https://github.com/zed-colonel/actionqueue) for full documentation.

## License

Apache-2.0
