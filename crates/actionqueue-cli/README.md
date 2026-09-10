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

## AQ-CONT-1 offline storage

```sh
actionqueue-cli storage inspect --data-dir ./data --json
actionqueue-cli storage backup --data-dir ./data --output ./queue-backup --json
actionqueue-cli storage restore --input ./queue-backup --data-dir ./restored --json
```

Stop the runtime before offline inspection, stats, backup, or restore. A live writable
session returns `store_in_use`. These commands never initialize or repair a source.
Restore requires an absent/empty destination, preserves store identity and sequence,
and verifies checksums and projection equivalence before publishing. Payload bytes are
not included in inspection output; external artifact contents are outside the backup.
See [the storage format and ownership rules](../actionqueue-storage/README.md).
