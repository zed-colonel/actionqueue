# actionqueue

The canonical executable is `actionqueue`.

```text
actionqueue daemon --data-dir STORE --enable-control --auth-file host.json
actionqueue ensure-task --file request.json --daemon http://127.0.0.1:8787 --token-file token
actionqueue task inspect TASK_ID --offline --data-dir STORE --json
actionqueue admission inspect --key KEY --offline --data-dir STORE
actionqueue signal admit --file signal.json --offline --data-dir STORE
actionqueue signal inspect SIGNAL_ID --offline --data-dir STORE
actionqueue wait inspect WAIT_ID --offline --data-dir STORE
actionqueue wait cancel WAIT_ID --run RUN_ID --offline --data-dir STORE
actionqueue wait resolve WAIT_ID --run RUN_ID --offline --data-dir STORE
actionqueue run inspect RUN_ID --offline --data-dir STORE
actionqueue run history RUN_ID --cursor CURSOR --offline --data-dir STORE
actionqueue run attempts RUN_ID --cursor CURSOR --offline --data-dir STORE
actionqueue trace TRACE_ID --offline --data-dir STORE
actionqueue trace --correlation ID --offline --data-dir STORE
actionqueue inspect --origin-ref REF --offline --data-dir STORE
actionqueue store inspect --data-dir STORE
actionqueue backup --data-dir STORE --output BACKUP
actionqueue restore --input BACKUP --data-dir EMPTY_DESTINATION
```

Use daemon access while its store lock is held. Tokens come from a file or
`ACTIONQUEUE_TOKEN`. Offline access is explicitly single-tenant; platform stores
require an authenticated host. The HTTP client currently supports local loopback
HTTP connections. JSON uses the v2 response schemas; text renders the same
redacted structural content. Creates and duplicates exit successfully; conflicts,
invalid input, and unavailability have distinct codes. See
[AQ-12 APIs](../../docs/aq-12-apis.md).

Run inspection includes the first history and attempt pages. Use `run history` or
`run attempts` to retrieve subsequent pages with `--cursor`; omit it for the first
page. Both accept `--limit`. Trace queries also accept `--edge-cursor`. Pagination
flags are rejected on commands without corresponding pagination support.
