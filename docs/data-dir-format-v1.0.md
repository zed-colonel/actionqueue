# Target data directory format (0.2.0)

This filename is retained for links; it documents AQ-CONT-1, not the old store.
The authoritative implementation is [storage](../crates/actionqueue-storage/README.md).

| Component | Version |
|---|---|
| Store manifest schema | 1 |
| WAL frame | 1 |
| Snapshot schema | 9 |
| Projection digest image | 9 |

The root contains `manifest.json`, `store.lock`, `wal/actionqueue.wal`, and optional
`snapshots/snapshot.bin`. A manifest binds contract, store UUID, versions, creation
identity/time, immutable feature profile and hash algorithms. Initialization
publishes a synced staging directory by atomic rename. Unidentified nonempty
stores and incompatible manifests are rejected without mutation.

WAL frames use `AQCONT1W`, format, record kind, payload schema, store UUID, sequence,
payload length, payload CRC-32 and header CRC-32 in a 52-byte little-endian header.
Payloads are storage-owned Postcard DTOs, limited to 16 MiB. Kinds and payload
schemas have explicit version mappings; they are not Rust enum discriminants.
Sequence 1 binds the manifest; later sequences must be contiguous.

Snapshots use `AQCONT1S`, frame version, length and payload CRC with a strict JSON
envelope limited to 256 MiB. The envelope binds the store identity, covered WAL
sequence, image versions and SHA-256 projection digest. It includes continuations,
checkpoints, resume assignments and attribution. Writers sync the WAL, publish by
atomic replacement, and sync the parent directory. Recovery validates against the
complete WAL and rebuilds derived indexes. Snapshot-only stores are unsupported.

Only physical snapshot damage permits fallback. Semantic, identity and compatibility
errors halt recovery. WAL CRC failures and interior corruption are never silently
skipped. See [recovery policy](wal-recovery-guide.md).
