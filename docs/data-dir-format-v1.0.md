# Target data directory format

This filename is retained for links; it documents AQ-CONT-1, not the old store.
The authoritative implementation is [storage](../crates/actionqueue-storage/README.md).

| Component | Version |
|---|---|
| Store manifest schema | 1 |
| WAL frame | 1 |
| Snapshot frame / schema | 2 / 10 |
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

Snapshots use `AQCONT1S`, frame version 2, length and payload CRC. Compact schema-10
records bind store/manifest identity, covered WAL sequence/end offset, the last
frame start/hash and a versioned rolling prefix commitment. A separate SHA-256
covers the binding, projection and inspection index. Both payload and complete
frame remain bounded by 256 MiB. Collections have at most 1,000,000 rows; lengths,
references and tags are checked before accepting records. Logical projection and
canonical digest version remain 9; opaque payload limits remain in force.

Capture flushes the live authority's covered WAL and shares immutable roots.
Publication encodes outside the authority lane, syncs a unique temporary file,
atomically replaces the snapshot and syncs the parent directory. Recovery validates
initialization and the checkpoint boundary, hydrates state and indexes, and seeks
directly to the tail. It trusts the skipped prefix; `store verify` and backup/restore
recompute the commitment and compare complete-history semantics. Snapshot-only
stores are unsupported. See [ADR-020](adrs/AQ-ADR-020-bounded-snapshot-bootstrap.md)
for the exact encoding, trust limitation and source-preserving schema-9 upgrade.

Only physical snapshot damage permits fallback. Semantic, identity and compatibility
errors halt recovery. WAL CRC failures and interior corruption are never silently
skipped. See [recovery policy](wal-recovery-guide.md).
