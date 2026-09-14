#!/usr/bin/env python3
"""Independent Python/hashlib known-answer vectors for ADR-002 and projection v2."""
import hashlib
import json
from pathlib import Path
import struct
import uuid

root = Path(__file__).parent
u32 = lambda n: struct.pack('<I', n)
u64 = lambda n: struct.pack('<Q', n)
text = lambda s: u64(len(s.encode())) + s.encode()
identity = lambda n: str(uuid.UUID(int=n))
refs = ['submitting_principal_ref', 'requesting_actor_ref', 'purpose_ref', 'authorization_ref',
        'identity_context_ref', 'signed_statement_ref', 'proof_context_ref', 'origin_ref']
request = {
    'admission_key': 'execution-unit/c1/a/1',
    'task_spec': {
        'id': identity(1), 'payload': {'bytes': [0, 1, 255], 'content_type': 'application/octet-stream'},
        'run_policy': {'Repeat': {'count': 2, 'interval_secs': 60}},
        'constraints': {'max_attempts': 3, 'timeout_secs': 30, 'concurrency_key': 'lock',
                        'concurrency_key_hold_policy': 'ReleaseOnRetry', 'safety_level': 'Transactional',
                        'required_executor_traits': ['gpu', 'cpu', 'gpu']},
        'metadata': {'tags': ['z', 'a', 'z'], 'priority': -7, 'description': 'desc'},
        'parent_task_id': identity(2), 'tenant_id': identity(5)},
    'dependencies': [identity(4), identity(3), identity(4)],
    'causal_context': {'trace_id': 'trace', 'correlation_id': 'corr',
                       'causation': {'parent_task_id': identity(6), 'parent_run_id': identity(7),
                                     'parent_attempt_id': identity(8), 'external_ref': 'external'},
                       **{name: f'ref/{i}' for i, name in enumerate(refs)}},
    'control_context': None}
# This encoder spells out the fixed vector independently of the Rust implementation.
b = b'AQ-CONT-1\0admission\0' + u32(1) + uuid.UUID(int=1).bytes
b += u64(3) + bytes([0, 1, 255]) + b'\1' + text('application/octet-stream')
b += b'\1' + u32(2) + u64(60)  # Repeat
b += u32(3) + b'\1' + u64(30) + b'\1' + text('lock')
b += bytes([1, 0, 2])  # retry hold, wait hold, safety
b += b'\1' + u64(2) + text('cpu') + text('gpu')
b += struct.pack('<i', -7) + b'\1' + text('desc') + u64(2) + text('a') + text('z')
b += b'\1' + uuid.UUID(int=2).bytes + u64(2) + uuid.UUID(int=3).bytes + uuid.UUID(int=4).bytes
b += b'\1' + uuid.UUID(int=5).bytes + text('trace') + text('corr')
b += b'\1' + b''.join(b'\1' + uuid.UUID(int=i).bytes for i in [6, 7, 8]) + b'\1' + text('external')
b += b''.join(b'\1' + text(f'ref/{i}') for i in range(8))
(root / 'admission-v1-vector.json').write_text(json.dumps({'request': request, 'canonical_hex': b.hex(), 'sha256': hashlib.sha256(b).hexdigest()}, indent=2) + '\n')
