#!/usr/bin/env python3
"""Independent projection v4 vector; preserves all earlier evidence."""
import hashlib
import json
import struct
from pathlib import Path
root = Path(__file__).parent

def tree(value):
    if value is None:
        return b'\0'
    if isinstance(value, bool):
        return b'\1' + bytes([value])
    if isinstance(value, int):
        return bytes([2 if value >= 0 else 3]) + struct.pack('<Q' if value >= 0 else '<q', value)
    if isinstance(value, str):
        data = value.encode()
        return b'\4' + struct.pack('<Q', len(data)) + data
    if isinstance(value, list):
        return b'\5' + struct.pack('<Q', len(value)) + b''.join(tree(v) for v in value)
    return b'\6' + struct.pack('<Q', len(value)) + b''.join(tree(k) + tree(value[k]) for k in sorted(value))

p = json.loads((root / 'projection-v3-vector.json').read_text())['projection']
p['version'] = p['metadata']['schema_version'] = 4
for name in ['waits', 'cancellations', 'pending_resumes', 'key_reservations']:
    p[name] = []
for task in p['tasks']:
    task['task_spec']['constraints']['concurrency_key_wait_policy'] = 'ReleaseWhileAwaiting'
(root / 'projection-v4-vector.json').write_text(json.dumps({
    'projection': p,
    'sha256': hashlib.sha256(b'AQ-CONT-1\0projection\0v4\0' + tree(p)).hexdigest(),
}, indent=2, sort_keys=True) + '\n')
