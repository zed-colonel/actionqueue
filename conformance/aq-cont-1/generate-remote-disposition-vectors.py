#!/usr/bin/env python3
"""Independent fixed CanonicalDispositionV1 vectors, using only Python stdlib.

Awaiting cannot include final output, so two legal compound vectors cover every
producer effect. No Rust-produced bytes or incidental JSON are hashed.
"""
import hashlib
import json
import struct
import uuid
from pathlib import Path
u64 = lambda x: struct.pack('<Q', x)
u32 = lambda x: struct.pack('<I', x)
blob = lambda x: u64(len(x)) + x
text = lambda x: blob(x.encode())
uid = lambda x: uuid.UUID(int=x).bytes
identity = lambda x: str(uuid.UUID(int=x))
header = b'AQ-CONT-1\0disposition\0' + u32(1)

def data(raw):
    h = hashlib.sha256(raw).digest()
    return {'Inline': {'content_type': None, 'bytes': list(raw), 'hash': {'algorithm': 'Sha256', 'bytes': list(h)}}}, b'\0\0' + blob(raw) + b'\1' + blob(h)

inline, inline_bytes = data(b'checkpoint')
signal_data, signal_bytes = data(b'signal')
output, output_bytes = data(b'output')
child = {'admission_key': 'child', 'task_spec': {
    'id': identity(100), 'payload': {'bytes': [1], 'content_type': None}, 'run_policy': 'Once',
    'constraints': {'max_attempts': 1, 'timeout_secs': None, 'concurrency_key': None,
        'concurrency_key_hold_policy': 'HoldDuringRetry', 'concurrency_key_wait_policy': 'ReleaseWhileAwaiting',
        'safety_level': 'Pure', 'required_executor_traits': None},
    'metadata': {'priority': 0, 'tags': [], 'description': None}, 'parent_task_id': None,
    'tenant_id': None, 'child_lifecycle_policy': 'Required'}, 'dependencies': [],
    'causal_override': {'correlation_id': 'child-corr', 'requesting_actor_ref': 'actor-ref', 'origin_ref': 'origin'}}
task_bytes = uid(100) + blob(b'\1') + b'\0\0' + u32(1) + bytes(6) + struct.pack('<i', 0) + b'\0' + u64(0) + b'\0' + u64(0) + b'\0'
child_bytes = text('child') + blob(task_bytes) + b'\0\1' + text('child-corr') + b'\1' + text('actor-ref') + b'\1' + text('origin')
wait = {'wait_id': identity(101), 'target': {'Signal': {'filter': {'tenant_id': None,
    'namespace': 'remote', 'kind': 'complete', 'correlation_id': 'corr', 'source_ref': 'source'},
    'match_policy': 'FirstMatch', 'eligible_from': {'After': 7}}},
    'deadline': {'at': 1000, 'policy': 'ResumeWithTimeout'}}
wait_bytes = uid(101) + b'\0\0' + text('remote') + text('complete') + b'\1' + text('corr') + b'\1' + text('source') + b'\0\1' + u64(7) + b'\1' + u64(1000) + b'\0'
signal = {'signal_id': 'emitted', 'namespace': 'remote', 'kind': 'complete', 'correlation_id': 'corr',
    'payload': signal_data, 'payload_hash': signal_data['Inline']['hash'], 'occurred_at': 99}
emission = text('emitted') + text('remote') + text('complete') + text('corr') + b'\1' + signal_bytes + b'\1\1' + blob(hashlib.sha256(b'signal').digest()) + b'\1' + u64(99)
consumption = [{'dimension': 'Token', 'amount': 2}, {'dimension': 'CostCents', 'amount': 3}, {'dimension': 'TimeSecs', 'amount': 4}]
consumption_bytes = u64(3) + b''.join(bytes([i]) + u64(n) for i, n in enumerate([2, 3, 4]))
compound = {'outcome': 'Awaiting', 'output': None, 'checkpoint': {'checkpoint_id': identity(102),
    'created_by_attempt': identity(103), 'data': inline}, 'wait': wait,
    'child_admissions': [child], 'emitted_signals': [signal], 'consumption': consumption}
compound_bytes = header + b'\1\5\2\0\3\1' + uid(102) + inline_bytes + uid(103) + b'\4\1' + wait_bytes + b'\5' + u64(1) + child_bytes + b'\6' + u64(1) + emission + b'\7' + consumption_bytes
completed = {'outcome': 'Complete', 'output': output, 'checkpoint': None, 'wait': None,
    'child_admissions': [], 'emitted_signals': [signal], 'consumption': consumption}
complete_bytes = header + b'\1\0\2\1' + output_bytes + b'\3\0\4\0\5' + u64(0) + b'\6' + u64(1) + emission + b'\7' + consumption_bytes
empty_bytes = header + b'\1\0\2\0\3\0\4\0\5' + u64(0) + b'\6' + u64(0) + b'\7' + u64(0)
def vector(d, b):
    return {'disposition': d, 'canonical_hex': b.hex(), 'sha256': hashlib.sha256(b).hexdigest()}
root = Path(__file__).parent
(root / 'disposition-v1-vector.json').write_text(json.dumps({
    'generator': 'generate-remote-disposition-vectors.py; Python hashlib, struct, uuid',
    'canonical_hex': empty_bytes.hex(), 'sha256': hashlib.sha256(empty_bytes).hexdigest(),
    'compound': vector(compound, compound_bytes), 'output': vector(completed, complete_bytes),
}, indent=2) + '\n')
