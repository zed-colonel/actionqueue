#!/usr/bin/env python3
"""Independent AQ-09 scoped-key, admission-v2, child-wait, wake and projection vectors."""
import hashlib
import json
import struct
import uuid
from pathlib import Path
root = Path(__file__).parent

def tree(value):
    if value is None: return b'\0'
    if isinstance(value, bool): return b'\1' + bytes([value])
    if isinstance(value, int): return bytes([2 if value >= 0 else 3]) + struct.pack('<Q' if value >= 0 else '<q', value)
    if isinstance(value, str):
        data = value.encode(); return b'\4' + struct.pack('<Q', len(data)) + data
    if isinstance(value, list): return b'\5' + struct.pack('<Q', len(value)) + b''.join(tree(v) for v in value)
    return b'\6' + struct.pack('<Q', len(value)) + b''.join(tree(k) + tree(value[k]) for k in sorted(value))

def write(name, value):
    (root / name).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')

p = json.loads((root / 'projection-v6-vector.json').read_text())['projection']
p['version'] = p['metadata']['schema_version'] = 7
for task in p['tasks']: task['task_spec']['child_lifecycle_policy'] = 'Required'
write('projection-v7-vector.json', {'projection': p, 'sha256': hashlib.sha256(b'AQ-CONT-1\0projection\0v7\0' + tree(p)).hexdigest()})
v1 = json.loads((root / 'admission-v1-vector.json').read_text())
raw = bytearray.fromhex(v1['canonical_hex'])
raw[len(b'AQ-CONT-1\0admission\0'):len(b'AQ-CONT-1\0admission\0')+4] = struct.pack('<I', 2)
write('admission-v2-vector.json', {policy: {'canonical_hex': (raw + bytes([tag])).hex(), 'sha256': hashlib.sha256(raw + bytes([tag])).hexdigest()} for tag, policy in enumerate(['Required','Detached'])})
parent='00000000-0000-0000-0000-000000000001'
run='00000000-0000-0000-0000-000000000002'
wait='00000000-0000-0000-0000-000000000003'
child='00000000-0000-0000-0000-000000000004'
local='batch/0'
raw=b'AQ-CONT-1\0child-key\0'+struct.pack('<I',1)+b'\0'+uuid.UUID(parent).bytes+uuid.UUID(run).bytes+struct.pack('<Q',len(local))+local.encode()
write('child-coordination-v1-vector.json', {
    'parent':parent, 'run':run, 'local_key':local, 'key_preimage_hex':raw.hex(), 'scoped_key':'child/v1/'+hashlib.sha256(raw).hexdigest(),
    'wait':{'wait_id':wait,'target':{'Children':{'task_ids':[child],'policy':'AllTerminal'}},'deadline':None},
    'wake':{'Children':{'wait_id':wait,'outcomes':[{'task_id':child,'status':'Succeeded'}]}}
})
