#!/usr/bin/env python3
"""Independent AQ-11 projection and disposition digest vectors."""
import hashlib, json, struct
from pathlib import Path
root = Path(__file__).parent
def tree(v):
    if v is None: return b'\0'
    if isinstance(v, bool): return b'\1' + bytes([v])
    if isinstance(v, int): return bytes([2 if v >= 0 else 3]) + struct.pack('<Q' if v >= 0 else '<q',v)
    if isinstance(v, str): return b'\4' + struct.pack('<Q',len(v.encode())) + v.encode()
    if isinstance(v, list): return b'\5' + struct.pack('<Q',len(v)) + b''.join(tree(x) for x in v)
    return b'\6' + struct.pack('<Q',len(v)) + b''.join(tree(k)+tree(v[k]) for k in sorted(v))
p = json.loads((root/'projection-v7-vector.json').read_text())['projection']
p['version'] = p['metadata']['schema_version'] = 8
p['control_history'] = []
(root/'projection-v8-vector.json').write_text(json.dumps({'projection':p,'sha256':hashlib.sha256(b'AQ-CONT-1\0projection\0v8\0'+tree(p)).hexdigest()},indent=2,sort_keys=True)+'\n')
b = b'AQ-CONT-1\0disposition\0' + struct.pack('<I',1) + bytes([1,0,2,0,3,0,4,0,5]) + struct.pack('<Q',0) + bytes([6]) + struct.pack('<Q',0) + bytes([7]) + struct.pack('<Q',0)
(root/'disposition-v1-vector.json').write_text(json.dumps({'generator':'Python hashlib and struct; complete(None), no effects','canonical_hex':b.hex(),'sha256':hashlib.sha256(b).hexdigest()},indent=2)+'\n')
