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
p['version'] = p['metadata']['schema_version'] = 9
p['control_history'] = []
(root/'projection-v9-vector.json').write_text(json.dumps({'projection':p,'sha256':hashlib.sha256(b'AQ-CONT-1\0projection\0v9\0'+tree(p)).hexdigest()},indent=2,sort_keys=True)+'\n')
import runpy
runpy.run_path(str(root / 'generate-remote-disposition-vectors.py'))
