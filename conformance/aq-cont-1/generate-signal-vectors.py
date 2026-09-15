#!/usr/bin/env python3
"""Independent hashlib vectors for CanonicalSignalV1 and projection v3. Preserves v1/v2."""
import hashlib
import json
from pathlib import Path
import struct
import uuid
root = Path(__file__).parent
u64 = lambda n: struct.pack('<Q', n)
blob = lambda b: u64(len(b)) + b
text = lambda s: blob(s.encode())
opt = lambda value, encoder: b'\0' if value is None else b'\1' + encoder(value)
hash_bytes = lambda h: b'\1' + blob(bytes(h['bytes']))
def payload(p):
    if 'Inline' in p:
        d = p['Inline']
        return b'\0' + opt(d['content_type'], text) + blob(bytes(d['bytes'])) + hash_bytes(d['hash'])
    d = p['External']
    return b'\1' + text(d['scheme']) + text(d['locator']) + hash_bytes(d['hash']) + opt(d['size_bytes'], u64) + opt(d['content_type'], text)
def link(c):
    return b''.join(opt(c[k], lambda s: uuid.UUID(s).bytes) for k in ['parent_task_id','parent_run_id','parent_attempt_id']) + opt(c['external_ref'], text)
def canonical(e):
    h = e['payload_hash']
    if e['payload'] is not None: h = next(iter(e['payload'].values()))['hash']
    return (b'AQ-CONT-1\0signal\0' + struct.pack('<I',1) + opt(e['tenant_id'], lambda s: uuid.UUID(s).bytes)
        + text(e['signal_id']) + text(e['namespace']) + text(e['kind']) + opt(e['correlation_id'], text)
        + opt(e['causation'], link) + opt(e['source_ref'], text) + opt(e['payload'],payload)
        + opt(h,hash_bytes) + opt(e['occurred_at'],u64))
base = dict(signal_id='event/1',tenant_id=None,namespace='remote',kind='complete',correlation_id=None,
    causation=None,source_ref=None,payload=None,payload_hash=None,occurred_at=None,received_at=42,control_context=None)
h = dict(algorithm='Sha256',bytes=list(hashlib.sha256(bytes([0,1,255])).digest()))
cases = [base, dict(base, signal_id='event/inline', correlation_id='job/λ', payload={'Inline':dict(content_type='application/octet-stream',bytes=[0,1,255],hash=h)},occurred_at=0),
    dict(base,signal_id='event/external',tenant_id=str(uuid.UUID(int=5)),correlation_id='job/1',causation=dict(parent_task_id=str(uuid.UUID(int=1)),parent_run_id=str(uuid.UUID(int=2)),parent_attempt_id=str(uuid.UUID(int=3)),external_ref='origin/1'),source_ref='source/1',payload={'External':dict(scheme='blob',locator='opaque/object',hash=h,size_bytes=2**64-1,content_type='application/octet-stream')},payload_hash=h,occurred_at=100),
    dict(base,signal_id='hash-only',payload_hash=h)]
(root/'signal-v1-vector.json').write_text(json.dumps({'cases':[dict(envelope=e,canonical_hex=canonical(e).hex(),sha256=hashlib.sha256(canonical(e)).hexdigest()) for e in cases]},indent=2,ensure_ascii=False)+'\n')
