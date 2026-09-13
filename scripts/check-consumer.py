#!/usr/bin/env python3
"""Build the distributed consumer with explicit checkout overrides, outside the workspace."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def run(output=None):
    target = json.loads(subprocess.check_output(['cargo', 'metadata', '--offline', '--format-version', '1', '--no-deps'], cwd=ROOT))['target_directory']
    with tempfile.TemporaryDirectory(prefix='aq-consumer-') as tmp:
        work = Path(tmp)
        shutil.copytree(ROOT / 'examples/downstream-handoff', work / 'consumer', ignore=shutil.ignore_patterns('target', 'Cargo.lock'))
        config = work / 'overrides.toml'
        config.write_text('[patch.crates-io]\n' + ''.join(f'{p.name} = {{ path = {json.dumps(str(p))} }}\n' for p in sorted((ROOT / 'crates').iterdir()) if (p / 'Cargo.toml').exists()))
        base = ['cargo', '--config', str(config)]
        env = dict(os.environ, CARGO_TARGET_DIR=target)
        meta = json.loads(subprocess.check_output(base + ['metadata', '--offline', '--format-version', '1'], cwd=work / 'consumer', env=env))
        for node in meta['resolve']['nodes']:
            if 'actionqueue-' in node['id'] and 'testing' in node['features']:
                raise ValueError('consumer inherited test instrumentation')
        subprocess.run(base + ['test', '--offline'], cwd=work / 'consumer', env=env, check=True)
        subprocess.run(base + ['build', '--offline'], cwd=work / 'consumer', env=env, check=True)
        if output:
            Path(output).write_text(json.dumps(meta, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report')
    run(parser.parse_args().report)
