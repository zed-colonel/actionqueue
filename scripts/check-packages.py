#!/usr/bin/env python3
"""Prepare real Cargo archives and verify their registry-only dependencies offline.

A temporary directory source seeds all eleven candidate versions together, including
mutual development dependencies. Then replace seeds with actual Cargo archives and
build the distributed consumer without path patches. No registry publication occurs.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[1]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def checksum(path, package=None):
    files = {str(p.relative_to(path)): digest(p) for p in sorted(path.rglob('*')) if p.is_file() and p.name != '.cargo-checksum.json'}
    (path / '.cargo-checksum.json').write_text(json.dumps({'files': files, 'package': package}))


def run(output):
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    meta = json.loads(subprocess.check_output(['cargo', 'metadata', '--offline', '--format-version', '1', '--no-deps'], cwd=ROOT))
    env = dict(os.environ, CARGO_TARGET_DIR=meta['target_directory'])
    with tempfile.TemporaryDirectory(prefix='aq-packages-') as tmp:
        work = Path(tmp)
        vendor = work / 'vendor'
        subprocess.run(['cargo', 'vendor', '--offline', '--locked', str(vendor)], cwd=ROOT, check=True, stdout=subprocess.DEVNULL)
        crates = sorted((ROOT / 'crates').glob('*/Cargo.toml'))
        for manifest in crates:
            data = tomllib.loads(manifest.read_text())['package']
            dest = vendor / data['name']
            shutil.copytree(manifest.parent, dest)
            checksum(dest)
        config = work / '.cargo/config.toml'
        config.parent.mkdir()
        config.write_text('[source.crates-io]\nreplace-with = "candidate"\n[source.candidate]\ndirectory = ' + json.dumps(str(vendor)) + '\n')
        for manifest in crates:
            data = tomllib.loads(manifest.read_text())['package']
            name, version = data['name'], data['version']
            dest = work / name
            shutil.copytree(manifest.parent, dest)
            # Lockfile preserves exact external versions, including any yanked transitive entries.
            shutil.copy2(ROOT / 'Cargo.lock', dest / 'Cargo.lock')
            subprocess.run(['cargo', 'package', '--offline', '--allow-dirty', '--no-verify'], cwd=dest, env=env, check=True)
            archive = Path(meta['target_directory']) / 'package' / f'{name}-{version}.crate'
            shutil.copy2(archive, output / archive.name)
        # Every seed is now replaced by the normalized, packaged artifact.
        for archive in sorted(output.glob('*.crate')):
            with tarfile.open(archive) as tar:
                tar.extractall(work / 'unpacked', filter='data')
                folder = work / 'unpacked' / tar.getnames()[0].split('/')[0]
            name = tomllib.loads((folder / 'Cargo.toml').read_text())['package']['name']
            shutil.rmtree(vendor / name)
            shutil.copytree(folder, vendor / name)
            checksum(vendor / name, digest(archive))
        consumer = work / 'consumer'
        shutil.copytree(ROOT / 'examples/downstream-handoff', consumer, ignore=shutil.ignore_patterns('target', 'Cargo.lock'))
        subprocess.run(['cargo', 'test', '--offline'], cwd=consumer, env=env, check=True)
        resolved = json.loads(subprocess.check_output(['cargo', 'metadata', '--offline', '--format-version', '1'], cwd=consumer, env=env))
        for node in resolved['resolve']['nodes']:
            if 'actionqueue-' in node['id'] and 'testing' in node['features']:
                raise ValueError('packaged production consumer has testing enabled')
        # Verify every published package, including CLI/daemon and optional layers.
        for manifest in crates:
            name = manifest.parent.name
            data = tomllib.loads(manifest.read_text())
            folder = work / 'unpacked' / f'{name}-{data["package"]["version"]}'
            subprocess.run(['cargo', 'build', '--offline'], cwd=folder, env=env, check=True)
            features = [f for f in ['workflow', 'budget', 'actor', 'platform'] if f in data.get('features', {})]
            if features:
                subprocess.run(['cargo', 'build', '--offline', '--no-default-features', '--features', ','.join(features)], cwd=folder, env=env, check=True)
        (output / 'resolution.json').write_text(json.dumps(resolved, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    run(parser.parse_args().output)
