#!/usr/bin/env python3
"""Execute release gates on one clean commit, prepare artifacts, or verify them.

No publication, Git mutation, or imported gate reports. Requires Python 3.11+.
An interrupted preparation keeps its evidence but cannot produce a valid manifest.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import tomllib

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = Path('conformance/aq-cont-1')
PROFILE = Path('docs/contracts/aq-cont-1-developmental-campaign-execution-profile.md')
PROFILES = ['', 'workflow', 'budget', 'workflow,budget', 'actor', 'platform', 'actor,platform', 'workflow,budget,actor,platform']
LIMITATIONS = [
    'Fresh AQ-CONT-1 stores only; no pre-contract migration or development-schema upgrade.',
    'Store feature profiles are immutable; compiled capabilities do not upgrade stores.',
    'Backup and restore require offline ownership; external artifact bytes are application-owned.',
    'Inspection is bounded, paginated, and redacted; references grant no authority.',
    'Mutation preparation clones the full projection; recovery verifies the full retained WAL.',
    'ActionQueue-owned reference certification only; WorldInterface and Exoskeleton are not certified.',
]


def require(value, message):
    if not value:
        raise ValueError(message)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT).decode().strip()


def clean(commit):
    require(re.fullmatch('[0-9a-f]{40}', commit), 'commit must be an exact SHA')
    require(git('rev-parse', 'HEAD') == commit, 'checkout does not match source commit')
    require(not git('status', '--porcelain', '--untracked-files=all'), 'dirty source checkout')


def safe(base, name):
    p = Path(name)
    require(not p.is_absolute() and '..' not in p.parts and str(p) == name, f'unsafe artifact path: {name}')
    path = base / p
    require(path.is_file() and not path.is_symlink() and path.resolve().is_relative_to(base.resolve()), f'missing/unsafe artifact: {name}')
    return path


def metadata():
    manifest = read(ROOT / PACKAGE / 'manifest.yaml')
    require(manifest['package_revision'] == 15 and manifest['status'] == 'executable', 'unexpected conformance revision/status')
    require(manifest['contract_revision'] == 'STACK-2026-07-20-CLEAN-1', 'wrong contract revision')
    for entry in manifest['fixtures'] + manifest['assets'] + manifest['acceptance_matrices']:
        require(sha(safe(ROOT / PACKAGE, entry['path'])) == entry['sha256'], f'tampered conformance asset: {entry["path"]}')
    for entry in manifest['normative_documents']:
        require(sha(safe(ROOT, entry['path'])) == entry['sha256'], 'tampered normative document')
    blocker_record = read(ROOT / 'docs/releases/blockers.json')
    require(blocker_record == {'schema_version': 1, 'blockers': []}, 'unresolved release blockers')
    crates = []
    for p in sorted((ROOT / 'crates').glob('*/Cargo.toml')):
        data = tomllib.loads(p.read_text())
        require(data['package']['version'] == '0.2.0', 'inconsistent crate version')
        for table in ['dependencies', 'dev-dependencies', 'build-dependencies']:
            for name, dep in data.get(table, {}).items():
                if name.startswith('actionqueue-'):
                    require((dep if isinstance(dep, str) else dep['version']) == '0.2.0', 'inconsistent internal dependency')
        crates.append({'name': data['package']['name'], 'version': data['package']['version']})
    require(len(crates) == 11, 'incomplete crate inventory')
    locked = tomllib.loads((ROOT / 'Cargo.lock').read_text())['package']
    require(sorted((x['name'], x['version']) for x in locked if x['name'].startswith('actionqueue-') and x['name'] != 'actionqueue-acceptance-harness') == sorted((x['name'], x['version']) for x in crates), 'inconsistent lockfile inventory')
    store_source = (ROOT / 'crates/actionqueue-storage/src/store/manifest.rs').read_text()
    versions = {key: int(re.search(r'\b' + key + r': (\d+),', store_source)[1]) for key in ['manifest_schema', 'wal_format', 'snapshot_schema', 'projection_version']}
    require(versions == dict(manifest_schema=1, wal_format=1, snapshot_schema=9, projection_version=9), 'unexpected store versions')
    matrix = manifest['acceptance_matrices'][0]
    require(matrix['profile_revision'] == 'STACK-DEVELOPMENTAL-DIAGNOSTICS-1' and matrix['case_count'] == 18 and matrix['normative_protocol_change'] is False, 'wrong developmental profile')
    return {
        'schema_version': 1, 'version': '0.2.0', 'tag': 'v0.2.0', 'crates': crates,
        'contract': manifest['contract'], 'contract_revision': manifest['contract_revision'],
        'conformance': {'revision': manifest['package_revision'], 'manifest_sha256': sha(ROOT / PACKAGE / 'manifest.yaml'), 'fixtures': manifest['fixtures'], 'assets': manifest['assets']},
        'developmental': {'revision': matrix['profile_revision'], 'profile_sha256': sha(ROOT / PROFILE), 'matrix_sha256': sha(ROOT / PACKAGE / matrix['path']), 'case_count': 18, 'protocol_or_authority_change': False},
        'lockfile_sha256': sha(ROOT / 'Cargo.lock'), 'store_versions': versions,
        'supported_feature_profiles': PROFILES, 'tested_feature_combinations': PROFILES,
        'known_limitations': LIMITATIONS,
        'downstream': {'identity': 'ActionQueue-owned reference adapter and independent outbox consumer', 'certification_scope': 'AQ-CONT-1 reference only; no WorldInterface revision certified'},
        'blockers': [],
    }


def validate_report(report):
    m = read(ROOT / PACKAGE / 'manifest.yaml')
    coverage = read(ROOT / PACKAGE / m['coverage'])
    require(report.get('schema_version') == 1, 'unsupported conformance report schema')
    require(report.get('passed') is True and report.get('profile') == 'full' and report.get('missing') == [], 'unsuccessful or partial conformance report')
    for key in ['package_revision', 'contract_revision', 'developmental_profile_revision']:
        require(report.get(key) == m[key], f'wrong report {key}')
    rows = report['results']
    hashes = {f['id']: f['sha256'] for f in m['fixtures']}
    require(rows and all(r['assertion_result'] == 'passed' and r['fixture_hash'] == hashes.get(r['fixture_id']) for r in rows), 'failed/unknown/tampered fixture evidence')

    def has(fixture, driver, variant, features=(), cut=None, check_cut=False):
        require(any(r['fixture_id'] == fixture and r['driver'] == driver and r['variant'] == variant
                    and set(features) <= set(r['feature_profile']) and (not check_cut or r.get('crash_point') == cut)
                    for r in rows), f'missing evidence {fixture}/{driver}/{variant}/{cut}')
    for case in coverage['cases']:
        require(case['fixtures'] and case['drivers'], 'empty conformance mapping')
        for fixture in case['fixtures']:
            for driver in case['drivers']:
                for variant in case['variants']:
                    has(fixture, driver, variant, case['required_features'])
    for fixture in coverage['public_fixtures']:
        for driver in coverage['public_drivers']:
            for variant in ['ordinary', 'replay', 'crash']:
                has(fixture, driver, variant)
    for fixture in m['fixtures']:
        if fixture['path'].startswith('fixtures/'):
            scenario = read(ROOT / PACKAGE / fixture['path'])
            for variant in set(fixture['variants']) | {'backup', 'corruption'}:
                for cut in scenario['recovery_cuts'] if variant == 'crash' else [None]:
                    has(fixture['id'], fixture['driver'], variant, scenario['required_features'], cut, True)


def gates():
    # Relative outputs make recorded commands independent of scratch directory names.
    result = [('format', '', ['cargo', 'fmt', '--all', '--', '--check']),
              ('consumer-format', '', ['cargo', 'fmt', '--manifest-path', 'examples/downstream-handoff/Cargo.toml', '--', '--check']),
              ('release-tests', '', ['python3', '-B', '-m', 'unittest', 'discover', '-s', 'tests/release'])]
    for i, profile in enumerate(PROFILES):
        features = ['--features', profile] if profile else []
        result.extend([(f'tests-{i}', profile, ['cargo', 'test', '--locked', '--workspace', *features, '--', '--test-threads=1']),
                       (f'clippy-{i}', profile, ['cargo', 'clippy', '--locked', '--all', '--all-targets', *features, '--', '-D', 'warnings'])])
        for package in ['actionqueue-cli', 'actionqueue-daemon']:
            result.append((f'production-{package}-{i}', profile, ['cargo', 'build', '--locked', '-p', package, '--no-default-features', *features]))
    result.extend([
        ('build', '', ['cargo', 'build', '--locked', '--workspace']),
        ('core-serde', 'serde', ['cargo', 'test', '--locked', '-p', 'actionqueue-core', '--features', 'serde']),
        ('storage-serde', 'serde', ['cargo', 'test', '--locked', '-p', 'actionqueue-storage', '--features', 'serde']),
        ('conformance', '', ['cargo', 'aq-conformance']),
        ('developmental', PROFILES[-1], ['cargo', 'aq-developmental']),
        ('full-conformance', PROFILES[-1], ['cargo', 'run', '--locked', '--example', 'aq_conformance', '--features', PROFILES[-1], '--', '--full', '--report', '{output}/conformance-report.json']),
        ('cross-feature', PROFILES[-1], ['bash', 'conformance/aq-cont-1/cross-feature-persistence.sh']),
        ('performance', '', ['cargo', 'bench', '--locked', '--bench', 'continuation']),
        ('rustdoc', PROFILES[-1], ['cargo', 'doc', '--locked', '--workspace', '--no-deps', '--features', PROFILES[-1]]),
        ('docs', '', ['python3', '-B', 'scripts/check-docs.py', '--api', '{target}/doc']),
        ('consumer', '', ['python3', '-B', 'scripts/check-consumer.py', '--report', '{output}/consumer-resolution.json']),
        ('packages', '', ['python3', '-B', 'scripts/check-packages.py', '--output', '{output}/crates']),
    ])
    return result


def validate_evidence(manifest, output):
    expected = gates()
    require(len(manifest['evidence']) == len(expected), 'missing release gates')
    for evidence, (name, profile, command) in zip(manifest['evidence'], expected):
        require(evidence['gate'] == name and evidence['command'] == command and evidence['feature_profile'] == profile, 'wrong gate identity/command/profile')
        require(evidence['source_commit'] == manifest['source_commit'] and evidence['result'] == 'passed', 'wrong source commit or failed gate')
        require(sha(safe(output, evidence['log'])) == evidence['sha256'], 'tampered gate log')
    validate_report(read(safe(output, 'conformance-report.json')))


def verify(path):
    path = Path(path).resolve()
    output = path.parent
    manifest = read(path)
    clean(manifest['source_commit'])
    expected = metadata()
    for key, value in expected.items():
        require(manifest.get(key) == value, f'inconsistent release {key}')
    require(set(manifest) == set(expected) | {'source_commit', 'rustc', 'evidence', 'artifacts'}, 'unknown/missing manifest fields')
    require(manifest['rustc'].startswith('rustc 1.89.0 '), 'wrong Rust compiler')
    artifacts = manifest['artifacts']
    require(len({a['path'] for a in artifacts}) == len(artifacts), 'duplicate artifact')
    for artifact in artifacts:
        require(sha(safe(output, artifact['path'])) == artifact['sha256'], 'tampered publication artifact')
    paths = {a['path'] for a in artifacts}
    required = {'source.bundle', 'release-notes.md', 'api-docs.tar', 'conformance-report.json', 'performance.json', 'consumer-resolution.json', 'crates/resolution.json', 'release-manifest.schema.json', str(PROFILE)}
    required |= {f'crates/{c["name"]}-{c["version"]}.crate' for c in manifest['crates']}
    required |= {str(p.relative_to(ROOT)) for p in (ROOT / PACKAGE).rglob('*') if p.is_file()}
    required |= {e['log'] for e in manifest['evidence']}
    require(required <= paths, 'missing publication assets')
    # Published frozen copies must equal the selected checkout, not merely their own inventory.
    for name in paths:
        if name.startswith(str(PACKAGE) + '/') or name == str(PROFILE):
            require(sha(output / name) == sha(ROOT / name), 'published contract/conformance bytes differ')
    require(sha(output / 'release-notes.md') == sha(ROOT / 'docs/releases/0.2.0.md'), 'wrong release notes')
    require(sha(output / 'release-manifest.schema.json') == sha(ROOT / 'docs/releases/release-manifest.schema.json'), 'wrong release schema')
    heads = subprocess.check_output(['git', 'bundle', 'list-heads', str(output / 'source.bundle')]).decode().splitlines()
    require(heads == [manifest['source_commit'] + ' HEAD'], 'source bundle has wrong commit')
    validate_evidence(manifest, output)
    sums = ''.join(f'{sha(output / name)}  {name}\n' for name in sorted(paths | {'release-manifest.json'}))
    require((output / 'SHA256SUMS').read_text() == sums, 'inconsistent checksums')
    print(f'Verified v0.2.0 at {manifest["source_commit"]}; publication remains operator-owned.')


def prepare(version, commit, output):
    require(version == '0.2.0', 'only release 0.2.0 is defined')
    clean(commit)
    manifest = metadata()
    output = Path(output).resolve()
    require(not output.is_relative_to(ROOT), 'artifact directory must be outside source checkout')
    require(not output.exists() or not any(output.iterdir()), 'output must be empty; preserve failed evidence in another directory')
    output.mkdir(parents=True, exist_ok=True)
    target = json.loads(subprocess.check_output(['cargo', 'metadata', '--offline', '--format-version', '1', '--no-deps'], cwd=ROOT))['target_directory']
    manifest.update(source_commit=commit, rustc=subprocess.check_output(['rustc', '-Vv']).decode(), evidence=[])
    require(manifest['rustc'].startswith('rustc 1.89.0 '), 'release requires Rust 1.89.0')
    (output / 'logs').mkdir()
    # TMPDIR captures the full runner's subordinate evidence in the release inventory.
    (output / 'scratch').mkdir()
    env = dict(os.environ, TMPDIR=str(output / 'scratch'), TMP=str(output / 'scratch'), TEMP=str(output / 'scratch'), AQ_PERFORMANCE_REPORT=str(output / 'performance.json'), PYTHONDONTWRITEBYTECODE='1', RUSTDOCFLAGS='-D warnings')
    for name, profile, command in gates():
        clean(commit)
        print(f'Running {name}', flush=True)
        log = f'logs/{name}.log'
        with (output / log).open('w') as stream:
            completed = subprocess.run([s.replace('{output}', str(output)).replace('{target}', target) for s in command], cwd=ROOT, env=env, stdout=stream, stderr=subprocess.STDOUT)
        require(completed.returncode == 0, f'gate {name} failed; inspect {output / log}')
        clean(commit)
        if name == 'full-conformance':
            validate_report(read(output / 'conformance-report.json'))
        manifest['evidence'].append(dict(gate=name, command=command, feature_profile=profile, source_commit=commit, result='passed', log=log, sha256=sha(output / log)))
        write(output / 'evidence-progress.json', manifest['evidence'])
    shutil.copytree(ROOT / PACKAGE, output / PACKAGE)
    (output / PROFILE).parent.mkdir(parents=True)
    shutil.copy2(ROOT / PROFILE, output / PROFILE)
    shutil.copy2(ROOT / 'docs/releases/0.2.0.md', output / 'release-notes.md')
    shutil.copy2(ROOT / 'docs/releases/release-manifest.schema.json', output / 'release-manifest.schema.json')
    subprocess.run(['git', 'bundle', 'create', str(output / 'source.bundle'), 'HEAD'], cwd=ROOT, check=True)
    with tarfile.open(output / 'api-docs.tar', 'w') as tar:
        for p in sorted((Path(target) / 'doc').rglob('*')):
            if p.is_file():
                info = tar.gettarinfo(p, arcname=str(p.relative_to(Path(target) / 'doc')))
                info.mtime = info.uid = info.gid = 0
                info.uname = info.gname = ''
                with p.open('rb') as f:
                    tar.addfile(info, f)
    clean(commit)
    # Scratch stores can be large; preserve only evidence files, never queue runtime data.
    evidence = output / 'supporting-evidence'
    for p in sorted((output / 'scratch').rglob('*')):
        if p.is_file() and (p.suffix in {'.json', '.log'} or p.name.endswith('.stderr')):
            relative = p.relative_to(output / 'scratch')
            if 'report' in p.name or 'evidence' in p.name or p.suffix in {'.log', '.stderr'}:
                dest = evidence / relative
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(p, dest)
    # Keep diagnostic scratch for inspection, excluded from publication/checksums.
    manifest['artifacts'] = [{'path': str(p.relative_to(output)), 'sha256': sha(p)} for p in sorted(output.rglob('*')) if p.is_file() and not p.is_relative_to(output / 'scratch')]
    write(output / 'release-manifest.json', manifest)
    paths = {a['path'] for a in manifest['artifacts']} | {'release-manifest.json'}
    (output / 'SHA256SUMS').write_text(''.join(f'{sha(output / name)}  {name}\n' for name in sorted(paths)))
    verify(output / 'release-manifest.json')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    prep = commands.add_parser('prepare')
    prep.add_argument('--version', required=True)
    prep.add_argument('--commit', required=True)
    prep.add_argument('--output', required=True)
    check = commands.add_parser('verify')
    check.add_argument('--manifest', required=True)
    args = parser.parse_args()
    try:
        if args.command == 'prepare':
            prepare(args.version, args.commit, args.output)
        else:
            verify(args.manifest)
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as error:
        sys.exit(f'release rejected: {error}')
