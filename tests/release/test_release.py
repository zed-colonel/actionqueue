"""Release integrity tests use synthetic evidence, never publishable evidence."""
import copy
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('release', ROOT / 'scripts/release.py')
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


def full_report():
    manifest = release.read(ROOT / release.PACKAGE / 'manifest.yaml')
    coverage = release.read(ROOT / release.PACKAGE / manifest['coverage'])
    hashes = {f['id']: f['sha256'] for f in manifest['fixtures']}
    rows = []

    def row(fixture, driver, variant, cut=None):
        rows.append(dict(fixture_id=fixture, fixture_hash=hashes[fixture], driver=driver,
                         variant=variant, crash_point=cut, assertion_result='passed',
                         feature_profile=release.PROFILES[-1].split(',')))
    for case in coverage['cases']:
        for fixture in case['fixtures']:
            for driver in case['drivers']:
                for variant in case['variants']:
                    row(fixture, driver, variant)
    for fixture in coverage['public_fixtures']:
        for driver in coverage['public_drivers']:
            for variant in ['ordinary', 'replay', 'crash']:
                row(fixture, driver, variant)
    for fixture in manifest['fixtures']:
        if fixture['path'].startswith('fixtures/'):
            scenario = release.read(ROOT / release.PACKAGE / fixture['path'])
            for variant in set(fixture['variants']) | {'backup', 'corruption'}:
                for cut in scenario['recovery_cuts'] if variant == 'crash' else [None]:
                    row(fixture['id'], fixture['driver'], variant, cut)
    return dict(schema_version=1, profile='full', passed=True, missing=[], results=rows,
                **{k: manifest[k] for k in ['package_revision', 'contract_revision', 'developmental_profile_revision']})


class ReportTests(unittest.TestCase):
    def test_complete_evidence_passes(self):
        release.validate_report(full_report())

    def test_missing_each_developmental_variant_is_rejected(self):
        report = full_report()
        rows = report['results']
        developmental = {(r['fixture_id'], r['driver'], r['variant']) for r in rows if 'DD-' in r['fixture_id'] or 'DEVELOPMENTAL' in r['fixture_id']}
        self.assertTrue(developmental)
        for fixture, driver, variant in developmental:
            with self.subTest(fixture=fixture, driver=driver, variant=variant):
                report['results'] = [r for r in rows if (r['fixture_id'], r['driver'], r['variant']) != (fixture, driver, variant)]
                with self.assertRaises(ValueError):
                    release.validate_report(report)

    def test_subset_cannot_be_labeled_full(self):
        report = full_report()
        report['results'] = report['results'][:1]
        with self.assertRaises(ValueError):
            release.validate_report(report)

    def test_wrong_revisions_and_failed_assertions(self):
        for key, value in [('schema_version', 99), ('profile', 'subset'), ('passed', False), ('package_revision', 12), ('contract_revision', 'wrong'), ('developmental_profile_revision', 'wrong'), ('missing', ['blocker'])]:
            report = full_report()
            report[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                release.validate_report(report)
        for key, value in [('fixture_hash', '0' * 64), ('assertion_result', 'failed'), ('fixture_id', 'unknown')]:
            report = full_report()
            report['results'][0][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                release.validate_report(report)

    def test_each_storage_cut_is_required(self):
        report = full_report()
        cuts = {(r['fixture_id'], r['crash_point']) for r in report['results'] if r['crash_point'] is not None}
        self.assertTrue(cuts)
        for fixture, cut in cuts:
            modified = copy.deepcopy(report)
            modified['results'] = [r for r in modified['results'] if (r['fixture_id'], r['crash_point']) != (fixture, cut)]
            with self.subTest(fixture=fixture, cut=cut), self.assertRaises(ValueError):
                release.validate_report(modified)


class ManifestTests(unittest.TestCase):
    def test_checkout_must_be_clean_and_exact(self):
        with patch.object(release, 'git', side_effect=['b' * 40]):
            with self.assertRaisesRegex(ValueError, 'does not match'):
                release.clean('a' * 40)
        for status in [' M Cargo.toml', '?? unexpected.rs']:
            with patch.object(release, 'git', side_effect=['a' * 40, status]):
                with self.assertRaisesRegex(ValueError, 'dirty'):
                    release.clean('a' * 40)

    def test_metadata_pins_release(self):
        metadata = release.metadata()
        self.assertEqual(metadata['version'], '0.3.0')
        self.assertEqual(metadata['tag'], 'v0.3.0')
        self.assertEqual(metadata['store_versions'], dict(manifest_schema=1, wal_format=1, snapshot_schema=10, projection_version=9))
        self.assertEqual({c['version'] for c in metadata['crates']}, {'0.3.0'})
        self.assertEqual(metadata['conformance']['revision'], 16)
        self.assertEqual(metadata['developmental']['case_count'], 18)

    def test_release_guidance_must_match_conformance_revision(self):
        revision = release.read(ROOT / release.PACKAGE / 'manifest.yaml')['package_revision']
        real_read = Path.read_text
        for file in ['README.md', 'docs/releases/0.3.0.md']:
            for replacement in [f'**{revision - 1}**', '**unknown**']:
                self.assertIn(f'**{revision}**', real_read(ROOT / file))
                def changed(path, *args, **kwargs):
                    value = real_read(path, *args, **kwargs)
                    return value.replace(f'**{revision}**', replacement) if path == ROOT / file else value
                with self.subTest(file=file, replacement=replacement):
                    with patch.object(Path, 'read_text', changed), self.assertRaisesRegex(ValueError, 'conformance revision in ' + file):
                        release.metadata()

    def test_assets_and_versions_cannot_be_forged(self):
        original = release.sha
        with patch.object(release, 'sha', side_effect=lambda p: '0' * 64 if str(p).endswith('admission-v1-vector.json') else original(p)):
            with self.assertRaisesRegex(ValueError, 'tampered'):
                release.metadata()
        real_read = Path.read_text
        cases = [
            ('crates/actionqueue-core/Cargo.toml', 'version = "0.3.0"', 'version = "0.2.0"'),
            ('crates/actionqueue-storage/Cargo.toml', 'actionqueue-core = "0.3.0"', 'actionqueue-core = "0.2.0"'),
            ('Cargo.toml', 'actionqueue-runtime = "0.3.0"', 'actionqueue-runtime = "0.2.0"'),
            ('Cargo.lock', 'version = "0.3.0"', 'version = "0.2.0"'),
            ('examples/downstream-handoff/Cargo.toml', '"=0.3.0"', '"=0.2.0"'),
            ('examples/downstream-handoff/Cargo.toml', '"=0.3.0"', '"0.3.0"'),
            ('crates/actionqueue-storage/src/store/manifest.rs', 'snapshot_schema: 10,', 'snapshot_schema: 9,'),
            ('docs/releases/release-manifest.schema.json', '"const": 10', '"const": 9'),
            ('docs/releases/release-manifest.schema.json', '"const": "0.3.0"', '"const": "0.2.0"'),
            ('docs/releases/0.3.0.md', 'Release identity: **0.3.0 / v0.3.0**.', 'Release identity: **0.2.0 / v0.2.0**.'),
        ]
        for file, old, new in cases:
            source = real_read(ROOT / file)
            self.assertIn(old, source, f'negative test no longer modifies {file}')
            self.assertNotEqual(source, source.replace(old, new))
            def changed(path, *args, **kwargs):
                value = real_read(path, *args, **kwargs)
                return value.replace(old, new) if path == ROOT / file else value
            with self.subTest(file=file, replacement=new), patch.object(Path, 'read_text', changed), self.assertRaises(ValueError):
                release.metadata()

    def test_prepare_rejects_other_releases_before_running_gates(self):
        for version in ['0.2.0', '0.3.1']:
            with self.subTest(version=version), self.assertRaisesRegex(ValueError, 'only release 0.3.0'):
                release.prepare(version, 'a' * 40, 'unused')

    def test_evidence_commit_command_log_and_inventory(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            release.write(output / 'conformance-report.json', full_report())
            (output / 'log').write_text('synthetic test log')
            manifest = {'source_commit': 'a' * 40, 'evidence': [dict(gate=n, feature_profile=f, command=c,
                source_commit='a' * 40, result='passed', log='log', sha256=release.sha(output / 'log')) for n, f, c in release.gates()]}
            release.validate_evidence(manifest, output)
            for key, value in [('source_commit', 'b' * 40), ('result', 'failed'), ('command', ['true']), ('sha256', '0' * 64), ('feature_profile', 'wrong')]:
                bad = copy.deepcopy(manifest)
                bad['evidence'][0][key] = value
                with self.subTest(key=key), self.assertRaises(ValueError):
                    release.validate_evidence(bad, output)
            manifest['evidence'].pop()
            with self.assertRaisesRegex(ValueError, 'missing release gates'):
                release.validate_evidence(manifest, output)

    def test_blockers_missing_assets_and_tampering(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(release, 'clean'):
            output = Path(tmp)
            manifest = release.metadata()
            manifest.update(source_commit='a' * 40, rustc='rustc 1.89.0 test', evidence=[], artifacts=[])
            path = output / 'release-manifest.json'
            for field, value, message in [('blockers', ['unresolved'], 'blockers'), ('artifacts', [], 'missing publication'), ('crates', [], 'crates'), ('store_versions', {}, 'store_versions')]:
                bad = copy.deepcopy(manifest)
                bad[field] = value
                release.write(path, bad)
                with self.subTest(field=field), self.assertRaisesRegex(ValueError, message):
                    release.verify(path)
            (output / 'tampered').write_text('changed')
            manifest['artifacts'] = [{'path': 'tampered', 'sha256': '0' * 64}]
            release.write(path, manifest)
            with self.assertRaisesRegex(ValueError, 'tampered publication'):
                release.verify(path)

    def test_prepare_refuses_imported_or_stale_output(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(release, 'clean'):
            output = Path(tmp)
            (output / 'conformance-report.json').write_text('{}')
            with self.assertRaisesRegex(ValueError, 'output must be empty'):
                release.prepare('0.3.0', 'a' * 40, output)
            self.assertFalse((output / 'release-manifest.json').exists())

    def test_rehashed_historical_notes_are_not_current_release_notes(self):
        # A self-consistent artifact inventory cannot substitute the old notes.
        with tempfile.TemporaryDirectory() as tmp, patch.object(release, 'clean'):
            output = Path(tmp)
            manifest = release.metadata()
            files = ['source.bundle', 'release-notes.md', 'api-docs.tar', 'conformance-report.json',
                     'performance.json', 'consumer-resolution.json', 'crates/resolution.json',
                     'release-manifest.schema.json', str(release.PROFILE)]
            files += [f'crates/{c["name"]}-{c["version"]}.crate' for c in manifest['crates']]
            for name in files:
                dest = output / name
                dest.parent.mkdir(parents=True, exist_ok=True)
                source = ROOT / name
                dest.write_bytes(source.read_bytes() if source.is_file() else b'synthetic artifact')
            shutil.copytree(ROOT / release.PACKAGE, output / release.PACKAGE)
            shutil.copy2(ROOT / 'docs/releases/0.2.0.md', output / 'release-notes.md')
            manifest.update(source_commit='a' * 40, rustc='rustc 1.89.0 test', evidence=[],
                            artifacts=[dict(path=str(p.relative_to(output)), sha256=release.sha(p))
                                       for p in sorted(output.rglob('*')) if p.is_file()])
            path = output / 'release-manifest.json'
            release.write(path, manifest)
            with self.assertRaisesRegex(ValueError, 'wrong release notes'):
                release.verify(path)

    def test_unsafe_artifact_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            (output / 'link').symlink_to(ROOT / 'Cargo.toml')
            for name in ['../Cargo.toml', '/etc/passwd', 'link', 'absent']:
                with self.subTest(name=name), self.assertRaises(ValueError):
                    release.safe(output, name)


if __name__ == '__main__':
    unittest.main()
