#!/usr/bin/env python3
"""Run isolated replication audit probes using Go overlays; never start services."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--baseline', action='store_true', help='run the unchanged package suite')
parser.add_argument('--fixtures', action='store_true', help='run HTTP convergence with temporary fixture repairs')
parser.add_argument('--perf', action='store_true', help='measure scan and chunk processing')
args = parser.parse_args()
root = Path(__file__).resolve().parents[2]
output = Path(tempfile.mkdtemp(prefix='gitea-replication-audit-'))
probe = Path(__file__).with_name('probe_test.go.txt').resolve()
overlay = output / 'overlay.json'
replacements = {str(root / 'modules/replication/replication_audit_probe_test.go'): str(probe)}
overlay.write_text(json.dumps({'Replace': replacements}))
wire = output / 'wire'
wire.mkdir()
failures = []


def run(name, command, experiment=None, codec=False):
    env = os.environ.copy()
    if experiment is not None:
        env['GOEXPERIMENT'] = experiment
    if codec:
        env['REPLICATION_AUDIT_CODEC_DIR'] = str(wire)
    with (output / f'{name}.log').open('w') as log:
        result = subprocess.run(command, cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT, check=False)
    print(f'{name}: exit={result.returncode}', flush=True)
    failures.append(result.returncode)
    return result.returncode


base = ['go', 'test', '-overlay', str(overlay), '-count=1']
run('codec-v1', base + ['-run', '^TestAuditCodec$', '-v', './modules/replication/'], 'nojsonv2', True)
run('codec-v2', base + ['-run', '^TestAuditCodec$', '-v', './modules/replication/'], 'jsonv2', True)
run('safety', base + ['-run', '^(TestAuditSafety|TestAuditSlowChunk|TestAuditLocalConfig)$', '-v', './modules/replication/'])
if args.baseline:
    run('baseline', ['go', 'test', '-json', './modules/replication/'])
    run('baseline-v1', ['go', 'test', '-json', './modules/replication/'], 'nojsonv2')
if args.fixtures:
    # Repair only test inputs: current signing metadata, settings provider,
    # OAuth configuration, and exclusion of the source's node-local app.ini.
    for name in ['control_server_test.go', 'incremental_test.go', 'fence_linux_test.go']:
        path = root / 'modules/replication' / name
        source = path.read_text().replace('signIncrementalManifest(', 'auditSignManifest(')
        source = re.sub(r'(func Test\w+\(t \*testing.T\) \{)', r'\1\n auditFixture(t)', source)
        if name == 'fence_linux_test.go':
            line = 'requireWriteFile(t, filepath.Join(sourceRoot, "custom", "conf", "app.ini"), "primary-config")'
            source = source.replace(line, line + '\n if err:=os.Remove(filepath.Join(sourceRoot,"custom","conf","app.ini"));err!=nil{t.Fatal(err)}')
        copied = output / name
        copied.write_text(source)
        replacements[str(path)] = str(copied)
    fixture_overlay = output / 'fixtures.json'
    fixture_overlay.write_text(json.dumps({'Replace': replacements}))
    run('fixtures', ['go', 'test', '-overlay', str(fixture_overlay), '-count=1', '-run',
        '^(TestIncrementalHTTPRoundTripAndSecondDelta|TestIncrementalFinalizationOnlyRechunksChanges|TestPreflightBaselineOnlyHashesChangedFiles|TestPreflightPerformsPeriodicFullScan|TestPreflightReusesTrustedPreflightManifest)$',
        '-v', './modules/replication/'], 'nojsonv2')
if args.perf:
    binary = output / 'audit.test'
    if run('compile', ['go', 'test', '-overlay', str(overlay), '-c', '-o', str(binary), './modules/replication/']) == 0:
        run('perf', ['/usr/bin/time', '-v', str(binary), '-test.run', '^TestAuditScanPerf$', '-test.v'])
print(f'Results: {output}')

raise SystemExit(1 if any(failures) else 0)
