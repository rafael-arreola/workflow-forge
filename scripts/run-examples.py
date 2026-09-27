#!/usr/bin/env python3
"""Run every local proof of concept; use isolated databases and bounded probe sizes."""
from pathlib import Path
import argparse
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--group', choices=['all', 'hosts', 'measurements', 'manual'], default='all')
selected = parser.parse_args().group


def run(*args):
    print('+ ' + ' '.join(map(str, args)), flush=True)
    subprocess.run(list(map(str, args)), cwd=ROOT, check=True, timeout=300)


def host(name, *args):
    run('cargo', 'run', '--locked', '-p', 'workflow-forge-examples',
        '--all-features', '--example', name, '--', *args)


with tempfile.TemporaryDirectory(prefix='workflow-forge-examples-') as directory:
    scratch = Path(directory)
    if selected in ['all', 'hosts']:
        for example in ['v2_customer', 'v2_outcomes']:
            host(example)
        run('cargo', 'run', '--locked', '-p', 'workflow-forge-authoring-example',
            '--example', 'round_trip')
        host('v2_inventory', '10', '1')
        for _ in range(2):
            host('v2_sqlite', scratch / 'customer.sqlite')
        host('v2_signal', scratch / 'signal.sqlite', 'start')
        host('v2_signal', scratch / 'signal.sqlite', 'signal')
    if selected in ['all', 'measurements']:
        for provider in ['memory', 'sqlite']:
            def flags(name):
                return [] if provider == 'memory' else ['--sqlite', scratch / f'{name}.sqlite']
            host('v2_measure', '1', '1024', '1000', '2', '--terminal-runs', '16', *flags('measure'))
            host('v2_capacity', 'saturation', '2', *flags('saturation'))
            host('v2_capacity', 'release', '2', '1024', *flags('release'))
    if selected in ['all', 'manual']:
        for example in ['inicio', 'sistema', 'cancelacion', 'modulos', 'respuestas',
                        'importacion', 'controles', 'artefactos', 'senales', 'durable']:
            args = [scratch / 'manual.sqlite', 'manual-receipt'] if example == 'durable' else []
            run('cargo', 'run', '--manifest-path', 'manual/ejemplos/Cargo.toml',
                '--locked', '--bin', example, '--', *args)
print(f'Proofs of concept passed: {selected}.', flush=True)
