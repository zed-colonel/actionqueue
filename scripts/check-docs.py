#!/usr/bin/env python3
"""Check active Markdown file links and generated ActionQueue API legacy boundaries."""
import argparse
import json
from pathlib import Path
import re
import subprocess
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[1]


def check(api=None):
    errors = []
    files = subprocess.check_output(['git', 'ls-files', '-z'], cwd=ROOT).decode().split('\0')
    exempt = ('archive/', 'docs/contracts/', 'docs/planning/', 'docs/adrs/')
    for name in files:
        if not name.endswith('.md') or name.startswith(exempt):
            continue
        path = ROOT / name
        for link in re.findall(r'\]\(([^\s)]+)(?:\s+"[^"]*")?\)', path.read_text()):
            link = unquote(link.split('#')[0])
            if not link or re.match(r'[a-z]+:', link) or link.startswith('/'):
                continue
            if not (path.parent / link).exists():
                errors.append(f'{name}: broken file link {link}')
    if api:
        api = Path(api)
        policy = json.loads((ROOT / 'conformance/aq-cont-1/contract-boundaries.json').read_text())
        symbols = [x['symbol'] for x in policy['legacy_symbols']['symbols'] if x['stage'] == 'forbid']
        pages = [p for p in api.rglob('*.html') if p.relative_to(api).parts[0].startswith('actionqueue_')]
        if not pages:
            errors.append('missing generated ActionQueue API pages')
        for path in pages:
            content = path.read_text()
            for symbol in symbols:
                if symbol in content:
                    errors.append(f'{path}: forbidden legacy symbol {symbol}')
    if errors:
        raise ValueError('\n'.join(errors))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--api')
    check(parser.parse_args().api)
