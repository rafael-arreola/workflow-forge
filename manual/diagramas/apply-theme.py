"""Apply the manual palette after Archify deliver, preserving provenance receipts."""
from pathlib import Path
import hashlib
import json
import re
import sys

ROOT = Path(__file__).resolve().parent
NAMES = ('arquitectura', 'arranque', 'modulos', 'datos', 'senales')

def digest(data):
    return {'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)}

def neutral_hex(match):
    value = match.group(1)
    if len(value) in (3, 4):
        value = ''.join(c * 2 for c in value)
    gray = round(sum(int(value[i:i + 2], 16) * w for i, w in ((0, .2126), (2, .7152), (4, .0722))))
    return '#' + f'{gray:02x}' * 3 + value[6:]

def neutral_rgb(match):
    gray = round(float(match[2]) * .2126 + float(match[3]) * .7152 + float(match[4]) * .0722)
    return f'{match[1]}({gray}, {gray}, {gray}{match[5]})'

def neutral_style(match):
    if 'archify-fonts' in match[1]:
        return match[0]
    css = re.sub(r'#([\da-fA-F]{8}|[\da-fA-F]{6}|[\da-fA-F]{4}|[\da-fA-F]{3})\b', neutral_hex, match[2])
    css = re.sub(r'\b(rgb|rgba)\(\s*([\d.]+)\s*,\s*([\d.]+)\s*,\s*([\d.]+)(\s*,\s*[\d.]+\s*|\s*)\)', neutral_rgb, css)
    return match[1] + css + '</style>'

css = (ROOT / 'manual-theme.css').read_text()
for name in sys.argv[1:] or NAMES:
    if name not in NAMES:
        raise SystemExit(f'Unknown diagram: {name}')
    target = ROOT / f'{name}.html'
    original = target.read_bytes()
    source = json.loads((ROOT / f'{name}.delivery.json').read_text())
    themed_receipt = ROOT / f'{name}.theme.json'
    if digest(original) != source['artifact']:
        if themed_receipt.exists():
            old = json.loads(themed_receipt.read_text())
            if old['artifact'] == digest(original) and old['stylesheet'] == digest(css.encode()) and old.get('transform') == digest(Path(__file__).read_bytes()):
                print(f'{name}: already themed')
                continue
        raise SystemExit(f'{name}: run Archify deliver again before applying the theme')
    html = re.sub(r'(<style\b[^>]*>)(.*?)</style>', neutral_style, original.decode(), flags=re.S)
    html = html.replace('data-theme="dark" data-preset="classic"', 'data-theme="light" data-preset="classic"', 1)
    html = html.replace("window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark'", "'light'")
    html = html.replace('</head>', '<style id="manual-palette">\n' + css + '</style>\n</head>', 1)
    output = html.encode()
    target.write_bytes(output)
    themed_receipt.write_text(json.dumps({
        'transform': digest(Path(__file__).read_bytes()),
        'reason': 'User-requested manual palette; styling only, same SVG geometry and interaction logic.',
        'source_delivery': f'{name}.delivery.json',
        'source_artifact': source['artifact'],
        'stylesheet': digest(css.encode()),
        'artifact': digest(output),
    }, indent=2) + '\n')
    print(f'{name}: manual palette applied')
