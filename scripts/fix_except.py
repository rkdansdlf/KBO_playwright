import re, pathlib

pat = re.compile(r'except\s+([A-Za-z_][\w.]*(?:,\s*[A-Za-z_][\w.]*)+):')
count = 0
for root in ['src', 'scripts', 'tools']:
    for p in pathlib.Path(root).rglob('*.py'):
        s = p.read_text(encoding='utf-8')
        new = pat.sub(lambda m: 'except (' + m.group(1) + '):', s)
        if new != s:
            p.write_text(new, encoding='utf-8')
            count += 1
print('files changed:', count)
PY