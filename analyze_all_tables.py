"""Analyze all tables in the HTML."""
from pathlib import Path
from bs4 import BeautifulSoup

html_file = Path("debug_futures_clicked.html")
html = html_file.read_text(encoding='utf-8')
soup = BeautifulSoup(html, 'lxml')

all_tables = soup.find_all('table')
print(f"Total tables: {len(all_tables)}\n")

for i, table in enumerate(all_tables):
    print(f"=== TABLE {i+1} ===")
    print(f"  ID: {table.get('id')}")
    print(f"  Class: {table.get('class')}")
    print(f"  Summary: {table.get('summary')}")

    # Parent div
    parent = table.find_parent('div')
    if parent:
        print(f"  Parent div ID: {parent.get('id')}")

    # Headers
    thead = table.find('thead')
    if thead:
        tr = thead.find('tr')
        if tr:
            headers = [th.get_text(strip=True) for th in tr.find_all(['th', 'td'])]
            print(f"  Headers: {headers}")
    else:
        # Try first row
        first_tr = table.find('tr')
        if first_tr:
            cells = [cell.get_text(strip=True) for cell in first_tr.find_all(['th', 'td'])]
            print(f"  First row (potential headers): {cells}")

    # Count rows
    rows = table.find_all('tr')
    print(f"  Total rows: {len(rows)}")

    print()
