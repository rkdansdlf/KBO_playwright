"""Analyze saved HTML to find Futures tables."""
from pathlib import Path
from bs4 import BeautifulSoup

html_file = Path("debug_futures_clicked.html")
if not html_file.exists():
    print("HTML file not found")
    exit(1)

html = html_file.read_text(encoding='utf-8')
soup = BeautifulSoup(html, 'lxml')

print("=== DIV IDs containing 'Futures' ===")
futures_divs = soup.find_all('div', id=lambda x: x and 'Futures' in x if x else False)
for div in futures_divs:
    print(f"  - {div.get('id')}")
    tables = div.find_all('table')
    print(f"    Tables: {len(tables)}")

print("\n=== Tables with class containing 'tData' ===")
tdata_tables = soup.find_all('table', class_=lambda x: x and 'tData' in x if x else False)
print(f"Found {len(tdata_tables)} tables")
for i, table in enumerate(tdata_tables[:3]):
    print(f"\nTable {i+1}:")
    print(f"  Class: {table.get('class')}")

    # Headers
    thead = table.find('thead')
    if thead:
        headers = [th.get_text(strip=True) for th in thead.find_all(['th', 'td'])]
        print(f"  Headers: {headers[:10]}")

    # First row
    tbody = table.find('tbody')
    if tbody:
        first_row = tbody.find('tr')
        if first_row:
            cells = [td.get_text(strip=True) for td in first_row.find_all(['th', 'td'])]
            print(f"  First row: {cells[:10]}")

print("\n=== All tables ===")
all_tables = soup.find_all('table')
print(f"Total tables: {len(all_tables)}")

print("\n=== Sample: First table with class 'tData' ===")
for table in all_tables:
    cls = table.get('class', [])
    if cls and any('tData' in c for c in cls):
        print(f"Class: {cls}")
        thead = table.find('thead')
        if thead:
            tr = thead.find('tr')
            if tr:
                headers = [th.get_text(strip=True) for th in tr.find_all(['th', 'td'])]
                print(f"Headers ({len(headers)}): {headers}")
        break
