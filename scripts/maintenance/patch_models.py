
path = 'src/models/game.py'
with open(path, 'r') as f:
    lines = f.readlines()

new_lines_content = [
    '    franchise_id = Column(Integer, nullable=True)\n',
    '    canonical_team_code = Column(String(10), nullable=True)\n'
]

# Insert in reverse order to maintain indices
# GameLineup: original 130 (0-indexed 129). Insert after, so at index 130.
# GameBattingStat: original 154 (0-indexed 153). Insert after, so at index 154.
# GamePitchingStat: original 201 (0-indexed 200). Insert after, so at index 201.

insert_indices = [201, 154, 130]

for idx in insert_indices:
    lines[idx:idx] = new_lines_content

with open(path, 'w') as f:
    f.writelines(lines)

print(f"Patched {path} at indices {insert_indices}")
