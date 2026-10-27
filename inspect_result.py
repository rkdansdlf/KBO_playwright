"""Print the pieces of verify_dim256_result.json needed for the report."""

from __future__ import annotations

import json
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

payload = json.loads(Path("verify_dim256_result.json").read_text(encoding="utf-8"))
print("summary:")
for dim, s in payload["summary"].items():
    print(f"  {dim}: {s}")
print(f"regressions 1536->256: {len(payload['regressions_1536_to_256'])}")
print(f"improvements 1536->256: {len(payload['improvements_1536_to_256'])}")
print()
for q in payload["per_query"]:
    if q["id"] in ("prod_rank_013", "prod_highlight_027", "prod_rule_020"):
        print(f"== {q['id']} filters={q['filters']} candidates={q['candidates']}")
        print(f"   query: {q['query']!r}")
        print(f"   relevant: {q['relevant']}")
        for dim, d in q["dims"].items():
            print(
                f"   dim {dim:>4}: hits={d['hits']} rank={d['first_rank']} mrr={d['mrr']:.3f} "
                f"top5={d['top_ids']}"
            )
        print()
