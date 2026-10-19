import os
from dotenv import load_dotenv
load_dotenv()
import httpx
key = os.getenv("OPENROUTER_API_KEY")
print("key present:", bool(key))
r = httpx.post("https://openrouter.ai/api/v1/embeddings", headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"}, json={"model":"nvidia/nemotron-3-embed-1b:free","input":["테스트 문장입니다."]}, timeout=60)
print("status", r.status_code)
data = r.json()
print("keys", list(data.keys()))
vecs = data.get("data", [])
print("num", len(vecs))
if vecs:
    print("dim", len(vecs[0]["embedding"]))
    print("first5", vecs[0]["embedding"][:5])
