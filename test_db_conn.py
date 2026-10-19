from src.db.vector_engine import get_vector_session
from sqlalchemy import text
with get_vector_session() as s:
    print("count:", s.execute(text("SELECT count(*) FROM rag_chunks")).scalar())
    print(s.execute(text("SELECT count(*) FROM rag_chunks WHERE embedding IS NOT NULL")).scalar())
