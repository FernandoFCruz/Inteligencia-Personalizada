from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.agents.query_agent.sql_generator import generate_sql
from app.db.connection import get_connection
from app.agents.postprocessing_agent.formatter import format_table
from app.agents.postprocessing_agent.answer_agent import generate_llm_answer


router = APIRouter()

class QueryIn(BaseModel):
    question: str


@router.get("/")
def root():
    return {"status": "ok"}


@router.post("/query")
async def query(payload: QueryIn):
    question = payload.question.strip()

    # 1 — Gerar SQL
    try:
        sql = generate_sql(question)
        if not sql.strip():
            raise ValueError("SQL vazio.")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao gerar SQL: {e}")

    # 2 — Executar SQL
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute(sql)
        raw_rows = cur.fetchall() if cur.description else []
        cols = [d[0] for d in cur.description] if cur.description else []

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao executar SQL: {e}")
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

    # --- serialização segura ---
    def clean_value(v):
        if isinstance(v, (int, float, str, bool)) or v is None:
            return v
        return str(v)

    rows = [
        [clean_value(value) for value in row]
        for row in raw_rows
    ]

    formatted = format_table(cols, rows)

    # 3 — Chamar a explicação LLM — agora com await
    final_answer = await generate_llm_answer(question, cols, rows)

    return {
        "sql": sql,
        "columns": cols,
        "rows": rows,
        "answer": final_answer,
        "result": formatted
    }