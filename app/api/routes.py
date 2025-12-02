from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.agents.query_agent.sql_generator import generate_sql
from app.db.connection import get_connection
from app.agents.postprocessing_agent.formatter import format_table
from app.agents.postprocessing_agent.answer_agent import generate_llm_answer_from_docs

from app.agents.mapping_agent.retriever import vector_search


router = APIRouter()

class QueryIn(BaseModel):
    question: str


@router.get("/")
def root():
    return {"status": "ok"}

@router.post("/query")
async def query(payload: QueryIn):
    question = payload.question.strip()

    # ------------------------------------------
    # 1) Gera SQL
    # ------------------------------------------
    sql = generate_sql(question)

    # Caso SQL seja None = LLM decidiu usar docs
    if not sql:
        docs = vector_search(question, top_k=3)

        if docs:
            answer = await generate_llm_answer_from_docs(question, docs)
            return {
                "sql": None,
                "answer": answer,
                "rag_used": True,
                "docs": docs
            }

        return {
            "sql": None,
            "answer": "Não encontrei resposta em SQL nem nos documentos.",
            "rag_used": True
        }

    # ------------------------------------------
    # 2) Executa SQL normalmente
    # ------------------------------------------
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

    # ------------------------------------------
    # 3) Caso tenha linhas => usa SQL
    # ------------------------------------------
    if raw_rows:
        final_answer = await generate_llm_answer(question, cols, raw_rows)

        return {
            "sql": sql,
            "columns": cols,
            "rows": raw_rows,
            "answer": final_answer,
            "rag_used": False
        }

    # ------------------------------------------
    # 4) Senão tenta documentos
    # ------------------------------------------
    docs = vector_search(question, top_k=3)

    if docs:
        answer = await generate_llm_answer_from_docs(question, docs)
        return {
            "sql": sql,
            "columns": [],
            "rows": [],
            "answer": answer,
            "rag_used": True,
            "docs": docs
        }

    # ------------------------------------------
    # 5) fallback final
    # ------------------------------------------
    return {
        "sql": sql,
        "columns": [],
        "rows": [],
        "answer": "Nenhum dado encontrado e nenhum documento relacionado.",
        "rag_used": False
    }