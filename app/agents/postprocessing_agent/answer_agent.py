# app/agents/postprocessing_agent/answer_agent.py
import json
import asyncio
from app.agents.llm.llama_api import call_llama_generate


# ---------------------------
# Limita tamanho e garante leveza
# ---------------------------
def build_table_summary(columns, rows, max_rows=20, max_chars=6000):
    """
    Serializa a tabela de forma segura e compacta.
    Garante que o JSON não fique grande demais para a LLM.
    """
    if not rows:
        return "A consulta não retornou linhas."

    limited = rows[:max_rows]

    table_obj = [
        {col: value for col, value in zip(columns, row)}
        for row in limited
    ]

    text = json.dumps(table_obj, ensure_ascii=False)

    # Se ainda ficar grande demais, corta
    if len(text) > max_chars:
        text = text[:max_chars] + "... (resumo truncado)"

    return text

async def call_llama_generate_safe(prompt: str, timeout: int = 300):
    """
    Garante que a chamada da LLM nunca ultrapasse X segundos.
    Se ultrapassar, devolve um fallback rápido.
    """

    loop = asyncio.get_event_loop()

    try:
        return await asyncio.wait_for(
            loop.run_in_executor(None, call_llama_generate, prompt),
            timeout=timeout
        )
    except asyncio.TimeoutError:
        return "A análise detalhada demorou mais do que o esperado. "\
               "Aqui está um resumo rápido baseado apenas nos dados fornecidos."
    except Exception:
        return "Não foi possível gerar uma explicação detalhada no momento."


# ---------------------------
# Gerador de Resposta Natural
# ---------------------------
async def generate_llm_answer(question: str, columns: list, rows: list):
    """
    Usa a LLM para gerar explicação da resposta SQL.
    """

    table_json = build_table_summary(columns, rows)

    prompt = f"""
Você é um analista de dados que interpreta resultados de SQL.

Regras importantes:
- Explique o resultado relacionando com a pergunta.
- Seja direto, objetivo e verdadeiro.
- NÃO invente dados.
- Baseie-se APENAS no JSON fornecido.
- Destaque valores relevantes.

Pergunta do usuário:
\"\"\"{question}\"\"\"

Resultado (máx 20 linhas):
{table_json}

Escreva a melhor explicação possível em português claro:
    """

    return await call_llama_generate_safe(prompt)

async def generate_llm_answer_from_docs(question: str, docs: list):

    context = "\n\n".join(d["text"] for d in docs[:3])

    prompt = f"""
Responda a pergunta usando SOMENTE o contexto abaixo.
Se não encontrar resposta, diga que não encontrou.

Pergunta:
{question}

Contexto:
{context}

Resposta:
"""

    return await call_llama_generate_safe(prompt)


async def generate_llm_answer_from_docs(question: str, docs: list):

    context = "\n\n".join(d["text"] for d in docs[:3])

    prompt = f"""
Responda a pergunta usando SOMENTE o contexto abaixo.
Se não encontrar resposta, diga que não encontrou.

Pergunta:
{question}

Contexto:
{context}

Resposta:
"""

    return await call_llama_generate_safe(prompt)