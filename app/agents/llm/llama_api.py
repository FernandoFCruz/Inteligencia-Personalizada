import requests
import re
from app.core.config import settings

LLAMA_URL = f"{settings.llama_server}/api/generate"
MODEL_NAME = "llama3.1"


def call_llama_generate(prompt: str) -> str:
    try:
        response = requests.post(
            LLAMA_URL,
            json={
                "model": MODEL_NAME,
                "prompt": prompt,
                "stream": False
            },
            timeout=300
        )
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Erro ao conectar ao servidor LLM: {e}")

    # valida status HTTP
    if response.status_code != 200:
        raise RuntimeError(f"Ollama retornou HTTP {response.status_code}: {response.text}")

    # tenta decodificar JSON
    try:
        data = response.json()
    except Exception as e:
        raise RuntimeError(f"Erro ao interpretar JSON da resposta do LLM: {e}")

    # extrai conteúdo
    raw = data.get("response")
    if not raw:
        raise RuntimeError("LLM retornou resposta vazia ou inválida.")

    # limpeza mínima (a limpeza pesada é no sql_generator)
    raw = re.sub(r"```.*?```", "", raw, flags=re.DOTALL)
    raw = raw.replace("`", "")

    return raw.strip()