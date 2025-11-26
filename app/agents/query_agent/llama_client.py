import requests
import time
import re
from app.core.config import settings

LLAMA_URL = f"{settings.llama_server}/api/generate"
MODEL_NAME = "llama3.1"

def call_llama_generate(prompt: str, retries=3):
    """
    Chama o LLM com retry automático e timeout expandido.
    """

    payload = {
        "model": MODEL_NAME,
        "prompt": prompt,
        "stream": False
    }

    for attempt in range(1, retries + 1):
        try:
            response = requests.post(
                LLAMA_URL,
                json=payload,
                timeout=180
            )
            data = response.json()
            raw = data.get("response", "").strip()

            raw = re.sub(r"```.*?```", "", raw, flags=re.DOTALL)
            raw = raw.replace("`", "")

            return raw

        except requests.exceptions.Timeout:
            print(f"[WARN] Timeout na tentativa {attempt}/{retries}...")
            if attempt == retries:
                raise Exception("Erro ao conectar ao servidor LLM: tempo limite atingido")
            time.sleep(2)

        except Exception as e:
            print(f"[WARN] Erro ao conectar ao LLM: {e}")
            if attempt == retries:
                raise Exception(f"Erro ao conectar ao servidor LLM: {e}")
            time.sleep(2)