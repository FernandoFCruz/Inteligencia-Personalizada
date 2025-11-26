# tag_refiner.py
import re
from collections import Counter

PII_PATTERNS = ["cpf","cnpj","email","telefone","telefone_celular","rg","documento"]
ID_PATTERNS = ["id","_id","uuid","codigo","cod"]

def refine_tags(docs):
    for d in docs:
        cols = [c["name"].lower() for c in d.get("columns",[])]
        tags = set(d.get("tags", []))

        # regra: PII se houver colunas cpf/cnpj/email
        if any(re.search(r"\b"+p+r"\b", " ".join(cols)) for p in ["cpf","cnpj","email"]):
            tags.add("pii")

        # regra: id detection
        if any(any(p in c for p in ID_PATTERNS) for c in cols):
            tags.add("identifier")

        # boost: se FK indica dominio
        if any("fk" in c for c in cols):
            tags.add("has_fk")

        # small heuristics: table name contains 'log' -> audit
        if "log" in d["table"].lower() or "audit" in d["table"].lower():
            tags.add("audit")

        d["tags"] = list(tags)
    return docs