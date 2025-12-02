import os
import json
import joblib
import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer
from app.core.config import settings
import numpy as np
from app.data_pipeline.classifier import TableClassifier


CLASSIFIER_PATH = "app/agents/mapping_agent/table_classifier.joblib"

EMBED_MODEL_PATH = settings.embed_model_path
embedder = SentenceTransformer(EMBED_MODEL_PATH)


# -------------------------------------------
# CARREGAR CLASSIFICADOR
# -------------------------------------------

def load_classifier():
    clf = TableClassifier()
    if clf.model is None:
        return None
    return clf


# -------------------------------------------
# CLIENTE DO CHROMA
# -------------------------------------------
def chroma_client():
    return chromadb.PersistentClient(path=settings.chroma_dir)


# -------------------------------------------
# SCORING TAGS SEMÂNTICAS
# -------------------------------------------
def score_semantic_tags(question: str, tags: list[str]) -> float:
    if not tags:
        return 0.0

    q_emb = embedder.encode(question)
    t_emb = embedder.encode(tags)

    sims = np.dot(t_emb, q_emb) / (
        np.linalg.norm(t_emb, axis=1) * np.linalg.norm(q_emb) + 1e-9
    )
    return float(max(sims))


# -------------------------------------------
# NORMALIZAÇÃO ROBUSTA DE METADADOS
# -------------------------------------------

def normalize_json_field(value):
    """
    Aceita:
    - lista de dicts
    - lista de strings JSON
    - string contendo JSON
    - None
    Retorna SEMPRE: lista de dicts limpos
    """
    if value is None:
        return []

    # caso seja string JSON
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except:
            return []

    # caso seja lista
    if isinstance(value, list):
        clean = []
        for item in value:
            if isinstance(item, dict):
                clean.append(item)
            elif isinstance(item, str):
                try:
                    obj = json.loads(item)
                    if isinstance(obj, dict):
                        clean.append(obj)
                except:
                    pass
        return clean

    return []


def normalize_string_list(value):
    """Campo tags e glossary — podem ser string JSON ou lista."""
    if value is None:
        return []

    if isinstance(value, str):
        try:
            return json.loads(value)
        except:
            return []

    if isinstance(value, list):
        return value

    return []


# -------------------------------------------
# VECTOR SEARCH — versão fortificada
# -------------------------------------------
def vector_search(question: str, top_k: int = 10, threshold=0.05):
    client = chroma_client()

    try:
        col = client.get_collection("db_schema")
    except:
        return []

    q_emb = embedder.encode([question]).tolist()

    res = col.query(
        query_embeddings=q_emb,
        n_results=top_k,
        include=["documents", "metadatas", "distances"]
    )

    ids = res.get("ids", [[]])[0]
    docs = res.get("documents", [[]])[0]
    metas = res.get("metadatas", [[]])[0]
    dists = res.get("distances", [[]])[0]

    out = []

    for i in range(len(ids)):
        tid = ids[i]
        meta = metas[i] if i < len(metas) else {}
        doc = docs[i] if i < len(docs) else ""
        dist = dists[i] if i < len(dists) else 0.0
        if dist < threshold:
                continue

        if not isinstance(meta, dict):
            continue

        # NORMALIZAÇÃO 100% — remove erro de coluna inválida
        columns = normalize_json_field(meta.get("columns"))
        tags = normalize_string_list(meta.get("tags"))
        glossary = normalize_string_list(meta.get("glossary_terms"))

        out.append({
            "id": tid,
            "text": doc or "",
            "score": float(dist),
            "columns": columns,
            "tags": tags,
            "glossary_terms": glossary,
            "schema": meta.get("schema"),
            "table": meta.get("table"),
            "domain": meta.get("domain", "")
        })

    return out


# -------------------------------------------
# BUSCA PELO CLASSIFICADOR
# -------------------------------------------
def classifier_search(question, top_k=10, threshold=0.05):
    clf = load_classifier()
    if clf is None:
        return None

    try:
        predictions = clf.predict(question, top_k=top_k)

        results = []
        for p in predictions:
            if p["score"] < threshold:
                continue

            table_id = p["id"]
            chroma_result = vector_search(table_id, top_k=1)

            if chroma_result:
                enriched = chroma_result[0]
                enriched["score"] = p["score"]
                results.append(enriched)

        return results or None

    except Exception as e:
        print(f"[WARN] Erro no classificador: {e}")
        return None

def map_tables(question: str, top_k: int = 10):
    classified = classifier_search(question, top_k=top_k)

    # fallback para vector search
    tables = classified or vector_search(question, top_k=top_k)

    # garante que seja lista
    if not tables or not isinstance(tables, list):
        return []

    # detecta documento externo
    if tables and tables[0]["id"].startswith("doc:"):
        return [{
            "type": "doc",
            "items": tables
        }]

    # senão são tabelas SQL
    return [{
        "type": "table",
        "items": tables
    }]