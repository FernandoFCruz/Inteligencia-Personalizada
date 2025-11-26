# run_full_pipeline.py
"""
Pipeline orquestrador final.
- Extrai schema usando app.data_pipeline.metadata_extractor.extract_schema
- Gera/normaliza descrições (faz fallback se make_table_document existir)
- Atribui tags iniciais via semantic_tagging.assign_semantic_tags
- Refina tags com heurísticas simples
- Gera um glossário agregado usando TF-IDF (keywords frequent)
- Calcula um score semântico (usa embedding_service if available)
- Classifica domínio (usa classifier.classify_table if available)
- Indexa tudo no Chroma via indexer.index_documents (seu indexer existente)

Salve este arquivo em: app/data_pipeline/run_full_pipeline.py
Execute: python -m app.data_pipeline.run_full_pipeline
"""

from pprint import pprint
from collections import Counter
from app.core.config import settings
import json

# Import user modules
try:
    from app.data_pipeline.metadata_extractor import extract_schema
except Exception as e:
    raise RuntimeError("Não foi possível importar extract_schema from metadata_extractor: " + str(e))

# semantic tagging
try:
    from app.data_pipeline.semantic_tagging import assign_semantic_tags, make_table_document
except Exception:
    # fallback name if your module uses different names
    assign_semantic_tags = None
    make_table_document = None

# indexer
try:
    from app.data_pipeline.indexer import index_documents
except Exception as e:
    raise RuntimeError("Não foi possível importar indexer.index_documents: " + str(e))

# optional helpers
try:
    from app.data_pipeline.embedding_service import encode_texts
except Exception:
    # try alternative path
    try:
        from app.data_pipeline.embedding_service import encode_texts
    except Exception:
        encode_texts = None

try:
    from app.data_pipeline.classifier import classify_table
except Exception:
    classify_table = None

# sklearn for keyword extraction
from sklearn.feature_extraction.text import TfidfVectorizer


# ---------------------------
# Utilities
# ---------------------------

def rich_text_for_doc(d):
    # Prefer your make_table_document if available
    if callable(make_table_document):
        try:
            return make_table_document(d)
        except Exception:
            pass

    # Fallback rich text construction
    cols = ", ".join([c.get("name", "") for c in d.get("columns", [])])
    pk = ", ".join(d.get("pk", [])) or "nenhuma"
    examples = d.get("examples") or []
    text = (
        f"Tabela {d.get('table')} do schema {d.get('schema')}. "
        f"Colunas: {cols}. "
        f"Primary key: {pk}. "
        f"Exemplos: {examples}."
    )
    return text


def refine_tags_by_rules(docs):
    import re
    PII = [r"\bcpf\b", r"\bcnpj\b", r"\bemail\b", r"\btelefone\b"]
    ID_PAT = ["id", "_id", "uuid", "codigo", "cod"]

    for d in docs:
        cols = [c.get("name", "").lower() for c in d.get("columns", [])]
        tags = set(d.get("tags", []))

        # PII
        for pat in PII:
            if any(re.search(pat, c) for c in cols):
                tags.add("pii")

        # identifier
        if any(any(p in c for p in ID_PAT) for c in cols):
            tags.add("identifier")

        # audit/log
        tname = d.get("table", "").lower()
        if "log" in tname or "audit" in tname:
            tags.add("audit")

        # foreign keys heuristics
        if any("fk" in (c.get("name","") or "").lower() for c in d.get("columns", [])):
            tags.add("has_fk")

        d["tags"] = list(tags)
    return docs


def generate_glossary_from_texts(docs, top_k_terms=200, per_doc=5):
    texts = [d.get("description") or rich_text_for_doc(d) for d in docs]
    if not texts:
        return []

    vec = TfidfVectorizer(max_features=5000, ngram_range=(1, 2))
    X = vec.fit_transform(texts)
    features = vec.get_feature_names_out()

    candidates = []
    for i in range(X.shape[0]):
        row = X[i].toarray().ravel()
        idx = row.argsort()[-per_doc:][::-1]
        for j in idx:
            if row[j] > 0:
                candidates.append(features[j])

    # frequency and pick top_k_terms
    freq = Counter(candidates)
    most = [t for t, _ in freq.most_common(top_k_terms)]

    glossary = []
    for term in most:
        glossary.append({"term": term, "definition": f"Termo identificado automaticamente: {term}"})
    return glossary


# ---------------------------
# Main pipeline
# ---------------------------

def main():

    print("[0] Limpando coleção existente no Chroma...")

    try:
        import chromadb
        from chromadb.config import Settings

        client = chromadb.PersistentClient(path=settings.chroma_dir)

        try:
            client.delete_collection("db_schema")
            print(" Coleção antiga 'db_schema' removida.")
        except Exception:
            print("ℹ Coleção 'db_schema' não existia, seguindo.")

    except Exception as e:
        print(f"⚠ Falha ao acessar ChromaDB: {e}")

    print("[1/7] Extraindo schema...")
    docs = extract_schema()
    print(f"Encontrados {len(docs)} documentos (tabelas).")

    print("[2/7] Gerando descricoes ricas e ids normalizados...")
    for d in docs:
        d.setdefault("schema", d.get("schema") or d.get("table", "public").split(".")[0])
        d.setdefault("table", d.get("table") or d.get("id", ""))
        d["id"] = d.get("id") or f"{d['schema']}.{d['table']}"
        d["description"] = d.get("description") or rich_text_for_doc(d)

    print("[3/7] Atribuindo tags semanticas iniciais (se disponível)...")
    if callable(assign_semantic_tags):
        try:
            tagged = assign_semantic_tags(docs)
            # assign_semantic_tags might return new docs or tags field
            for i, d in enumerate(docs):
                if isinstance(tagged, list) and i < len(tagged):
                    d_tags = tagged[i].get("tags") if isinstance(tagged[i], dict) else None
                    if d_tags:
                        d["tags"] = d_tags
        except Exception as e:
            print("assign_semantic_tags falhou:", e)

    # ensure tags field exists
    for d in docs:
        d.setdefault("tags", [])

    print("[4/7] Refinando tags por regras heuristicas...")
    docs = refine_tags_by_rules(docs)

    print("[5/7] Gerando glossario automatico (TF-IDF)...")
    glossary = generate_glossary_from_texts(docs, top_k_terms=200, per_doc=5)
    print(f"Glossario sugerido: {len(glossary)} termos")

    # attach small per-doc glossary (top keywords per doc)
    for i, d in enumerate(docs):
        text = d.get("description")
        # simple per-doc top keywords using same vectorizer on single doc
        # fallback: use first N glossary terms that appear in text
        found = [t for t in (term["term"] for term in glossary) if t in text]
        d["glossary"] = found[:8]

    print("[6/7] Classificando dominio (se classificador disponivel)...")
    if callable(classify_table):
        try:
            for d in docs:
                d["domain"] = classify_table(d)
        except Exception as e:
            print("classify_table falhou:", e)
            for d in docs:
                d.setdefault("domain", "")
    else:
        for d in docs:
            d.setdefault("domain", "")

    print("[7/7] Calculando score semantico (se embedding service disponivel)...")
    if callable(encode_texts):
        try:
            texts = [d.get("description") for d in docs]
            embs = encode_texts(texts)
            # use L2 norm mean as a simple proxy score (or any other metric)
            import numpy as np
            norms = [float(np.linalg.norm(e)) for e in embs]
            for d, s in zip(docs, norms):
                d["semantic_score"] = float(s)
        except Exception as e:
            print("encode_texts falhou:", e)
            for d in docs:
                d.setdefault("semantic_score", 0.0)
    else:
        for d in docs:
            d.setdefault("semantic_score", 0.0)

    print("[8] Indexando no Chroma (indexer.index_documents)...")
    # indexer expects list of docs with fields such as id, table, schema, columns, description, tags, glossary, semantic_score
    success = index_documents(docs)
    if success:
        print("Pipeline finalizado com sucesso!")
    else:
        print("Pipeline finalizado, mas indexer retornou False — verifique logs.")


if __name__ == '__main__':
    main()