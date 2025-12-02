from pprint import pprint
from collections import Counter
from app.core.config import settings
import json
from app.data_pipeline.Docs_Vettor.doc_ingestor import load_documents
from app.data_pipeline.indexer import index_text_documents
from app.data_pipeline.tag_refiner import refine_tags
from app.data_pipeline.glossary_generator import generate_glossary_from_docs

# Import user modules
try:
    from app.data_pipeline.metadata_extractor import extract_schema
except Exception as e:
    raise RuntimeError("Não foi possível importar extract_schema from metadata_extractor: " + str(e))

# semantic tagging
try:
    from app.data_pipeline.semantic_tagging import assign_semantic_tags, make_table_document
except Exception:
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
except:
    encode_texts = None

try:
    from app.data_pipeline.classifier import classify_table
except:
    classify_table = None

from sklearn.feature_extraction.text import TfidfVectorizer


def main():

    print("[0] Limpando coleção existente no Chroma...")

    try:
        import chromadb

        client = chromadb.PersistentClient(path=settings.chroma_dir)
        try:
            client.delete_collection("db_schema")
            print("✔ Coleção antiga 'db_schema' removida.")
        except:
            print("ℹ Coleção 'db_schema' não existia.")

    except Exception as e:
        print(f"⚠ Falha ao acessar ChromaDB: {e}")

    print("[1/7] Extraindo schema...")
    docs = extract_schema()
    print(f"Encontrados {len(docs)} documentos (tabelas).")

    print("[2/7] Gerando descrições ricas...")
    for d in docs:
        d["id"] = d.get("id") or f"{d['schema']}.{d['table']}"
        d["description"] = d.get("description") or f"Tabela {d['table']} no schema {d['schema']}."

    print("[3/7] Atribuindo tags semânticas...")
    if callable(assign_semantic_tags):
        try:
            tagged = assign_semantic_tags(docs)
            for i, d in enumerate(docs):
                if isinstance(tagged, list):
                    d["tags"] = tagged[i].get("tags", [])
        except Exception as e:
            print("assign_semantic_tags falhou:", e)

    for d in docs:
        d.setdefault("tags", [])

    print("[4/7] Refinando tags heurísticas...")
    docs = refine_tags(docs)

    print("[5/7] Gerando glossário automático...")
    glossary = generate_glossary_from_docs(docs)
    print(f"Glossário sugerido: {len(glossary)} termos")

    print("[6/7] Classificando domínio...")
    if callable(classify_table):
        for d in docs:
            try:
                d["domain"] = classify_table(d)
            except:
                d["domain"] = ""
    else:
        for d in docs:
            d["domain"] = ""

    print("[7/7] Calculando score semântico...")
    if callable(encode_texts):
        texts = [d.get("description") for d in docs]
        try:
            import numpy as np
            norms = [float(np.linalg.norm(e)) for e in encode_texts(texts)]
            for d, s in zip(docs, norms):
                d["semantic_score"] = s
        except:
            for d in docs:
                d["semantic_score"] = 0.0
    else:
        for d in docs:
            d["semantic_score"] = 0.0

    print("[8] Indexando schema...")
    index_documents(docs)

    print("[9] Indexando documentos externos...")
    external_docs = load_documents()
    if external_docs:
        index_text_documents(external_docs)

    print("Pipeline finalizado com sucesso!")


if __name__ == '__main__':
    main()