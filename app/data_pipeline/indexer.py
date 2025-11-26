from chromadb import PersistentClient
from app.core.config import settings
from sentence_transformers import SentenceTransformer
import json

# Carrega embedder 1x
EMBED_MODEL_PATH = settings.embed_model_path
embedder = SentenceTransformer(EMBED_MODEL_PATH)


def index_documents(docs):
    """
    Indexa documentos completos no ChromaDB, incluindo:
    - descrição rica
    - tags geradas
    - termos de glossário
    - score semântico
    - metadados estruturais
    - domínio classificado
    """

    client = PersistentClient(path=settings.chroma_dir)

    collection = client.get_or_create_collection(
        name="db_schema",
        metadata={"hnsw:space": "cosine"}
    )

    ids = []
    texts = []
    metadatas = []

    for d in docs:

        # ---------------------------------------------------
        # GARANTE que colunas essenciais SEMPRE existam
        # ---------------------------------------------------
        columns = d.get("columns", [])
        colnames = {c["name"] for c in columns}

        ESSENTIAL_COLS = {
            "ativo": "VARCHAR(1)",
            "tipo_entidade": "VARCHAR(1)",
            "codcli": "BIGINT",
        }

        for col_name, col_type in ESSENTIAL_COLS.items():
            if col_name not in colnames:
                columns.append({"name": col_name, "type": col_type})

        # ---------------------------------------------------
        # TEXTO RICO (melhor para embeddings)
        # ---------------------------------------------------
        text = d.get("description") or (
            f"Tabela {d['table']} do schema {d['schema']}. "
            f"Colunas: {', '.join([c['name'] for c in columns])}. "
        )

        ids.append(d["id"])
        texts.append(text)

        # ---------------------------------------------------
        # METADADOS COMPLETOS — sempre str, como Chroma exige
        # ---------------------------------------------------
        metadatas.append({
            "table": d["table"],
            "schema": d["schema"],

            # CORRIGIDO: sempre uma string JSON válida
            "columns": json.dumps(columns),

            "pk": ", ".join(d.get("pk", [])),
            "row_count": str(d.get("row_count", "")),

            # também sempre JSON string
            "tags": json.dumps(d.get("tags", [])),
            "glossary_terms": json.dumps(d.get("glossary", [])),

            "semantic_score": float(d.get("semantic_score", 0)),
            "domain": d.get("domain", ""),
        })

    # ---------------------------------------------------
    # EMBEDDINGS
    # ---------------------------------------------------
    embeddings = embedder.encode(texts, show_progress_bar=True)

    # ---------------------------------------------------
    # INDEXAÇÃO
    # ---------------------------------------------------
    collection.upsert(
        ids=ids,
        embeddings=embeddings.tolist(),
        documents=texts,
        metadatas=metadatas
    )

    print("✅ Indexação completa no ChromaDB.")
    print(f"📦 {len(ids)} itens atualizados.")
    return True