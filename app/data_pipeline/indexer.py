from chromadb import PersistentClient
from app.core.config import settings
from sentence_transformers import SentenceTransformer
import json

# Carregado uma única vez
EMBED_MODEL_PATH = settings.embed_model_path
embedder = SentenceTransformer(EMBED_MODEL_PATH)


def get_collection():
    client = PersistentClient(path=settings.chroma_dir)

    return client.get_or_create_collection(
        name="db_schema",
        metadata={"hnsw:space": "cosine"}
    )


# ------------------------------------------------------------------
# INDEXAÇÃO DE DOCUMENTOS EXTERNOS (pdf, docx, txt)
# ------------------------------------------------------------------
def index_text_documents(docs):
    """
    docs deve ser no formato:
    [
      ("nome.ext", "conteúdo do arquivo"),
      ...
    ]
    """
    if not docs:
        print("⚠ Nenhum documento externo para indexar.")
        return True

    collection = get_collection()

    ids = [f"doc:{name}" for name, _ in docs]
    texts = [content for _, content in docs]

    embeddings = embedder.encode(texts, show_progress_bar=True)

    collection.upsert(
        ids=ids,
        embeddings=embeddings.tolist(),
        documents=texts,
        metadatas=[
            {
                "type": "external_doc",
                "source": name
            }
            for name, _ in docs
        ]
    )

    print(f"📄 Indexação externa concluída: {len(ids)} docs")
    return True


# ------------------------------------------------------------------
# INDEXAÇÃO DO SCHEMA DO BANCO
# ------------------------------------------------------------------
def index_documents(docs):

    collection = get_collection()

    ids = []
    texts = []
    metadatas = []

    for d in docs:

        columns = d.get("columns", [])
        colnames = {c["name"] for c in columns}

        # Campos essenciais de ERP
        ESSENTIAL_COLS = {
            "ativo": "VARCHAR(1)",
            "tipo_entidade": "VARCHAR(1)",
            "codcli": "BIGINT",
        }

        for col_name, col_type in ESSENTIAL_COLS.items():
            if col_name not in colnames:
                columns.append({"name": col_name, "type": col_type})

        text = d.get("description") or (
            f"Tabela {d['table']} do schema {d['schema']}. "
            f"Colunas: {', '.join([c['name'] for c in columns])}. "
        )

        ids.append(d["id"])
        texts.append(text)

        metadatas.append({
            "table": d["table"],
            "schema": d["schema"],

            # SEMPRE JSON:
            "columns": json.dumps(columns),
            "tags": json.dumps(d.get("tags", [])),
            "glossary_terms": json.dumps(d.get("glossary", [])),

            "pk": ", ".join(d.get("pk", [])),
            "row_count": str(d.get("row_count", "")),
            "semantic_score": float(d.get("semantic_score", 0)),
            "domain": d.get("domain", ""),
        })

    # Gera embeddings das tabelas
    embeddings = embedder.encode(texts, show_progress_bar=True)

    collection.upsert(
        ids=ids,
        embeddings=embeddings.tolist(),
        documents=texts,
        metadatas=metadatas
    )

    print("✅ Indexação completa no ChromaDB.")
    print(f"📦 {len(ids)} tabelas indexadas.")
    return True