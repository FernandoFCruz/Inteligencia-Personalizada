import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer
from app.core.config import settings

EMBED_MODEL_PATH = settings.embed_model_path
embedder = SentenceTransformer(EMBED_MODEL_PATH)


def index_documents_with_tags(docs):
    """
    Indexa documentos semânticos usando multilingual-e5-base.
    Metadados são salvos com tipos compatíveis com Chroma (str, int, float, bool).
    Listas são convertidas para JSON string.
    """

    import json

    client = chromadb.PersistentClient(path=settings.chroma_dir)

    collection = client.get_or_create_collection(
        name="db_schema",
        metadata={"hnsw:space": "cosine"}
    )

    ids = []
    embeddings = []
    metadatas = []
    documents = []

    for d in docs:
        doc_id = d.get("id")
        text = d.get("description")

        # encoding
        emb = embedder.encode(text).tolist()

        # metadata safe-conversion
        metadata = {}
        for k, v in d.items():
            if isinstance(v, (str, int, float, bool)):
                metadata[k] = v
            else:
                metadata[k] = json.dumps(v)

        ids.append(doc_id)
        embeddings.append(emb)
        metadatas.append(metadata)
        documents.append(text)

    # batch add
    collection.add(
        ids=ids,
        embeddings=embeddings,
        metadatas=metadatas,
        documents=documents
    )

    print(f"✔ Indexação completa: {len(ids)} documentos adicionados.")
    return True