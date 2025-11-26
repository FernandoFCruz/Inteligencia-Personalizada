# app/tests/test_chroma.py
from chromadb import PersistentClient
from app.core.config import settings

def main():
    print("Testando conexão com ChromaDB...")

    try:
        client = PersistentClient(path=settings.chroma_dir)
        print("Chroma conectado.")
    except Exception as e:
        print(f"Falha ao conectar ao Chroma: {e}")
        return

    try:
        col = client.get_collection("db_schema")
        print("Coleção encontrada: db_schema")
    except Exception as e:
        print(f"Coleção db_schema não encontrada: {e}")
        return

    try:
        items = col.peek()
        print(f"Primeiros documentos: {items}")
    except Exception as e:
        print(f"Erro ao listar documentos: {e}")
        return

    print("Teste do ChromaDB finalizado.")

if __name__ == "__main__":
    main()