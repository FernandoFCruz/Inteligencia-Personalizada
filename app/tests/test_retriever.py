# app/tests/test_retriever.py
from app.agents.mapping_agent.retriever import vector_search

def main():
    print("=== Testando Retriever ===")

    queries = [
        "cliente",
        "fornecedor",
        "estoque",
        "financeiro",
        "email"
    ]

    for q in queries:
        print(f"\nQuery: {q}")
        results = vector_search(q, top_k=3)
        for r in results:
            print(f"- {r.get('id')}, score={r.get('score')}")
            print("  cols=", len(r.get("columns", [])))
            print("  tags=", r.get("tags"))

    print("\nTeste do retriever finalizado.")

if __name__ == "__main__":
    main()