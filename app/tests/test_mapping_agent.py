# app/tests/test_mapping_agent.py
from app.agents.mapping_agent.retriever import map_tables

def test_query(q):
    print(f"\nPergunta: {q}")
    tables = map_tables(q)
    print("Resultado:", tables)

def main():
    print("=== Teste de Mapping Agent ===")

    test_query("listar clientes ativos")
    test_query("produtos com estoque baixo")
    test_query("quais pedidos foram cancelados")
    test_query("valores financeiros pagos hoje")

    print("\nTeste de mapping agent finalizado.")

if __name__ == "__main__":
    main()