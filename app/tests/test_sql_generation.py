# app/tests/test_sql_generation.py
from app.agents.query_agent.sql_generator import generate_sql

def test_sql(q):
    print(f"\nPergunta: {q}")
    try:
        sql = generate_sql(q)
        print("SQL gerado:", sql)
    except Exception as e:
        print("Erro:", e)

def main():
    print("=== Teste de SQL Generator ===")

    test_sql("listar clientes ativos")
    test_sql("quais fornecedores estão inativos")
    test_sql("listar produtos mais caros")
    test_sql("pedidos do cliente 123")
    test_sql("qual o estoque do produto X")

    print("\nTeste do SQL generator finalizado.")

if __name__ == "__main__":
    main()