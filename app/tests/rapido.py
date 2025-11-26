from app.agents.mapping_agent.retriever import map_tables
from app.agents.query_agent.sql_generator import inject_schema
from app.agents.query_agent.sql_generator import remove_invalid_columns
from app.agents.query_agent.sql_generator import fix_type_mismatches
from app.agents.query_agent.sql_generator import validate_columns

question="liste os itens com maior valor"
sql="SELECT  FROM sisplan.co_iten_001;"
tables_context = map_tables(question)

# tables_context = [t for t in tables_context if isinstance(t, dict) and t.get("id")]

if not tables_context:
    raise Exception("Nenhuma tabela apropriada encontrada.")

# tabela escolhida (top-1)
table_ctx = tables_context[0]

sql = inject_schema(sql, tables_context)
sql = remove_invalid_columns(sql, tables_context)
sql = fix_type_mismatches(sql,tables_context)

# validação final
validate_columns(sql, tables_context)