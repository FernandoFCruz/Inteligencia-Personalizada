from sqlalchemy import create_engine, text
from sqlalchemy import inspect as sqlalchemy_inspect
from app.core.config import settings


def extract_schema():
    """
    Extrai informações reais do schema e gera documentos textuais
    adequados para indexação no ChromaDB.
    """

    engine = create_engine(settings.database_url)
    insp = sqlalchemy_inspect(engine)
    schema = settings.schema

    docs = []

    # Todas as tabelas
    tables = insp.get_table_names(schema=schema)

    for table in tables:
        # Colunas
        cols = insp.get_columns(table, schema=schema)
        pk = [c["name"] for c in cols if c.get("primary_key")]

        col_list = []

        for c in cols:
            col_name = c["name"]

            # Valida se tem daddos na coluna
            has_data = False
            try:
                with engine.connect() as conn:
                    result = conn.execute(
                        text(f'SELECT 1 FROM "{schema}"."{table}" WHERE "{col_name}" IS NOT NULL LIMIT 1')
                    ).scalar()

                    if result is not None:
                        has_data = True

            except Exception as e:
                print(f"[WARN] Erro ao verificar dados da coluna {schema}.{table}.{col_name}: {e}")
                has_data = False

            if has_data:
                col_list.append({
                    "name": col_name,
                    "type": str(c["type"]),
                    "nullable": c.get("nullable", True),
                    "has_data": True
                })
            else:
                print(f"[SKIP] Coluna ignorada por não possuir dados: {schema}.{table}.{col_name}")

        # Contagem de linhas
        row_count = None
        try:
            with engine.connect() as conn:
                row_count = conn.execute(
                    text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
                ).scalar()
        except Exception as e:
            print(f"Erro ao contar linhas da tabela {schema}.{table}: {e}")
            row_count = 0

        if row_count==0: continue
        if col_list == []: continue

        # Criar texto descritivo simples — suficiente para embedding
        description_text = generate_table_description(table, col_list, pk)

        doc = {
            "id": f"{schema}.{table}",
            "schema": schema,
            "table": table,
            "row_count": row_count,
            "columns": col_list,
            "pk": pk,
            "text": description_text  # texto base para indexação
        }

        docs.append(doc)

    return docs


def generate_table_description(table, columns, pk):
    """
    Gera o texto simples que será enviado ao Chroma.
    """

    col_lines = []
    for c in columns:
        null_text = "NULL" if c["nullable"] else "NOT NULL"
        col_lines.append(f"- {c['name']} ({c['type']}, {null_text})")

    pk_text = ", ".join(pk) if pk else "nenhuma chave primária"

    final_text = f"""
Tabela: {table}
Chave primária: {pk_text}
Colunas:
{chr(10).join(col_lines)}
"""

    return final_text.strip()