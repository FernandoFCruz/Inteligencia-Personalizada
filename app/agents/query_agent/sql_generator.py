# app/agents/query_agent/sql_generator_final.py
import re
from typing import List, Dict, Any, Union

from app.agents.mapping_agent.retriever import map_tables
from app.agents.llm.llama_api import call_llama_generate


# =====================================================
# 1. FORMAT CONTEXT → usado no prompt do LLM
# =====================================================
def format_context(tables_context: List[Dict[str, Any]]) -> str:
    if not tables_context:
        return "Nenhuma tabela mapeada."

    lines = []
    for item in tables_context:
        table_id = item.get("id", "?")
        score = round(item.get("score", 0), 4)
        cols = item.get("columns", [])
        col_names = []

        for c in cols:
            if isinstance(c, dict):
                col_names.append(c.get("name"))
            elif isinstance(c, str):
                col_names.append(c)

        lines.append(f"Tabela {table_id} (relevância {score})\n  Colunas: {', '.join(col_names)}")

    return "\n\n".join(lines)


# =====================================================
# 2. LIMPEZA DO SQL RETORNADO PELO LLM
# =====================================================
def clean_llm_output(raw: str) -> str:
    cleaned = (raw or "").strip()
    cleaned = re.sub(r"```.*?```", "", cleaned, flags=re.DOTALL)
    cleaned = cleaned.replace("`", "")

    if ";" in cleaned:
        cleaned = cleaned.split(";")[0] + ";"

    match = re.search(r"(SELECT|WITH)[\s\S]*?;", cleaned, flags=re.IGNORECASE)
    if match:
        return match.group(0).strip()

    return cleaned.strip()


# =====================================================
# 3. INJEÇÃO SEGURA DO SCHEMA (se faltar)
# =====================================================
def inject_schema(sql: str, tables_context: List[Dict[str, Any]]):
    for item in tables_context:
        tid = item.get("id")

        if "." not in tid:
            continue

        schema, table = tid.split(".", 1)

        # já tem schema.table? pula
        if f"{schema}.{table}" in sql.lower():
            continue

        pattern = rf"(?<!['\"])\b{re.escape(table)}\b(?!['\"])"
        replacement = f"{schema}.{table}"
        sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)

    return sql


# =====================================================
# 4. MAPA DE TIPOS DAS COLUNAS
# =====================================================
def get_column_map(columns: List[Dict[str, Any]]) -> Dict[str, str]:
    out = {}
    for c in columns or []:
        if isinstance(c, dict):
            name = (c.get("name") or "").lower()
            typ = (c.get("type") or "text").lower()
            out[name] = typ
        elif isinstance(c, str):
            out[c.lower()] = "text"
    return out


# =====================================================
# 5. CORRIGIR TIPOS AUTOMATICAMENTE
# =====================================================
def fix_type_mismatches(sql: str, tables_context: list[dict]) -> str:
    """
    Corrige comparações de tipos no WHERE considerando TODAS as tabelas envolvidas,
    incluindo aliases (ex: p.ativo, cli.status, etc).
    """

    if " where " not in sql.lower():
        return sql

    # ------------------------------
    # Mapear todas as colunas por tabela
    # ------------------------------
    table_types = {}   # { schema.table : {col: tipo} }

    for t in tables_context:
        tid = t.get("id", "").lower()
        cols = t.get("columns", [])
        cmap = {}

        for c in cols:
            if isinstance(c, dict):
                name = c.get("name", "").lower()
                typ = (c.get("type") or "").lower()
                cmap[name] = typ
            elif isinstance(c, str):
                cmap[c.lower()] = "text"

        if cmap:
            table_types[tid] = cmap

    # ------------------------------
    # Detectar aliases no SQL
    # ------------------------------
    alias_map = {}  # alias → tabela completa

    join_matches = re.findall(
        r"from\s+([a-zA-Z0-9_\.]+)\s+([a-zA-Z0-9_]+)|"
        r"join\s+([a-zA-Z0-9_\.]+)\s+([a-zA-Z0-9_]+)",
        sql, flags=re.IGNORECASE
    )

    for m in join_matches:
        if m[0] and m[1]:
            alias_map[m[1].lower()] = m[0].lower()
        if m[2] and m[3]:
            alias_map[m[3].lower()] = m[2].lower()

    # ------------------------------
    # Unificar todas as colunas para resolução sem alias
    # ------------------------------
    all_columns = {}
    for cmap in table_types.values():
        all_columns.update(cmap)

    # ------------------------------
    # Quebrar WHERE
    # ------------------------------
    before, where = re.split(r"\bwhere\b", sql, flags=re.IGNORECASE, maxsplit=1)
    where = where.strip()

    # ------------------------------
    # Padrão para capturar col = valor
    # ------------------------------
    matches = re.findall(
        r"([a-zA-Z_][a-zA-Z0-9_\.]*)\s*(=|!=)\s*('?[^']*'?|\d+)",
        where
    )

    domain_map = {
        "ativo": {"1": "'S'", "0": "'N'", "true": "'S'", "false": "'N'"},
    }

    def get_column_type(identifier: str) -> str | None:
        """
        Retorna o tipo da coluna levando em conta alias ou tabela verdadeira.
        """
        ident = identifier.lower()

        # caso seja alias.col
        if "." in ident:
            alias, col = ident.split(".", 1)
            if alias in alias_map:
                full_table = alias_map[alias]
                return table_types.get(full_table, {}).get(col)

        return all_columns.get(ident)

    for col_raw, op, raw_val in matches:
        col = col_raw.strip()
        col_l = col.lower()

        val = raw_val.strip()
        val_unq = val.strip("'\"")

        # descobrir tipo da coluna
        col_type = get_column_type(col)
        if not col_type:
            continue

        # NORMALIZAR SUBSTRINGS
        pattern = re.escape(f"{col} {op} {raw_val}")

        if any(t in col_type for t in ("int", "numeric", "decimal", "bigint", "float")):

            if not val_unq.isdigit():
                where = re.sub(pattern, "1=1", where)
            else:
                where = re.sub(pattern, f"{col} {op} {val_unq}", where)

        elif any(t in col_type for t in ("char", "varchar", "text")):

            if col_l in domain_map and val_unq in domain_map[col_l]:
                new_val = domain_map[col_l][val_unq]
                where = re.sub(pattern, f"{col} {op} {new_val}", where)
                continue

            if not (val.startswith("'") and val.endswith("'")):
                new_val = f"'{val_unq}'"
                where = re.sub(pattern, f"{col} {op} {new_val}", where)

    # reconstruir SQL
    return before + " WHERE " + where


# =====================================================
# 6. REMOVER CONDIÇÕES INVÁLIDAS (colunas inexistentes)
# =====================================================
def remove_invalid_columns(sql: str, tables_context: list[dict]) -> str:

    if " where " not in sql.lower():
        return sql

    # ------------------------------
    # Coletar colunas por tabela
    # ------------------------------
    table_cols = {}   # { schema.table : set(colunas) }

    for t in tables_context:
        tid = t.get("id", "").lower()
        cols = t.get("columns", [])
        valid = set()

        for c in cols:
            if isinstance(c, dict):
                valid.add(c.get("name", "").lower())
            elif isinstance(c, str):
                valid.add(c.lower())

        if tid:
            table_cols[tid] = valid

    # ------------------------------
    # Identificar aliases no SQL
    # ------------------------------
    alias_map = {}  # alias → tabela

    join_matches = re.findall(
        r"from\s+([a-zA-Z0-9_\.]+)\s+([a-zA-Z0-9_]+)|"
        r"join\s+([a-zA-Z0-9_\.]+)\s+([a-zA-Z0-9_]+)",
        sql,
        flags=re.IGNORECASE
    )

    for m in join_matches:
        # m = (table1, alias1, table2, alias2)
        if m[0] and m[1]:
            alias_map[m[1].lower()] = m[0].lower()
        if m[2] and m[3]:
            alias_map[m[3].lower()] = m[2].lower()

    # ------------------------------
    # Separar head e WHERE
    # ------------------------------
    head, where = re.split(r"\bwhere\b", sql, flags=re.IGNORECASE, maxsplit=1)
    parts = re.split(r"(\s+AND\s+|\s+OR\s+)", where, flags=re.IGNORECASE)

    cleaned = []
    SQL_KW = {"and","or","in","like","between","is","null"}

    for token in parts:
        stripped = token.strip()

        # manter AND/OR
        if stripped.lower() in ("and", "or"):
            cleaned.append(token)
            continue

        # extrair identificador principal da condição
        m = re.match(r"([a-zA-Z_][a-zA-Z0-9_\.]*)", stripped)
        if not m:
            continue

        ident = m.group(1).lower()

        # caso seja alias.coluna
        if "." in ident:
            alias, col = ident.split(".", 1)

            # alias não registrado? descarta condição
            if alias not in alias_map:
                print(f"[FILTER] Alias desconhecido → {token.strip()}")
                continue

            table_id = alias_map[alias]
            valid_cols = table_cols.get(table_id, set())

            if col not in valid_cols:
                print(f"[FILTER] Coluna inválida ({alias}.{col}) → {token.strip()}")
                continue

            # coluna válida
            cleaned.append(token)
            continue

        # caso seja coluna simples (sem alias)
        # verificar em todas as tabelas possíveis
        col_is_valid = any(
            ident in cols for cols in table_cols.values()
        )

        if col_is_valid:
            cleaned.append(token)
        else:
            print(f"[FILTER] Coluna inválida → {token.strip()}")

    # reconstruir WHERE
    result = "".join(cleaned).strip()
    result = re.sub(r"^(AND|OR)\s+", "", result, flags=re.IGNORECASE)
    result = re.sub(r"\s+(AND|OR)$", "", result, flags=re.IGNORECASE)

    if not result:
        return head.strip()

    return head + " WHERE " + result

# =====================================================
# 7. VALIDAÇÃO FINAL
# =====================================================
def validate_columns(sql: str, tables_context: Union[Dict[str, Any], List[Dict[str, Any]]]):
    """
    Valida colunas referenciadas no SQL, considerando um ou mais table_contexts.

    tables_context pode ser:
      - um dict (um único contexto), ou
      - uma lista de dicts (várias tabelas/aliases).

    Levanta Exception("Coluna inválida detectada: X") se encontrar token que
    pareça ser uma coluna mas não exista em nenhuma tabela do contexto.
    """

    # Normaliza tables_context para lista
    if tables_context is None:
        tables_context = []
    if isinstance(tables_context, dict):
        tables_context = [tables_context]

    # 1) construir conjuntos de colunas, tabelas e schemas válidos
    valid_cols = set()
    table_names = set()
    schema_names = set()
    full_tables = set()

    for t in tables_context:
        if not isinstance(t, dict):
            continue
        schema = (t.get("schema") or "").lower()
        table = (t.get("table") or "").lower()
        tid = (t.get("id") or "").lower()  # geralmente "schema.table"
        if schema:
            schema_names.add(schema)
        if table:
            table_names.add(table)
        if tid:
            full_tables.add(tid)

        cols = t.get("columns", []) or []
        for c in cols:
            if isinstance(c, dict):
                name = (c.get("name") or "").lower()
                if name:
                    valid_cols.add(name)
            elif isinstance(c, str):
                valid_cols.add(c.lower())

    # 2) detectar aliases no SQL (FROM / JOIN), suportando "AS"
    alias_map = {}  # alias -> full_table_or_table_string
    alias_regex = re.findall(
        r"(?:from|join)\s+([a-zA-Z0-9_.]+)(?:\s+(?:as\s+)?([a-zA-Z0-9_]+))?",
        sql,
        flags=re.IGNORECASE
    )
    for table_token, alias in alias_regex:
        table_token_l = table_token.lower()
        if alias:
            alias_map[alias.lower()] = table_token_l
        else:
            pass

    aliases = set(alias_map.keys())

    # 3) tokens do SQL
    tokens = re.findall(r"[a-zA-Z_][a-zA-Z0-9_]*", sql)

    SQL_KW = {
        "select","from","where","and","or","limit","offset","order","by","group",
        "having","join","left","right","inner","outer","full","cross","natural",
        "using","on","distinct","asc","desc","insert","update","delete","merge",
        "create","alter","drop","truncate","comment","rename","returning",
        "union","all","intersect","except","in","not","between","like","ilike",
        "similar","exists","is","null","true","false","case","when","then","else",
        "end","count","sum","avg","min","max","coalesce","nullif","greatest",
        "least","fetch","partition","over","filter","as","into"
    }

    # helper: se token faz parte de algum 'schema.table' presente no SQL
    def is_part_of_any_schema_table(token: str) -> bool:
        tl = token.lower()
        sql_l = sql.lower()
        for ft in full_tables:
            if ft and ft in sql_l:
                schema_part, _, table_part = ft.partition(".")
                if tl == schema_part or tl == table_part:
                    return True
        return False

    # 4) validar token por token
    for t in tokens:
        tl = t.lower()

        # ignorar keywords
        if tl in SQL_KW:
            continue

        # ignorar schemas/tables/aliases exatos
        if tl in schema_names or tl in table_names or tl in aliases:
            continue

        # ignorar se parte de schema.table que aparece no SQL
        if is_part_of_any_schema_table(t):
            continue

        # ignorar números/textos de 1 caractere
        if tl.isdigit():
            continue
        if len(tl) == 1:
            continue

        # se token estiver qualificado com ponto (alias.col ou schema.table) -> ignorar parte tabela/schema
        # (o regex já quebra por token, então "alias.col" vira dois tokens; aqui cuidamos de casos onde token é coluna qualificada)
        # já coberto pelas verificações acima via aliases e is_part_of_any_schema_table

        # finalmente: token deve ser uma coluna válida em AO MENOS UMA tabela do contexto
        if tl not in valid_cols:
            # mensagem mais informativa: mostrar token e um resumo das colunas válidas (limitado)
            sample = ", ".join(sorted(list(valid_cols))[:30])
            raise Exception(f"Coluna inválida detectada: {t}.")

    # se passou por todos sem exceção -> OK (retorna None)
    return None

# =====================================================
# 8. GERAÇÃO FINAL DO SQL
# =====================================================
def generate_sql(question: str) -> str:
    # ----------------------
    # tabela mais provável
    # ----------------------
    tables_context = map_tables(question)
    tables_context = tables_context[:3]

    if not tables_context:
        raise Exception("Nenhuma tabela ou documento encontrado")

    ctx = tables_context[0]

    if ctx["type"] == "doc":
        return None  # <<< muito importante


    prompt = f"""
Você é um gerador de SQL seguro.
NÃO invente tabelas ou colunas.

Use APENAS as tabelas listadas como id
e APENAS as colubas listadas como name

{format_context(tables_context)}

Retorne SOMENTE um SQL válido (terminado em ";").
Sem explicações.

Pergunta:
{question}
"""

    raw = call_llama_generate(prompt)
    sql = clean_llm_output(raw)

    print(prompt)
    print(raw)

    # ----------------------
    # pipeline de correções
    # ----------------------
    sql = inject_schema(sql, tables_context)
    sql = remove_invalid_columns(sql, tables_context)
    sql = fix_type_mismatches(sql, tables_context)

    # validação final
    #  validate_columns(sql, tables_context)

    sql = re.sub(r"\s+", " ", sql).strip()
    if not sql.endswith(";"):
        sql += ";"

    return sql