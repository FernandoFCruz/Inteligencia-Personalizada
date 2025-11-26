# app/data_pipeline/semantic_tagging.py
import numpy as np
from sentence_transformers import SentenceTransformer
from app.core.config import settings

# Caminho do modelo
EMBED_MODEL_PATH = settings.embed_model_path
embedder = SentenceTransformer(EMBED_MODEL_PATH)

# Conceitos principais
CONCEPTS = {

    # --- CLIENTES / ENTIDADES ---
    "cliente": [
        "cliente", "clientes", "cli", "codcli", "nome_cliente",
        "cpf", "cnpj", "telefone", "celular", "email",
        "endereco", "bairro", "cep", "cidade", "uf",
        "contato", "inscricao_estadual", "inscricao_municipal",
        "tipo_entidade", "situacao_cliente", "ativo",
    ],

    # --- FORNECEDORES ---
    "fornecedor": [
        "fornecedor", "fornecedores", "forn", "codfor",
        "razao_social", "nome_fantasia", "cpf", "cnpj", "ie", "im",
        "contato", "telefone", "email", "endereco",
        "prazo_pagamento", "tipo_entidade",
    ],

    # --- PRODUTO / ESTOQUE ---
    "produto": [
        "produto", "produtos", "item", "sku", "codprod",
        "descricao", "descricao_produto",
        "preco", "valor", "custo", "preco_venda", "preco_compra",
        "estoque", "qtd", "quantidade", "saldo_estoque",
        "ncm", "cest", "unidade", "categoria", "subcategoria",
        "lote", "validade", "fabricacao",
    ],

    # --- PEDIDOS / VENDAS ---
    "pedido": [
        "pedido", "pedidos", "num_pedido", "id_pedido",
        "itens_pedido", "qtd", "quantidade", 
        "cliente", "codcli",
        "valor_total", "subtotal", "desconto",
        "status_pedido", "data_pedido", "data_entrega",
        "vendedor", "representante", "comissao",
    ],

    # --- FINANCEIRO / FATURAS ---
    "financeiro": [
        "financeiro", "conta", "contas", "conta_pagar", "conta_receber",
        "pagar", "receber", "pago", "pendente", "vencido",
        "nota", "nf", "nfe", "nfse",
        "boleto", "pix", "fatura", "duplicata",
        "saldo", "saldo_atual", "valor", "valor_pago", "valor_aberto",
        "data_vencimento", "data_pagamento", "juros", "multa",
    ],

    # --- FATURAMENTO / NOTAS FISCAIS ---
    "fiscal": [
        "nota_fiscal", "nf", "nfe", "nfce", "modelo",
        "chave_acesso", "cfop", "cst", "ncm",
        "emissao", "data_emissao", "data_saida",
        "icms", "ipi", "pis", "cofins", "iss",
        "valor_total", "base_calculo", "aliquota",
    ],

    # --- ACESSO / USUÁRIOS / SEGURANÇA ---
    "acesso": [
        "acesso", "login", "logon", "senha",
        "usuario", "usuarios", "user", "perfil",
        "permissao", "permissoes",
        "log", "logs", "sessao", "sessão", "token",
        "ip", "host", "rede",
    ],

    # --- LOGÍSTICA / ESTOQUE AVANÇADO ---
    "logistica": [
        "estoque", "armazem", "almoxarifado",
        "endereco_estoque", "rua", "prateleira", "box",
        "movimentacao", "movimentações", 
        "entrada", "saida", "transferencia",
        "romaneio", "expedicao", "entrega", "transporte",
    ],

    # --- RH / PESSOAS ---
    "rh": [
        "funcionario", "funcionarios", "colaborador",
        "cargo", "setor", "departamento",
        "salario", "ferias", "beneficios",
        "ponto", "batida", "horas", "holerite",
    ],

    # --- CRM / ATENDIMENTO ---
    "crm": [
        "lead", "oportunidade", "pipeline",
        "atendimento", "ticket", "chamado",
        "status", "responsavel", "cliente",
        "origem", "motivo", "sla",
    ],

    # --- PROJETOS / OBRAS ---
    "projeto": [
        "projeto", "projetos", "obra", "obras",
        "orcamento", "custo", "centro_custo",
        "etapa", "fase", "atividade",
        "responsavel", "prazo", "cronograma",
    ],

    # --- COMPRAS ---
    "compras": [
        "compra", "compras", "pedido_compra", "solicitacao",
        "cotacao", "cotação", "fornecedor",
        "item_compra", "valor", "quantidade",
    ],

    # --- SERVIÇOS ---
    "servico": [
        "servico", "serviços", "prestador", "contrato",
        "os", "ordem_servico", "ordem_de_servico",
        "atividade", "horas", "mao_de_obra",
    ],
}

# -----------------------------------
# 🔧 Funções auxiliares
# -----------------------------------

def _normalize(v: np.ndarray):
    return v / (np.linalg.norm(v) + 1e-10)


def build_concept_embeddings(concepts: dict):
    vectors = {}
    for concept, keywords in concepts.items():
        text = " ".join(keywords)
        emb = embedder.encode([text], show_progress_bar=False)[0]
        vectors[concept] = _normalize(np.array(emb, dtype=np.float32))
    return vectors


# -----------------------------------
# 🔥 DESCRIÇÃO SEMÂNTICA AUTOMÁTICA
# -----------------------------------

def generate_smart_description(table: str, columns: list):
    col_names = [c["name"].lower() for c in columns]
    ctx = []

    if any(x in col_names for x in ["cpf", "cnpj", "cliente", "nome", "email"]):
        ctx.append("This table stores information related to clients or people.")
    if any(x in col_names for x in ["produto", "sku", "estoque", "valor"]):
        ctx.append("This table contains product or inventory related information.")
    if any(x in col_names for x in ["pedido", "item", "quantidade"]):
        ctx.append("This table stores sales orders or order items.")
    if any(x in col_names for x in ["valor", "saldo", "pagamento", "conta"]):
        ctx.append("This table contains financial or accounting data.")
    if any(x in col_names for x in ["login", "senha", "usuario", "acesso"]):
        ctx.append("This table relates to system access, authentication or security.")

    if not ctx:
        ctx.append("This table stores structured business data related to the system domain.")

    return (
        f"Semantic description for table '{table}': "
        f"{' '.join(ctx)} It contains the following fields: {', '.join(col_names)}."
    )


def make_table_document(d: dict) -> str:
    desc = generate_smart_description(d["table"], d["columns"])
    parts = [desc]

    examples = d.get("examples") or []
    if examples:
        ex_text = []
        for ex in examples[:3]:
            pairs = [f"{k}:{v}" for k, v in ex.items()]
            ex_text.append(", ".join(pairs))
        parts.append("Sample rows: " + " | ".join(ex_text))

    return "\n".join(parts)


# -----------------------------------
# 🔥 SCORE DE IDENTIDADE (principal)
# -----------------------------------

def score_table_identity_for_concept(doc, keywords):
    """
    Score determinístico baseado em identidade real da tabela.
    - Detecta quando o conceito aparece em colunas-chave
    - Evita que tabelas gigantes (ex: XML) recebam tags erradas
    """

    cols = [c["name"].lower() for c in doc.get("columns", [])]

    # 1) PK, FK, IDs, códigos
    identity_cols = [
        c for c in cols if (
            c.startswith("id") or
            c.startswith("cod") or
            c.endswith("_id") or
            "pk" in c
        )
    ]

    # secundárias
    secondary_cols = [c for c in cols if c not in identity_cols]

    # Hits fortes
    strong_hits = sum(1 for c in identity_cols if any(k in c for k in keywords))

    # Hits fracos
    weak_hits = sum(1 for c in secondary_cols if any(k in c for k in keywords))

    # -------------------------------
    # 1️⃣ Tabelas gigantes → ignorar hits fracos
    # -------------------------------
    if len(cols) > 120:
        if strong_hits > 0:
            return 1.0
        return 0.0

    # -------------------------------
    # 2️⃣ Hits fortes definem identidade
    # -------------------------------
    if strong_hits > 0:
        return min(1.0, 0.70 + strong_hits * 0.10)

    # -------------------------------
    # 3️⃣ Hits fracos contam pouco
    # -------------------------------
    if weak_hits > 0:
        return min(0.20, weak_hits * 0.02)

    return 0.0


# -----------------------------------
# 🔥 TAGGING FINAL
# -----------------------------------

def assign_semantic_tags(docs: list, threshold: float = 0.60):
    concept_embs = build_concept_embeddings(CONCEPTS)

    print("🔎 Gerando embeddings das tabelas...")
    texts = [make_table_document(d) for d in docs]

    emb_batch = embedder.encode(texts, show_progress_bar=True)
    emb_batch = np.array([_normalize(np.array(e, dtype=np.float32)) for e in emb_batch])

    results = []

    print("🏷 Atribuindo semantic tags...")

    for idx, d in enumerate(docs):
        table_emb = emb_batch[idx]
        score_map = {}
        tags = []

        for concept, cemb in concept_embs.items():

            # Similaridade base
            sim = float(np.dot(table_emb, cemb))

            # Score determinístico fortíssimo
            identity = score_table_identity_for_concept(d, CONCEPTS[concept])

            # Score final
            final_score = sim + identity
            score_map[concept] = round(final_score, 5)

            if final_score >= threshold:
                tags.append({"tag": concept, "score": final_score})

        # Ordenar
        tags = sorted(tags, key=lambda x: x["score"], reverse=True)

        d["tags"] = [t["tag"] for t in tags]
        d["_semantic_scores"] = score_map
        results.append(d)

    print("✅ Semantic tagging finalizado.")
    return results
