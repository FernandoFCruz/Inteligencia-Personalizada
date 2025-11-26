#!/usr/bin/env python3
import random
import json
from app.data_pipeline.metadata_extractor import extract_schema
from app.data_pipeline.classifier import TableClassifier   # precisa existir!


TEMPLATES = [
    "Mostre informações sobre a tabela {table}.",
    "Quais são as colunas da tabela {table}?",
    "O que existe na tabela {table}?",
    "Quero ver registros da tabela {table}.",
    "Consultar dados da tabela {table}.",
    "Informações relacionadas à tabela {table}.",
    "Listar registros da tabela {table}.",
    "Mostrar dados da tabela {table}.",
]


def synthesize_questions(table_name, table_text, n=20):
    """
    Gera perguntas sintéticas usando:
    - nome da tabela
    - partes da descrição textual gerada pelo metadata_extractor
    """

    qs = []
    text_tokens = table_text.split()

    for _ in range(n):
        template = random.choice(TEMPLATES)
        q = template.format(table=table_name)

        # 30% de chance de incluir parte do texto descritivo
        if table_text and random.random() < 0.30:
            extra = " ".join(text_tokens[:5])
            q = q + " " + extra

        qs.append(q)

    return qs


def build_dataset():
    docs = extract_schema()

    texts = []
    labels = []

    for d in docs:
        table_name = d["table"]              # nome lógico
        table_text = d["text"]               # texto descritivo (novo campo)
        table_id = d["id"]                   # label final ex: "schema.table"

        # gerar exemplos sintéticos
        synth = synthesize_questions(table_name, table_text, n=30)

        for q in synth:
            texts.append(q)
            labels.append(table_id)

    return texts, labels


def main():
    print("📦 Extraindo schema...")
    texts, labels = build_dataset()
    print(f"📊 Amostras total: {len(texts)}")

    clf = TableClassifier()   # precisa existir este módulo

    print("🤖 Treinando classificador...")
    clf.train(texts, labels)

    print(f"💾 Classificador salvo em {clf.model_path}")


if __name__ == "__main__":
    main()