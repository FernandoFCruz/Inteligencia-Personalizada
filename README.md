
# Inteligência Personalizada – Query Agent com IA (RAG + SQL + Pós-Processamento)

## 🧠 Objetivo do Projeto
O objetivo deste projeto é desenvolver um agente inteligente capaz de interpretar perguntas em linguagem natural, localizar automaticamente as tabelas corretas em um banco de dados empresarial (ERP), gerar consultas SQL seguras e validadas, executar no banco real e responder em linguagem natural usando um modelo LLM local (LLaMA via Olhama/Ollama).

## 📘 Escopo
- Extração automática do schema.
- Pipeline RAG completo com ChromaDB.
- SQL Generator robusto com validação.
- Pós-processamento com LLM.
- API FastAPI para interação.
- Execução segura no PostgreSQL.

## 🔧 Descrição Técnica
### 1. Data Pipeline
- Extração de schema real.
- Geração de descrições semânticas.
- Glossário automático (TF-IDF).
- Tags heurísticas.
- Indexação no ChromaDB.

### 2. Mapping Agent (RAG)
- Busca semântica.
- Classificação opcional por domínio.
- Suporte multi-tabelas.

### 3. SQL Generator
- Limpeza e validação rígida.
- Injeção de schema.
- Correção de tipos.
- Remoção de colunas inválidas.
- Suporte a múltiplas tabelas.

### 4. Pós-processamento
- Tabelas formatadas.
- Resumo em linguagem natural.

### 5. API FastAPI
Endpoints:
- `POST /query`
- `GET /`

## 📦 Instalação
### Requisitos
- Python 3.10+
- PostgreSQL
- Olhama/Ollama
- ChromaDB

### Setup
```bash
git clone <repo>
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Configuração
Criar `.env` com:
```
DATABASE_URL=postgresql://user:pass@host/db
SCHEMA=sisplan
LLAMA_SERVER=http://localhost:11434
CHROMA_DIR=./chroma_data
```

### Pipeline
```bash
python -m app.data_pipeline.run_full_pipeline
```

### LLAMA
```bash
ollama serve
```

### API
```bash
uvicorn main:app --reload
```

### INTERFACE
```bash
streamlit run .\frontend\proto\streamlit_app.py
```

## 🚀 Tecnologias
- Python / FastAPI  
- PostgreSQL  
- ChromaDB  
- SentenceTransformers  
- LLaMA 3.1  
- TF-IDF / Scikit-Learn  

## 🛡 Ética & LGPD
- LLM local.
- Dados não enviados externamente.
- Apenas informações do banco autorizado.

## 🏁 Status
- Pipeline ✔
- SQL Generator ✔
- Pós-processamento ✔
- API ✔