# 🧠 TCC - LLM Local e Intranet Especializada nas Regras de Negócio

Este repositório contém o portfólio completo do Trabalho de Conclusão de Curso (TCC) com o tema:  
**"Desenvolvimento de uma LLM Local e Intranet Especializada nas Regras de Negócio de uma Empresa"**.

---

## 📌 Objetivo

Desenvolver e implantar uma **Large Language Model (LLM)** local, integrada a uma **intranet corporativa**, com foco em fornecer respostas contextualizadas baseadas nas regras, documentos e processos internos da empresa. O projeto visa aumentar a eficiência no acesso à informação, reduzir gargalos operacionais e fomentar a autonomia dos colaboradores.

---

## 🏗️ Estrutura do Repositório

📁 /docs
Documentação, artigos e relatórios do projeto.
📁 /src
Código-fonte da aplicação (backend, frontend, LLM).
📁 /models
Modelos treinados ou configurados para uso local.
📁 /datasets
Dados e documentos utilizados no fine-tuning da LLM.
📁 /intranet
Estrutura da intranet (interface, autenticação, buscas).
📄 README.md
Este arquivo.
📄 TCC_final.pdf
Versão final do trabalho acadêmico.


---

## ⚙️ Tecnologias Utilizadas

- 🧠 **LLM local**: [LLama.cpp](https://github.com/ggerganov/llama.cpp), [Ollama](https://ollama.ai/), [LangChain](https://www.langchain.com/)
- 🗃️ **Base de conhecimento**: RAG (Retrieval-Augmented Generation) com embeddings via [FAISS](https://github.com/facebookresearch/faiss)
- 🌐 **Intranet**: React + Node.js (ou Django, Flask, etc.)
- 📄 **Documentos**: Leitura e processamento de PDFs, DOCXs, e planilhas
- 🔐 **Segurança**: Autenticação local com controle de permissões
- 📦 **Containerização**: Docker

---

## 🧪 Funcionalidades Desenvolvidas

- [x] Integração de modelo LLM com documentos internos
- [x] Interface web para consulta e resposta
- [x] Upload e indexação de novos documentos
- [x] Busca semântica por conteúdo interno
- [x] Fine-tuning e adaptação às regras de negócio

---

## 🚀 Como Executar o Projeto

### 1. Clonar o repositório
```bash
git clone https://github.com/seuusuario/portfolio-tcc-llm.git
cd portfolio-tcc-llm