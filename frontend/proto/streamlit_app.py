import streamlit as st
import requests
import pandas as pd

API_URL = "http://localhost:8000/query"

st.set_page_config(page_title="Inteligência Personalizada", layout="wide")

st.title("🧠 Inteligência Personalizada")
st.write("Digite sua pergunta e recebe a informa extraida e tratada diretamente do banco")

question = st.text_input("Pergunta ao banco de dados:", "")

if st.button("Gerar SQL e Consultar"):
    if not question.strip():
        st.warning("Digite uma pergunta primeiro!")
    else:
        with st.spinner("Consultando..."):
            try:
                response = requests.post(
                    API_URL,
                    json={"question": question},
                    timeout=500
                )
            except Exception as e:
                st.error(f"Erro ao contactar API: {e}")
                st.stop()

            if response.status_code != 200:
                st.error(f"Erro: {response.json().get('detail')}")
                st.stop()

            data = response.json()

            # -------------------------
            # NOVO: RESPOSTA NATURAL
            # -------------------------
            st.subheader("🗣️ Resposta interpretada:")
            st.success(data.get("answer", "Nenhuma interpretação disponível."))

            st.subheader("📄 SQL Gerado:")
            st.code(data["sql"], language="sql")

            st.subheader("📊 Resultado:")

            rows = data.get("rows", [])
            cols = data.get("columns", [])

            if not rows:
                st.info("Nenhum resultado encontrado.")
            else:
                df = pd.DataFrame(rows, columns=cols)
                st.dataframe(df, use_container_width=True)