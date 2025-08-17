# pages/0_Wizard_Mes_Ano.py
# Wizard: baixa todos os pacotes de um mês/ano direto da RFB e prepara as tabelas.
# Funciona no Streamlit Cloud sem depender de upload do usuário.

import streamlit as st
from lib.ui import inject_global_css
from lib.loaders import prepare_all_for_month, month_dir_url

st.set_page_config(page_title="Wizard por Mês/Ano", page_icon="🪄", layout="wide")
inject_global_css()

st.title("🪄 Wizard — Baixar todos os pacotes por Mês/Ano (RFB)")

colA, colB = st.columns(2)
year = colA.number_input("Ano", min_value=2018, max_value=2100, value=2025, step=1)
month = colB.number_input("Mês", min_value=1, max_value=12, value=6, step=1)

st.caption(f"Diretório alvo: **{month_dir_url(int(year), int(month))}**")

targets = st.multiselect(
    "Quais conjuntos baixar?",
    ["empresas", "estabelecimentos", "socios", "simples", "paises", "municipios", "qualificacoes", "naturezas", "cnaes"],
    default=["empresas", "estabelecimentos", "socios", "simples"],
    help="Você pode incluir as tabelas de domínio também; elas são menores."
)

if st.button("▶️ Baixar e preparar", type="primary"):
    with st.spinner("Baixando e preparando... isso pode levar alguns minutos conforme a sua seleção."):
        logs = prepare_all_for_month(int(year), int(month), targets or None)
    if not logs:
        st.warning("Nenhum arquivo foi preparado. Verifique mês/ano e a seleção.")
    else:
        st.success("Concluído!")
        for item in logs:
            status, dataset, url, msg = item  # status: ok | warn | error
            if status == "ok":
                st.write(f"✅ **{dataset}** — {msg}")
            elif status == "warn":
                st.warning(f"⚠️ **{dataset}** — {msg}")
            else:
                st.error(f"❌ **{dataset}** — {msg}")

st.info("Dica: após o wizard, use a página **Home** para pré-visualizar as tabelas (LIMIT 50).")