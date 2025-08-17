# pages/2_Catalogo.py
import streamlit as st
from lib.ui import inject_global_css
from lib.loaders import get_catalog, query

st.set_page_config(page_title="📒 Catálogo de Cargas", page_icon="📒", layout="wide")
inject_global_css()

st.title("📒 Catálogo de Cargas")

# Carregar catálogo
df = get_catalog()

if df.empty:
    st.info("Ainda não há registros no catálogo. Use o Wizard por Mês/Ano ou a Home para preparar dados.")
    st.stop()

# Filtros
col1, col2, col3 = st.columns([1,1,1.5])
with col1:
    datasets = sorted(df["dataset"].dropna().unique().tolist())
    f_dataset = st.multiselect("Dataset", datasets, default=[])
with col2:
    months = sorted(df["month_ref"].dropna().unique().tolist(), reverse=True)
    f_month = st.multiselect("Mês/Ano", months, default=[])
with col3:
    search = st.text_input("Buscar (no caminho/URL)", placeholder="trecho do caminho ou URL")

filtered = df.copy()
if f_dataset:
    filtered = filtered[filtered["dataset"].isin(f_dataset)]
if f_month:
    filtered = filtered[filtered["month_ref"].isin(f_month)]
if search:
    s = search.lower()
    filtered = filtered[filtered["parquet_path"].str.lower().str.contains(s) | filtered["source_url"].str.lower().str.contains(s)]

st.caption(f"{len(filtered)} registro(s) no catálogo.")
st.dataframe(filtered, use_container_width=True, height=420)

st.divider()
st.subheader("🔎 Pré-visualização rápida (LIMIT 50)")

colA, colB = st.columns([2,1])
with colA:
    sel_dataset = st.selectbox("Tabela para pré-visualizar", sorted(df["dataset"].dropna().unique().tolist()))
with colB:
    limit = st.number_input("LIMIT", min_value=10, max_value=5000, value=50, step=10)

if st.button("Exibir amostra da tabela", type="primary"):
    try:
        sample = query(f"SELECT * FROM {sel_dataset} LIMIT {int(limit)}")
        if sample.empty:
            st.warning("Tabela vazia ou não carregada ainda.")
        else:
            st.dataframe(sample, use_container_width=True)
    except Exception as e:
        st.error(f"Falha ao consultar a tabela '{sel_dataset}': {e}")

st.caption("Dica: a coluna **source_url** no catálogo mostra de onde o zip foi baixado.")