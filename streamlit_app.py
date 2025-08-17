# app.py
# Base limpa: seleção do dataset, escolha da fonte (URL / ZIP / CSV), preparo e pré-visualização.
# Lembre: em .streamlit/config.toml, defina [server] maxUploadSize=500 para 500 MB.

from pathlib import Path
import io
import streamlit as st
from lib.ui import inject_global_css
from lib.loaders import (
    download_zip, extract_tabular_from_zip, read_csv_semicolon_to_parquet,
    ensure_table_from_parquet, query
)

st.set_page_config(page_title="CNPJ — Preparação de Dados", page_icon="🗂️", layout="wide")
inject_global_css()

UPLOAD_LIMIT_MB = 500  # exibição/checagem apenas

st.title("🗂️ CNPJ — Preparação e Consulta Básica")
st.caption("Carregue os conjuntos (Empresas, Estabelecimentos, Sócios, Simples e Domínios). ZIP interno sem extensão? Sem problemas.")

# Datasets (tabelas e pistas de nome p/ localizar dentro do ZIP)
DATASETS = {
    "empresas": {"table": "empresas", "keywords": ["empresas","empresa","empresas1","empresa1"], "hint": "Cadastro de empresas."},
    "estabelecimentos": {"table": "estabelecimentos", "keywords": ["estabelec","estabelecimentos"], "hint": "Unidades, CNAE, endereço."},
    "socios": {"table": "socios", "keywords": ["socios","sócios","socio","socio1"], "hint": "Sócios (PF/PJ/estrangeiro)."},
    "simples": {"table": "simples", "keywords": ["simples","mei"], "hint": "Opção Simples/MEI."},
    "paises": {"table": "paises", "keywords": ["paises","países","pais"], "hint": "Domínio Países."},
    "municipios": {"table": "municipios", "keywords": ["municipio","municípios","municipios"], "hint": "Domínio Municípios."},
    "qualificacoes": {"table": "qualificacoes", "keywords": ["qualificacao","qualificações","qualificacoes"], "hint": "Domínio Qualificações."},
    "naturezas": {"table": "naturezas", "keywords": ["natureza","naturezas"], "hint": "Domínio Naturezas Jurídicas."},
    "cnaes": {"table": "cnaes", "keywords": ["cnae","cnaes"], "hint": "Domínio CNAEs."},
}

# Estado
if "fonte" not in st.session_state: st.session_state.fonte = "URL ZIP (RFB)"

col_tipo, col_hint = st.columns([1.2, 1.8])
with col_tipo:
    tipo = st.selectbox("Tipo de arquivo (dataset)", list(DATASETS.keys()), index=1, format_func=lambda k: k.capitalize())
with col_hint:
    st.info(DATASETS[tipo]["hint"])

st.markdown("**Selecione a fonte do arquivo:**")
b1, b2, b3 = st.columns(3)
with b1:
    if st.button("🌐 URL ZIP (RFB)", use_container_width=True): st.session_state.fonte = "URL ZIP (RFB)"
with b2:
    if st.button("🗜️ Upload ZIP", use_container_width=True): st.session_state.fonte = "Upload ZIP"
with b3:
    if st.button("🧾 Upload CSV", use_container_width=True): st.session_state.fonte = "Upload CSV"
st.caption(f"Fonte atual: **{st.session_state.fonte}** • Limite de upload: **{UPLOAD_LIMIT_MB} MB**")

table_name = DATASETS[tipo]["table"]
keywords = DATASETS[tipo]["keywords"]

st.divider()
with st.expander("⚙️ Preparar/Carregar dados", expanded=True):
    fonte = st.session_state.fonte

    if fonte == "URL ZIP (RFB)":
        url = st.text_input("URL do ZIP da RFB", placeholder="Ex.: https://arquivos.receitafederal.gov.br/.../Empresas1.zip")
        with st.popover("🔎 Ver arquivos do ZIP (debug)"):
            if url:
                try:
                    import requests, zipfile
                    r = requests.get(url, timeout=60); r.raise_for_status()
                    bio = io.BytesIO(r.content)
                    with zipfile.ZipFile(bio, "r") as z: st.write([m.filename for m in z.infolist()])
                except Exception as e:
                    st.warning(f"Não foi possível listar: {e}")
        if st.button("Baixar e preparar", type="primary", use_container_width=True, disabled=not url):
            try:
                zip_path = Path("data") / f"{table_name}.zip"
                download_zip(url, zip_path)
                fobj = extract_tabular_from_zip(zip_path, prefer_keywords=keywords)
                parquet = read_csv_semicolon_to_parquet(fobj, table_name)
                ensure_table_from_parquet(table_name, parquet, replace=True)
                st.success(f"Tabela **{table_name}** pronta: `{parquet}`")
            except Exception as e:
                st.error(f"Falha ao preparar: {e}")

    elif fonte == "Upload ZIP":
        st.caption(f"Aceita até **{UPLOAD_LIMIT_MB} MB** (config.toml).")
        up_zip = st.file_uploader("Selecione um arquivo ZIP", type=["zip"])
        if up_zip is not None:
            size_attr = getattr(up_zip, "size", None)
            size_mb = (size_attr / (1024 * 1024)) if isinstance(size_attr, (int, float)) else None
            if size_mb is not None and size_mb > UPLOAD_LIMIT_MB:
                st.error(f"O arquivo tem {size_mb:.1f} MB (> {UPLOAD_LIMIT_MB} MB).")
            else:
                if st.button("Preparar do ZIP", type="primary", use_container_width=True):
                    try:
                        tmp_zip = Path("data") / f"tmp_upload_{table_name}.zip"
                        tmp_zip.write_bytes(up_zip.read())
                        try:
                            fobj = extract_tabular_from_zip(tmp_zip, prefer_keywords=keywords)
                            parquet = read_csv_semicolon_to_parquet(fobj, table_name)
                            ensure_table_from_parquet(table_name, parquet, replace=True)
                            st.success(f"Tabela **{table_name}** pronta (upload ZIP): `{parquet}`")
                        finally:
                            tmp_zip.unlink(missing_ok=True)
                    except Exception as e:
                        st.error(f"Falha ao preparar: {e}")
        else:
            st.info("Envie um arquivo ZIP para prosseguir.")

    elif fonte == "Upload CSV":
        st.caption(f"Aceita até **{UPLOAD_LIMIT_MB} MB** (config.toml).")
        up_csv = st.file_uploader("Selecione um arquivo CSV", type=["csv"])
        if up_csv is not None:
            size_attr = getattr(up_csv, "size", None)
            size_mb = (size_attr / (1024 * 1024)) if isinstance(size_attr, (int, float)) else None
            if size_mb is not None and size_mb > UPLOAD_LIMIT_MB:
                st.error(f"O arquivo tem {size_mb:.1f} MB (> {UPLOAD_LIMIT_MB} MB).")
            else:
                if st.button("Preparar do CSV", type="primary", use_container_width=True):
                    try:
                        parquet = read_csv_semicolon_to_parquet(io.BytesIO(up_csv.read()), table_name)
                        ensure_table_from_parquet(table_name, parquet, replace=True)
                        st.success(f"Tabela **{table_name}** pronta (upload CSV): `{parquet}`")
                    except Exception as e:
                        st.error(f"Falha ao preparar: {e}")
        else:
            st.info("Envie um arquivo CSV para prosseguir.")
    else:
        st.info("Se você já tem `.parquet` em /data, as consultas usarão essas tabelas.")

st.divider()
st.subheader("🔎 Pré-visualização (LIMIT 50)")
if st.button("Exibir amostra da tabela", use_container_width=True):
    try:
        df = query(f"SELECT * FROM {table_name} LIMIT 50")
        if df.empty: st.warning("Tabela vazia ou sem linhas para exibir.")
        else: st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error("Não foi possível consultar a tabela. Confirme se já foi preparada.")
        st.exception(e)