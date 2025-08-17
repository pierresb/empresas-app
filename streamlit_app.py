# app.py
# CNPJ — Preparação e Consulta Básica (otimizado para Streamlit Cloud)
# Observação: no Streamlit Cloud o upload do navegador costuma ser limitado (~200 MB).
# Use preferencialmente a opção "URL ZIP (RFB)" para contornar o limite.

from pathlib import Path
import io
import os
import streamlit as st

from lib.ui import inject_global_css
from lib.loaders import (
    download_zip,
    extract_tabular_from_zip,
    read_csv_semicolon_to_parquet,
    ensure_table_from_parquet,
    query,
)

st.set_page_config(page_title="CNPJ — Preparação de Dados", page_icon="🗂️", layout="wide")
inject_global_css()

# Limite efetivo lido do Streamlit (no Cloud, normalmente ~200)
EFFECTIVE_LIMIT_MB = st.get_option("server.maxUploadSize") or 200
UPLOAD_ENABLED = EFFECTIVE_LIMIT_MB >= 300  # se <300, consideramos upload "não confiável" no Cloud

st.title("🗂️ CNPJ — Preparação e Consulta (Cloud-friendly)")
st.caption(
    "No Streamlit Cloud, uploads grandes (≳200 MB) falham por limite do provedor. "
    "Use **URL ZIP (RFB)** para carregar arquivos grandes com segurança."
)

# -------------------- Datasets (tabelas e keywords) --------------------
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

# -------------------- Estado/UI: tipo + fonte --------------------
if "fonte" not in st.session_state:
    st.session_state.fonte = "URL ZIP (RFB)"

col_tipo, col_hint = st.columns([1.2, 1.8])
with col_tipo:
    tipo = st.selectbox("Tipo de arquivo (dataset)", list(DATASETS.keys()), index=1, format_func=lambda k: k.capitalize())
with col_hint:
    st.info(DATASETS[tipo]["hint"])

st.markdown("**Selecione a fonte do arquivo:**")
b1, b2, b3 = st.columns(3)
with b1:
    if st.button("🌐 URL ZIP (RFB)", use_container_width=True):
        st.session_state.fonte = "URL ZIP (RFB)"
with b2:
    # no Cloud, uploads grandes falham; mantemos o botão, mas desativamos a seção abaixo
    if st.button("🗜️ Upload ZIP", use_container_width=True):
        st.session_state.fonte = "Upload ZIP"
with b3:
    if st.button("🧾 Upload CSV", use_container_width=True):
        st.session_state.fonte = "Upload CSV"

st.caption(
    f"Fonte atual: **{st.session_state.fonte}** • Limite efetivo de upload do ambiente: **{int(EFFECTIVE_LIMIT_MB)} MB**"
)

table_name = DATASETS[tipo]["table"]
keywords = DATASETS[tipo]["keywords"]

st.divider()
with st.expander("⚙️ Preparar/Carregar dados", expanded=True):
    fonte = st.session_state.fonte

    # ========== Fluxo recomendado no Cloud ==========
    if fonte == "URL ZIP (RFB)":
        url = st.text_input(
            "URL do ZIP (RFB ou outro link direto para o arquivo)",
            placeholder="Ex.: https://arquivos.receitafederal.gov.br/.../Empresas1.zip",
        )

        # (Opcional) HEAD para estimar tamanho antes de baixar
        if url:
            with st.popover("🔎 Verificar tamanho e conteúdo (opcional)"):
                try:
                    import requests, zipfile
                    head = requests.head(url, timeout=30, allow_redirects=True)
                    size = int(head.headers.get("content-length") or 0)
                    if size:
                        st.write(f"Tamanho reportado: **{size/1024/1024:.1f} MB**")
                    # lista arquivos do zip rapidamente
                    r = requests.get(url, timeout=60)
                    r.raise_for_status()
                    bio = io.BytesIO(r.content)
                    with zipfile.ZipFile(bio, "r") as z:
                        st.write([m.filename for m in z.infolist()][:20])
                except Exception as e:
                    st.warning(f"Não foi possível inspecionar: {e}")

        if st.button("Baixar e preparar", type="primary", use_container_width=True, disabled=not url):
            try:
                zip_path = Path("data") / f"{table_name}.zip"
                # download com barra de progresso já é feito dentro de download_zip()
                download_zip(url, zip_path)
                fobj = extract_tabular_from_zip(zip_path, prefer_keywords=keywords)
                parquet = read_csv_semicolon_to_parquet(fobj, table_name)
                ensure_table_from_parquet(table_name, parquet, replace=True)
                st.success(f"Tabela **{table_name}** preparada a partir de: `{parquet}`")
            except Exception as e:
                st.error(f"Falha ao preparar: {e}")

    # ========== Uploads (desencorajados no Cloud para arquivos grandes) ==========
    elif fonte == "Upload ZIP":
        if not UPLOAD_ENABLED:
            st.warning(
                "Este ambiente não suporta uploads grandes (≳200 MB). "
                "Prefira a opção **URL ZIP (RFB)** acima."
            )
        up_zip = st.file_uploader("Selecione um arquivo ZIP", type=["zip"], disabled=not UPLOAD_ENABLED)
        if up_zip is not None and UPLOAD_ENABLED:
            size_attr = getattr(up_zip, "size", None)
            size_mb = (size_attr / (1024 * 1024)) if isinstance(size_attr, (int, float)) else None
            if size_mb is not None and size_mb > EFFECTIVE_LIMIT_MB:
                st.error(f"O arquivo tem {size_mb:.1f} MB e excede o limite efetivo de {int(EFFECTIVE_LIMIT_MB)} MB.")
            else:
                if st.button("Preparar do ZIP", type="primary", use_container_width=True):
                    try:
                        # grava em disco em streaming (menos RAM)
                        tmp_zip = Path("data") / f"tmp_upload_{table_name}.zip"
                        with open(tmp_zip, "wb") as f:
                            for chunk in iter(lambda: up_zip.read(1024 * 1024), b""):
                                if not chunk:
                                    break
                                f.write(chunk)
                        try:
                            fobj = extract_tabular_from_zip(tmp_zip, prefer_keywords=keywords)
                            parquet = read_csv_semicolon_to_parquet(fobj, table_name)
                            ensure_table_from_parquet(table_name, parquet, replace=True)
                            st.success(f"Tabela **{table_name}** preparada (upload ZIP): `{parquet}`")
                        finally:
                            tmp_zip.unlink(missing_ok=True)
                    except Exception as e:
                        st.error(f"Falha ao preparar: {e}")
        elif not UPLOAD_ENABLED:
            st.info("Envie um link na opção **URL ZIP** acima para processar o arquivo grande.")

    elif fonte == "Upload CSV":
        if not UPLOAD_ENABLED:
            st.warning(
                "Este ambiente não suporta uploads grandes (≳200 MB). "
                "Prefira a opção **URL ZIP (RFB)** acima."
            )
        up_csv = st.file_uploader("Selecione um arquivo CSV", type=["csv"], disabled=not UPLOAD_ENABLED)
        if up_csv is not None and UPLOAD_ENABLED:
            size_attr = getattr(up_csv, "size", None)
            size_mb = (size_attr / (1024 * 1024)) if isinstance(size_attr, (int, float)) else None
            if size_mb is not None and size_mb > EFFECTIVE_LIMIT_MB:
                st.error(f"O arquivo tem {size_mb:.1f} MB e excede o limite efetivo de {int(EFFECTIVE_LIMIT_MB)} MB.")
            else:
                if st.button("Preparar do CSV", type="primary", use_container_width=True):
                    try:
                        parquet = read_csv_semicolon_to_parquet(io.BytesIO(up_csv.read()), table_name)
                        ensure_table_from_parquet(table_name, parquet, replace=True)
                        st.success(f"Tabela **{table_name}** preparada (upload CSV): `{parquet}`")
                    except Exception as e:
                        st.error(f"Falha ao preparar: {e}")
        elif not UPLOAD_ENABLED:
            st.info("Envie um link na opção **URL ZIP** acima para processar o arquivo grande.")

    else:
        st.info("Se você já tem `.parquet` em /data, as consultas usarão essas tabelas.")

st.divider()
st.subheader("🔎 Pré-visualização (LIMIT 50)")
if st.button("Exibir amostra da tabela", use_container_width=True):
    try:
        df = query(f"SELECT * FROM {table_name} LIMIT 50")
        if df.empty:
            st.warning("Tabela vazia ou sem linhas para exibir.")
        else:
            st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error("Não foi possível consultar a tabela. Confirme se já foi preparada.")
        st.exception(e)

# Rodapé (debug do limite)
st.sidebar.caption(f"server.maxUploadSize (efetivo): {int(EFFECTIVE_LIMIT_MB)} MB")