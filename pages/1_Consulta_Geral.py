# pages/1_Consulta_Geral.py
import re
import streamlit as st
from lib.ui import inject_global_css
from lib.loaders import query

st.set_page_config(page_title="Consulta Geral", page_icon="🔎", layout="wide")
inject_global_css()

st.title("🔎 Consulta Geral (CNPJ / Nome / UF / Município / CNAE)")

cnpj = st.text_input("CNPJ (parcial/total, com ou sem máscara)")
nome = st.text_input("Nome Fantasia ou Razão Social (contém)")
uf = st.text_input("UF", max_chars=2)
municipio = st.text_input("Município (código ou trecho do nome)")
cnae = st.text_input("CNAE Principal (ex.: 6201501)")

sql = """
WITH est AS (
  SELECT
    CONCAT(LPAD("CNPJ BÁSICO",8,'0'), LPAD("CNPJ ORDEM",4,'0'), LPAD("CNPJ DV",2,'0')) AS cnpj14,
    "NOME FANTASIA" AS nome_fantasia,
    "UF" AS uf,
    "MUNICÍPIO" AS municipio,
    "CNAE FISCAL PRINCIPAL" AS cnae_principal,
    "CNAE FISCAL SECUNDÁRIA" AS cnae_sec
  FROM estabelecimentos
),
emp AS (
  SELECT "CNPJ BÁSICO" AS cnpj_basico, "RAZÃO SOCIAL / NOME EMPRESARIAL" AS razao FROM empresas
)
SELECT e.cnpj14, emp.razao, e.nome_fantasia, e.uf, e.municipio, e.cnae_principal, e.cnae_sec
FROM est e
LEFT JOIN emp ON emp.cnpj_basico = SUBSTR(e.cnpj14,1,8)
WHERE 1=1
"""
params = []

if cnpj:
    digits = re.sub(r"\D+","", cnpj)
    sql += " AND e.cnpj14 LIKE ?"; params.append(f"%{digits}%")
if nome:
    v = f"%{nome.lower()}%"
    sql += " AND (LOWER(emp.razao) LIKE ? OR LOWER(e.nome_fantasia) LIKE ?)"
    params += [v, v]
if uf:
    sql += " AND e.uf = ?"; params.append(uf.upper())
if municipio:
    sql += " AND CAST(e.municipio AS TEXT) LIKE ?"; params.append(f"%{municipio}%")
if cnae:
    sql += " AND e.cnae_principal = ?"; params.append(cnae)

sql += " LIMIT 1000"

if st.button("Buscar", type="primary"):
    try:
        df = query(sql, tuple(params))
        if df.empty: st.warning("Nenhum resultado.")
        else:
            st.success(f"{len(df)} linha(s).")
            st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Erro: {e}")