# lib/loaders.py
# Pipeline robusto p/ ZIPs/CSVs da RFB (até quando o arquivo interno não tem extensão).
# Lê em chunks (sep=';') -> Parquet -> insere em DuckDB. Concatenação via GLOB.

from __future__ import annotations
import io, zipfile
from pathlib import Path
from typing import List, Tuple
import duckdb, pandas as pd, requests
from tqdm import tqdm

DATA = Path("data"); DATA.mkdir(exist_ok=True)
DB_PATH = (DATA / "cnpj.duckdb").as_posix()

def open_con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH, read_only=False)

def query(sql: str, params: Tuple | None = None) -> pd.DataFrame:
    con = open_con()
    try:
        return con.execute(sql, params or ()).fetchdf()
    finally:
        con.close()

def ensure_table_from_parquet(name: str, parquet_path: Path, replace: bool = False) -> None:
    con = open_con()
    try:
        if replace: con.execute(f"DROP TABLE IF EXISTS {name}")
        con.execute(f"CREATE TABLE IF NOT EXISTS {name} AS SELECT * FROM parquet_scan('{parquet_path.as_posix()}') LIMIT 0")
        con.execute(f"INSERT INTO {name} SELECT * FROM parquet_scan('{parquet_path.as_posix()}')")
    finally:
        con.close()

def download_zip(url: str, out_zip: Path) -> Path:
    out_zip.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with open(out_zip, "wb") as f, tqdm(total=total, unit="B", unit_scale=True, desc=out_zip.name) as pbar:
            for chunk in r.iter_content(1024*1024):
                if chunk: f.write(chunk); pbar.update(len(chunk))
    return out_zip

def _choose_zip_member(zf: zipfile.ZipFile, prefer_keywords: List[str] | None = None) -> zipfile.ZipInfo:
    members = [m for m in zf.infolist() if not m.is_dir()]
    if not members: raise FileNotFoundError("Nenhum arquivo elegível dentro do ZIP.")
    if prefer_keywords:
        keys = [k.lower() for k in prefer_keywords]
        by_kw = [m for m in members if any(k in m.filename.lower() for k in keys)]
        if by_kw: return max(by_kw, key=lambda m: m.file_size)
    return max(members, key=lambda m: m.file_size)

def extract_tabular_from_zip(zip_path: Path, prefer_keywords: List[str] | None = None) -> io.BytesIO:
    with zipfile.ZipFile(zip_path, "r") as z:
        member = _choose_zip_member(z, prefer_keywords)
        raw = z.read(member)
    # “CSV-like” check leve
    _ = raw[:4096].decode("latin1", errors="ignore")
    return io.BytesIO(raw)

def _read_csv_iterator(fobj: io.BytesIO | str, chunksize: int):
    try:
        return pd.read_csv(fobj, sep=";", dtype=str, chunksize=chunksize, encoding="latin1", low_memory=False)
    except UnicodeDecodeError:
        if hasattr(fobj, "seek"): fobj.seek(0)
        return pd.read_csv(fobj, sep=";", dtype=str, chunksize=chunksize, encoding="utf-8", low_memory=False)

def read_csv_semicolon_to_parquet(fobj: io.BytesIO | str, name: str, chunksize: int = 400_000) -> Path:
    # limpa temporários antigos
    for old in DATA.glob(f"tmp_{name}_*.parquet"):
        try: old.unlink(missing_ok=True)
        except: pass

    it = _read_csv_iterator(fobj, chunksize)
    total = 0
    for i, chunk in enumerate(it):
        # normalização leve -> string
        for c in chunk.columns:
            if chunk[c].dtype != "object":
                chunk[c] = chunk[c].astype("string")
        (DATA / f"tmp_{name}_{i:06d}.parquet").write_bytes(chunk.to_parquet(index=False))
        total += len(chunk)
    if total == 0: raise ValueError("Nenhum chunk lido do CSV.")

    # concatena via GLOB
    con = open_con()
    try:
        final_path = DATA / f"{name}.parquet"
        globpat = (DATA / f"tmp_{name}_*.parquet").as_posix()
        con.execute(f"COPY (SELECT * FROM parquet_scan('{globpat}')) TO '{final_path.as_posix()}' (FORMAT PARQUET)")
    finally:
        con.close()
    # limpa
    for p in DATA.glob(f"tmp_{name}_*.parquet"):
        try: p.unlink(missing_ok=True)
        except: pass
    return final_path

def prepare_from_zip_url(url: str, name: str) -> Path:
    zip_path = DATA / f"{name}.zip"
    download_zip(url, zip_path)
    fobj = extract_tabular_from_zip(zip_path, prefer_keywords=[name])
    parquet = read_csv_semicolon_to_parquet(fobj, name)
    ensure_table_from_parquet(name, parquet, replace=True)
    return parquet

def prepare_from_uploaded_zip_bytes(zip_bytes: bytes, name: str) -> Path:
    tmp_zip = DATA / f"tmp_upload_{name}.zip"
    tmp_zip.write_bytes(zip_bytes)
    try:
        fobj = extract_tabular_from_zip(tmp_zip, prefer_keywords=[name])
        parquet = read_csv_semicolon_to_parquet(fobj, name)
        ensure_table_from_parquet(name, parquet, replace=True)
        return parquet
    finally:
        try: tmp_zip.unlink(missing_ok=True)
        except: pass

def prepare_from_uploaded_csv_bytes(csv_bytes: bytes, name: str) -> Path:
    fobj = io.BytesIO(csv_bytes)
    parquet = read_csv_semicolon_to_parquet(fobj, name)
    ensure_table_from_parquet(name, parquet, replace=True)
    return parquet
    
# ===================== BLOCO NOVO (FINAL DO loaders.py) =====================

# Nomes esperados por mês/ano (pastas oficiais da RFB). Nem todos os meses têm todos os arquivos.
_EXPECTED_FILES = {
    "empresas": ["Empresas1.zip", "Empresas2.zip"],
    "estabelecimentos": ["Estabelecimentos1.zip", "Estabelecimentos2.zip", "Estabelecimentos3.zip"],
    "socios": ["Socios1.zip", "Socios2.zip"],
    "simples": ["Simples.zip"],
    "paises": ["Paises.zip"],
    "municipios": ["Municipios.zip"],
    "qualificacoes": ["Qualificacoes.zip"],
    "naturezas": ["Naturezas.zip"],
    "cnaes": ["Cnaes.zip"],
}

# Palavras-chave para localizar o arquivo interno “sem extensão” dentro do ZIP
_DATASET_KEYWORDS = {
    "empresas": ["empresas", "empresa"],
    "estabelecimentos": ["estabelec", "estabelecimentos"],
    "socios": ["socios", "sócios", "socio"],
    "simples": ["simples", "mei"],
    "paises": ["paises", "países", "pais"],
    "municipios": ["municipio", "municípios", "municipios"],
    "qualificacoes": ["qualificacao", "qualificações", "qualificacoes"],
    "naturezas": ["natureza", "naturezas"],
    "cnaes": ["cnae", "cnaes"],
}

def month_dir_url(year: int, month: int) -> str:
    """
    Retorna a URL canônica do diretório de um mês/ano no portal de dados abertos da RFB.
    Ex.: 2025-06 → https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-06/
    """
    return f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{year:04d}-{month:02d}/"

def prepare_all_for_month(year: int, month: int, datasets: list[str] | None = None) -> list[tuple[str, str, str, str]]:
    """
    Baixa e prepara todos os datasets escolhidos para um determinado mês/ano.
    Retorna uma lista de tuplas de log: (status, dataset, url, msg)
      - status: 'ok' | 'warn' | 'error'
      - dataset: nome lógico (empresas, estabelecimentos, ...)
      - url: URL tentada (arquivo ZIP)
      - msg: mensagem resumida do resultado ou do erro
    """
    base = month_dir_url(year, month)
    targets = datasets or list(_EXPECTED_FILES.keys())
    logs: list[tuple[str, str, str, str]] = []

    for dataset in targets:
        files = _EXPECTED_FILES.get(dataset, [])
        if not files:
            logs.append(("warn", dataset, base, "Sem arquivos esperados configurados para este dataset"))
            continue

        keywords = _DATASET_KEYWORDS.get(dataset, [dataset])

        for fname in files:
            url = base + fname
            try:
                zip_path = DATA / f"{dataset}_{year:04d}{month:02d}_{fname}"
                # download pode falhar se o arquivo não existir no mês — tratamos como aviso
                download_zip(url, zip_path)

                fobj = extract_tabular_from_zip(zip_path, prefer_keywords=keywords)
                parquet = read_csv_semicolon_to_parquet(fobj, dataset)
                ensure_table_from_parquet(dataset, parquet, replace=True)

                logs.append(("ok", dataset, url, f"Preparado: {parquet.name}"))
            except Exception as e:
                # alguns meses não têm todos os pacotes → registrar e seguir
                err = str(e)
                if "404" in err or "Not Found" in err:
                    logs.append(("warn", dataset, url, "Arquivo não disponível no mês/ano informado"))
                else:
                    logs.append(("error", dataset, url, f"Falha: {e}"))
    return logs
# ===================== FIM DO BLOCO NOVO =====================

# ===================== BLOCO DE CATÁLOGO (FINAL DO loaders.py) =====================
from datetime import datetime

def _ensure_catalog():
    """Cria a tabela de catálogo se não existir."""
    con = open_con()
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS catalog (
              id BIGINT,
              dataset TEXT NOT NULL,
              month_ref TEXT NOT NULL,        -- 'YYYY-MM'
              source_url TEXT,
              parquet_path TEXT NOT NULL,
              rows BIGINT,
              loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    finally:
        con.close()

def _count_rows_in_parquet(parquet_path: Path) -> int | None:
    try:
        con = open_con()
        try:
            return con.execute(
                f"SELECT COUNT(*) AS n FROM parquet_scan('{parquet_path.as_posix()}')"
            ).fetchone()[0]
        finally:
            con.close()
    except Exception:
        return None

def add_to_catalog(dataset: str, month_ref: str, parquet_path: Path, source_url: str | None):
    """Insere um registro no catálogo."""
    _ensure_catalog()
    rows = _count_rows_in_parquet(parquet_path)
    con = open_con()
    try:
        con.execute(
            "INSERT INTO catalog (id, dataset, month_ref, source_url, parquet_path, rows, loaded_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                None,  # id (sem PK/auto-inc; pode ficar NULL)
                dataset,
                month_ref,
                source_url or "",
                parquet_path.as_posix(),
                rows if rows is not None else None,
                datetime.now(),
            )
        )
    finally:
        con.close()

def get_catalog():
    """Retorna o catálogo ordenado do mais recente para o mais antigo."""
    _ensure_catalog()
    return query("""
        SELECT
          COALESCE(id, ROW_NUMBER() OVER (ORDER BY loaded_at DESC)) AS id,
          dataset, month_ref, source_url, parquet_path, rows, loaded_at
        FROM catalog
        ORDER BY loaded_at DESC
    """)

# ---- Reexport: URL do mês já existe no arquivo; se não, mantenha aqui para a página usar ----
def month_dir_url(year: int, month: int) -> str:
    return f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{year:04d}-{month:02d}/"

# Atualize o wizard para registrar em catálogo:
# Substitua a função prepare_all_for_month que você tem por esta versão (mesma assinatura, agora com add_to_catalog)
def prepare_all_for_month(year: int, month: int, datasets: list[str] | None = None) -> list[tuple[str, str, str, str]]:
    """
    Baixa e prepara todos os datasets escolhidos para um determinado mês/ano.
    Retorna lista de logs: (status, dataset, url, msg).
    Também grava um registro no catálogo para cada parquet gerado com sucesso.
    """
    base = month_dir_url(year, month)

    # nomes esperados por dataset (alguns meses não têm todos; tudo bem)
    expected = {
        "empresas": ["Empresas1.zip", "Empresas2.zip"],
        "estabelecimentos": ["Estabelecimentos1.zip", "Estabelecimentos2.zip", "Estabelecimentos3.zip"],
        "socios": ["Socios1.zip", "Socios2.zip"],
        "simples": ["Simples.zip"],
        "paises": ["Paises.zip"],
        "municipios": ["Municipios.zip"],
        "qualificacoes": ["Qualificacoes.zip"],
        "naturezas": ["Naturezas.zip"],
        "cnaes": ["Cnaes.zip"],
    }
    # keywords para pegar o arquivo interno certo dentro do ZIP, mesmo sem extensão
    kw = {
        "empresas": ["empresas", "empresa"],
        "estabelecimentos": ["estabelec", "estabelecimentos"],
        "socios": ["socios", "sócios", "socio"],
        "simples": ["simples", "mei"],
        "paises": ["paises", "países", "pais"],
        "municipios": ["municipio", "municípios", "municipios"],
        "qualificacoes": ["qualificacao", "qualificações", "qualificacoes"],
        "naturezas": ["natureza", "naturezas"],
        "cnaes": ["cnae", "cnaes"],
    }

    targets = datasets or list(expected.keys())
    month_ref = f"{year:04d}-{month:02d}"
    logs: list[tuple[str, str, str, str]] = []

    for dataset in targets:
        files = expected.get(dataset, [])
        if not files:
            logs.append(("warn", dataset, base, "Sem arquivos esperados configurados para este dataset"))
            continue

        keywords = kw.get(dataset, [dataset])

        for fname in files:
            url = base + fname
            try:
                zip_path = DATA / f"{dataset}_{year:04d}{month:02d}_{fname}"
                download_zip(url, zip_path)

                fobj = extract_tabular_from_zip(zip_path, prefer_keywords=keywords)
                parquet = read_csv_semicolon_to_parquet(fobj, dataset)
                ensure_table_from_parquet(dataset, parquet, replace=True)

                # registra catálogo
                add_to_catalog(dataset, month_ref, parquet, url)

                logs.append(("ok", dataset, url, f"Preparado: {parquet.name}"))
            except Exception as e:
                err = str(e)
                if "404" in err or "Not Found" in err:
                    logs.append(("warn", dataset, url, "Arquivo não disponível no mês/ano informado"))
                else:
                    logs.append(("error", dataset, url, f"Falha: {e}"))
    return logs
# ===================== FIM DO BLOCO DE CATÁLOGO =====================