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