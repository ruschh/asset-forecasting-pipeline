# etl/extract/extract_benchmark.py

from pathlib import Path
import pandas as pd

from etl.utils.config import load_assets_config, get_paths
from etl.utils.io import read_excel_or_csv, save_parquet


def standardize_benchmark_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza o layout do benchmark (IBOV) vindo do Excel gerado pelo yfinance.

    Entrada típica (IBOV.xlsx via yfinance.reset_index()):
        Date, Open_^BVSP, High_^BVSP, Low_^BVSP, Close_^BVSP, Adj Close_^BVSP, Volume_^BVSP

    Saída:
        date, ibov_close, ibov_ret_1d
    """
    df = df_raw.copy()

    # 1) Normaliza nomes de colunas: strip + lower
    col_norm = {c: str(c).strip() for c in df.columns}
    df.rename(columns=col_norm, inplace=True)

    col_lower = {c: c.lower() for c in df.columns}
    df.rename(columns=col_lower, inplace=True)
    # Ex.: 'Adj Close_^BVSP' -> 'adj close_^bvsp'

    # 2) Detecta coluna de data
    date_col = None
    for c in df.columns:
        if "date" in c or "data" in c:
            date_col = c
            break

    if date_col is None:
        raise ValueError("Coluna de data ('Data'/'Date') não encontrada no benchmark.")

    # 3) Detecta coluna de fechamento (prefere adj close)
    close_col = None
    for c in df.columns:
        if "adj close" in c or "adjclose" in c or "adj_close" in c:
            close_col = c
            break
    if close_col is None:
        for c in df.columns:
            if "close" in c:
                close_col = c
                break

    if close_col is None:
        raise ValueError("Coluna de fechamento ('Adj Close' ou 'Close') não encontrada no benchmark.")

    # 4) Monta DataFrame padronizado
    df_std = df[[date_col, close_col]].copy()
    df_std.rename(columns={date_col: "date", close_col: "ibov_close"}, inplace=True)

    # 5) Converte para datetime e ordena
    df_std["date"] = pd.to_datetime(df_std["date"])
    df_std = df_std.sort_values("date")

    # 6) Calcula retorno diário
    df_std["ibov_ret_1d"] = df_std["ibov_close"].pct_change()

    return df_std


def run_extract_benchmark() -> None:
    """
    RAW -> BRONZE para o benchmark (IBOV) definido em configs/assets.yml.
    """
    cfg = load_assets_config()
    paths = get_paths()
    raw_dir: Path = paths["raw"]
    bronze_dir: Path = paths["bronze"]

    bench_cfg_list = cfg.get("benchmark", [])
    if not bench_cfg_list:
        raise ValueError("Nenhum benchmark definido em configs/assets.yml (chave 'benchmark').")

    bench_cfg = bench_cfg_list[0]
    file_path = raw_dir / bench_cfg["path"]

    df_raw = read_excel_or_csv(file_path)
    df_std = standardize_benchmark_df(df_raw)

    out_path = bronze_dir / "benchmark_IBOV.parquet"
    save_parquet(df_std, out_path)
    print(f"[EXTRACT] Benchmark IBOV salvo em BRONZE: {out_path}")
