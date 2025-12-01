# etl/extract/extract_prices.py

from pathlib import Path
import pandas as pd

from etl.utils.config import load_assets_config, get_paths
from etl.utils.io import read_excel_or_csv, save_parquet


def standardize_price_df(df_raw: pd.DataFrame, ticker: str | None = None) -> pd.DataFrame:
    """
    Padroniza o layout dos preços vindos de fontes externas para o formato interno:

        date, ticker, open, high, low, close, volume

    - Normaliza nomes de colunas para minúsculas.
    - Aceita variações como:
        Date, data, Adj Close, Close, High, Low, Open, Volume,
        e versões com sufixo de ticker (ex.: 'close_bbas3.sa').
    """
    df = df_raw.copy()

    # 1) Normaliza nomes de colunas: strip + lower
    col_norm = {c: str(c).strip() for c in df.columns}
    df.rename(columns=col_norm, inplace=True)

    col_lower = {c: c.lower() for c in df.columns}
    df.rename(columns=col_lower, inplace=True)
    # Ex.: 'Adj Close_BBAS3.SA' -> 'adj close_bbas3.sa'

    # 2) Detecta coluna de data
    date_col = None
    for c in df.columns:
        if "date" in c or "data" in c:
            date_col = c
            break

    if date_col is None:
        raise ValueError("Coluna de data ('Date'/'Data') não encontrada na planilha.")

    # 3) Detecta colunas OHLCV por substring

    def find_col(substring_list, avoid_substrings=None):
        """
        Procura, na ordem, por qualquer substring em substring_list dentro dos nomes de coluna.
        Opcionalmente evita colunas que contenham avoid_substrings.
        """
        avoid_substrings = avoid_substrings or []
        for sub in substring_list:
            for c in df.columns:
                if sub in c:
                    if any(a in c for a in avoid_substrings):
                        continue
                    return c
        return None

    # abertura
    open_col = find_col(["open"])
    # máxima
    high_col = find_col(["high"])
    # mínima
    low_col = find_col(["low"])
    # fechamento: prefere 'adj close' se existir, depois 'close'
    close_col = find_col(["adj close", "adjclose", "adj_close"])
    if close_col is None:
        close_col = find_col(["close"])
    # volume
    volume_col = find_col(["volume", "vol"])

    rename_map = {}
    if open_col:
        rename_map[open_col] = "open"
    if high_col:
        rename_map[high_col] = "high"
    if low_col:
        rename_map[low_col] = "low"
    if close_col:
        rename_map[close_col] = "close"
    if volume_col:
        rename_map[volume_col] = "volume"

    df = df.rename(columns=rename_map)

    required_cols = ["open", "high", "low", "close", "volume"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Colunas OHLCV faltando na planilha: {missing}")

    # 4) Monta DataFrame padronizado
    df_std = df[[date_col, "open", "high", "low", "close", "volume"]].copy()
    df_std.rename(columns={date_col: "date"}, inplace=True)

    # 5) Converte para datetime
    df_std["date"] = pd.to_datetime(df_std["date"])

    # 6) Adiciona ticker, se fornecido
    if ticker is not None:
        df_std["ticker"] = ticker

    cols = ["date", "ticker", "open", "high", "low", "close", "volume"] if "ticker" in df_std.columns else \
           ["date", "open", "high", "low", "close", "volume"]

    return df_std[cols]


def run_extract_prices() -> None:
    """
    RAW -> BRONZE para todos os ativos definidos em configs/assets.yml.
    """
    cfg = load_assets_config()
    paths = get_paths()
    raw_dir: Path = paths["raw"]
    bronze_dir: Path = paths["bronze"]

    assets = cfg.get("assets", [])
    if not assets:
        raise ValueError("Nenhum ativo definido em configs/assets.yml (chave 'assets').")

    for asset in assets:
        ticker = asset["ticker"]
        # assume que 'path' é o nome do arquivo dentro de data/raw
        file_path = raw_dir / asset["path"]

        df_raw = read_excel_or_csv(file_path)
        df_std = standardize_price_df(df_raw, ticker=ticker)

        out_path = bronze_dir / f"prices_{ticker}.parquet"
        save_parquet(df_std, out_path)
        print(f"[EXTRACT] Ativo {ticker} salvo em BRONZE: {out_path}")
