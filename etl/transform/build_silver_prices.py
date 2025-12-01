# etl/transform/build_silver_prices.py

from pathlib import Path
import pandas as pd

from etl.utils.config import load_assets_config, get_paths
from etl.utils.io import save_parquet
from etl.utils.calendar import build_trading_calendar
from etl.quality.checks import run_basic_price_checks


def load_all_bronze_prices(bronze_dir: Path, tickers: list[str]) -> pd.DataFrame:
    """
    Lê todos os arquivos de BRONZE para os tickers informados e concatena.
    """
    dfs = []
    for ticker in tickers:
        path = bronze_dir / f"prices_{ticker}.parquet"
        df = pd.read_parquet(path)
        dfs.append(df)
    df_all = pd.concat(dfs, ignore_index=True)
    return df_all


def run_build_silver_prices() -> None:
    """
    BRONZE -> SILVER para preços dos ativos (asset_prices_daily + trading_calendar).
    """
    cfg = load_assets_config()
    paths = get_paths()
    bronze_dir: Path = paths["bronze"]
    silver_dir: Path = paths["silver"]

    tickers = [a["ticker"] for a in cfg.get("assets", [])]
    if not tickers:
        raise ValueError("Nenhum ativo definido em configs/assets.yml (chave 'assets').")

    df_all = load_all_bronze_prices(bronze_dir, tickers)

    # Remover duplicatas e registros sem date/close
    df_all = df_all.drop_duplicates(subset=["date", "ticker"])
    df_all = df_all.dropna(subset=["date", "close"])

    # Checagens básicas
    run_basic_price_checks(df_all)

    # Calendário de pregão
    calendar = build_trading_calendar(df_all)

    # Persistência
    save_parquet(df_all, silver_dir / "asset_prices_daily.parquet")
    save_parquet(calendar, silver_dir / "trading_calendar.parquet")

    print(f"[SILVER] asset_prices_daily e trading_calendar salvos em {silver_dir}")
