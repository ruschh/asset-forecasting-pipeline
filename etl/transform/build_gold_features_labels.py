# etl/transform/build_gold_features_labels.py

from pathlib import Path
import numpy as np
import pandas as pd

from etl.utils.config import get_paths
from etl.utils.io import save_parquet


def add_asset_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Recebe asset_prices_daily com colunas:
      date, ticker, open, high, low, close, volume
    Retorna com features: retornos, volatilidade, EMAs, lags etc.
    """
    df = df.sort_values(["ticker", "date"]).copy()

    # ==========================
    # Retornos
    # ==========================
    df["ret_1d"] = df.groupby("ticker")["close"].pct_change()
    df["ret_5d"] = df.groupby("ticker")["close"].pct_change(periods=5)

    # ==========================
    # Volatilidade móvel (21 dias) anualizada
    # ==========================
    rolling_vol = (
        df.groupby("ticker")["ret_1d"]
        .rolling(21)
        .std()
        .reset_index(level=0, drop=True)
    )
    df["vol_21d"] = rolling_vol * np.sqrt(252)

    # ==========================
    # Médias móveis exponenciais (EMAs)
    # ==========================
    # EMA de 9 períodos
    df["ema_9"] = (
        df.groupby("ticker")["close"]
        .transform(lambda s: s.ewm(span=9, adjust=False).mean())
    )

    # EMA de 72 períodos
    df["ema_72"] = (
        df.groupby("ticker")["close"]
        .transform(lambda s: s.ewm(span=72, adjust=False).mean())
    )

    # EMA de 200 períodos
    df["ema_200"] = (
        df.groupby("ticker")["close"]
        .transform(lambda s: s.ewm(span=200, adjust=False).mean())
    )

    # Ratios entre EMAs (úteis como features)
    df["ema_9_72_ratio"] = df["ema_9"] / df["ema_72"]
    df["ema_9_200_ratio"] = df["ema_9"] / df["ema_200"]

    # ==========================
    # Lags de retornos (ex: 3 lags)
    # ==========================
    for lag in [1, 2, 3]:
        df[f"ret_1d_lag{lag}"] = (
            df.groupby("ticker")["ret_1d"].shift(lag)
        )

    return df


def define_label(df: pd.DataFrame) -> pd.DataFrame:
    """
    Define target_direction: prever se o retorno de amanhã é positivo ou não.
    """
    df = df.sort_values(["ticker", "date"]).copy()

    df["futuro_ret_1d"] = df.groupby("ticker")["ret_1d"].shift(-1)
    df["target_direction"] = (df["futuro_ret_1d"] > 0).astype(int)

    # Remove linhas sem label
    df = df.dropna(subset=["futuro_ret_1d"])
    return df


def add_ibov_features(df_assets: pd.DataFrame, df_ibov: pd.DataFrame) -> pd.DataFrame:
    """
    Faz join por date, trazendo ibov_ret_1d e seus lags.
    """
    df_ibov = df_ibov.sort_values("date").copy()

    # Lags do ibov
    for lag in [1, 2, 3]:
        df_ibov[f"ibov_ret_lag{lag}"] = df_ibov["ibov_ret_1d"].shift(lag)

    df_merged = df_assets.merge(df_ibov, on="date", how="left")
    return df_merged


def _max_drawdown_from_ret(returns: pd.Series) -> float:
    """
    Calcula max drawdown aproximado a partir de série de retornos diários.
    """
    cum = (1 + returns.fillna(0)).cumprod()
    running_max = cum.cummax()
    drawdown = (cum - running_max) / running_max
    return float(drawdown.min())  # valor negativo


def compute_asset_kpis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula KPIs agregados por ticker:
    - mean_ret_1d
    - vol_daily
    - vol_annual
    - sharpe_like
    - skew_ret
    - kurtosis_ret
    - hit_ratio
    - max_drawdown
    """
    grouped = df.groupby("ticker")

    # usa funções explícitas para evitar erro com 'kurt'
    base = grouped["ret_1d"].agg(
        mean_ret_1d="mean",
        vol_daily="std",
        skew_ret=lambda x: x.skew(),
        kurtosis_ret=lambda x: x.kurt(),   # ou x.kurtosis()
    ).reset_index()

    base["vol_annual"] = base["vol_daily"] * np.sqrt(252)
    base["sharpe_like"] = base["mean_ret_1d"] / base["vol_daily"]

    hits = grouped["ret_1d"].apply(lambda x: (x > 0).mean()).reset_index(name="hit_ratio")
    mdd = grouped["ret_1d"].apply(_max_drawdown_from_ret).reset_index(name="max_drawdown")

    kpis = base.merge(hits, on="ticker").merge(mdd, on="ticker")
    return kpis


def run_build_gold_features_labels() -> None:
    """
    SILVER -> GOLD:
    - asset_features_daily.parquet (features + label target_direction)
    - asset_kpis_summary.parquet
    """
    paths = get_paths()
    silver_dir: Path = paths["silver"]
    gold_dir: Path = paths["gold"]

    df_prices = pd.read_parquet(silver_dir / "asset_prices_daily.parquet")
    df_ibov = pd.read_parquet(silver_dir / "benchmark_ibov.parquet")

    # Features por ativo
    df_feat = add_asset_features(df_prices)

    # Label de classificação
    df_feat = define_label(df_feat)

    # Features do IBOV
    df_feat = add_ibov_features(df_feat, df_ibov)

    # Persistência da tabela principal
    save_parquet(df_feat, gold_dir / "asset_features_daily.parquet")

    # KPIs agregados
    df_kpis = compute_asset_kpis(df_feat)
    save_parquet(df_kpis, gold_dir / "asset_kpis_summary.parquet")

    print(f"[GOLD] asset_features_daily e asset_kpis_summary salvos em {gold_dir}")
