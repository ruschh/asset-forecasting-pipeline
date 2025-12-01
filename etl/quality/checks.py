# etl/quality/checks.py

import pandas as pd


def check_no_null_dates(df: pd.DataFrame) -> None:
    if "date" not in df.columns:
        raise ValueError("DataFrame não contém coluna 'date'.")
    if df["date"].isna().any():
        raise ValueError("Há datas nulas na tabela.")


def check_unique_date_ticker(df: pd.DataFrame) -> None:
    """
    Garante que (date, ticker) é chave única.
    Ignora se não houver coluna 'ticker'.
    """
    if "ticker" not in df.columns:
        return
    if df.duplicated(subset=["date", "ticker"]).any():
        raise ValueError("Chave (date, ticker) não é única.")


def check_non_negative_prices(df: pd.DataFrame) -> None:
    if "close" not in df.columns:
        return
    if (df["close"] < 0).any():
        raise ValueError("Há preços negativos em 'close'.")


def run_basic_price_checks(df: pd.DataFrame) -> None:
    """
    Função agregadora que roda checagens básicas em dados de preço.
    """
    check_no_null_dates(df)
    check_unique_date_ticker(df)
    check_non_negative_prices(df)
