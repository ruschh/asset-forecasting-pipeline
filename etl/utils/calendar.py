# etl/utils/calendar.py

import pandas as pd


def build_trading_calendar(df_prices: pd.DataFrame) -> pd.DataFrame:
    """
    Constrói calendário de pregão a partir das datas existentes em df_prices.
    Assume que df_prices possui coluna 'date' (datetime).
    """
    unique_dates = df_prices["date"].dropna().sort_values().unique()
    calendar = pd.DataFrame({"date": unique_dates})
    return calendar