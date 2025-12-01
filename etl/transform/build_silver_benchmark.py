# etl/transform/build_silver_benchmark.py

from pathlib import Path
import pandas as pd

from etl.utils.config import get_paths
from etl.utils.io import save_parquet


def run_build_silver_benchmark() -> None:
    """
    BRONZE -> SILVER para o benchmark IBOV, adicionando ibov_ret_1d.
    """
    paths = get_paths()
    bronze_dir: Path = paths["bronze"]
    silver_dir: Path = paths["silver"]

    df = pd.read_parquet(bronze_dir / "benchmark_IBOV.parquet")
    df = df.sort_values("date").copy()

    df["ibov_ret_1d"] = df["ibov_close"].pct_change()

    save_parquet(df, silver_dir / "benchmark_ibov.parquet")
    print(f"[SILVER] benchmark_ibov salvo em {silver_dir}")
