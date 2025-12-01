# etl/utils/io.py

from pathlib import Path
import pandas as pd


def ensure_dir(path: Path) -> None:
    """
    Garante que o diretório exista.
    """
    path.mkdir(parents=True, exist_ok=True)


def read_excel_or_csv(path: Path) -> pd.DataFrame:
    """
    Lê planilha Excel ou CSV e retorna DataFrame.
    """
    suffix = path.suffix.lower()
    if suffix in [".xlsx", ".xls"]:
        df = pd.read_excel(path)
    elif suffix == ".csv":
        df = pd.read_csv(path)
    else:
        raise ValueError(f"Formato de arquivo não suportado: {path}")
    return df


def save_parquet(df: pd.DataFrame, path: Path) -> None:
    """
    Salva DataFrame em formato Parquet (sem índice).
    """
    ensure_dir(path.parent)
    df.to_parquet(path, index=False)