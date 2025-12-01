import numpy as np
import pandas as pd
from pathlib import Path

def dataframe_coeficientes(coeficientes, colunas):
    return pd.DataFrame(
        data = coeficientes, 
        index = colunas, 
        columns = ["coeficiente"],
    ).sort_values(by="coeficiente")

def read_parquet_robust(path: Path) -> pd.DataFrame:
    """
    Tenta ler um parquet usando pyarrow; se falhar, tenta fastparquet.
    """
    try:
        print(f"[INFO] Lendo com engine='pyarrow': {path.name}")
        return pd.read_parquet(path, engine="pyarrow")
    except Exception as e1:
        print(f"[WARN] Falha com pyarrow: {e1}")
        print(f"[INFO] Tentando engine='fastparquet'...")
        try:
            return pd.read_parquet(path, engine="fastparquet")
        except Exception as e2:
            print(f"[ERRO] Falha também com fastparquet: {e2}")
            raise