# etl/utils/config.py

from pathlib import Path
import yaml

# Considera que este arquivo está em: projeto_previsao_ativos/etl/utils/config.py
BASE_DIR = Path(__file__).resolve().parents[2]  # raiz do projeto
DATA_DIR = BASE_DIR / "data"
CONFIG_DIR = BASE_DIR / "configs"


def load_assets_config() -> dict:
    """
    Lê configs/assets.yml e retorna dicionário com ativos e benchmark.

    Exemplo de assets.yml:

    assets:
      - ticker: BBAS3
        path: "BBAS3.xlsx"
      - ticker: PETR4
        path: "PETR4.xlsx"

    benchmark:
      - name: IBOV
        path: "IBOV.xlsx"
    """
    config_path = CONFIG_DIR / "assets.yml"
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg


def get_paths() -> dict:
    """
    Retorna os diretórios principais (raw, bronze, silver, gold).
    """
    return {
        "raw": DATA_DIR / "raw",
        "bronze": DATA_DIR / "bronze",
        "silver": DATA_DIR / "silver",
        "gold": DATA_DIR / "gold",
    }