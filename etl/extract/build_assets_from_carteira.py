import yaml
import time
import pandas as pd
import yfinance as yf

from pathlib import Path
from typing import List, Dict

from etl.utils.config import DATA_DIR, CONFIG_DIR
from etl.utils.io import ensure_dir


CARTEIRA_FILE = DATA_DIR / "raw" / "Carteira Ativos.xlsx"
ASSETS_YML   = CONFIG_DIR / "assets.yml"


# ===============================================================
# 1. Extrai tickers do arquivo Carteira Ativos.xlsx
# ===============================================================

def extract_tickers_from_carteira(carteira_path: Path) -> List[str]:
    """
    Lê 'Carteira Ativos.xlsx' e extrai a lista de tickers únicos em maiúsculas.
    Assume coluna 'Unnamed: 1' com os códigos, e uma linha de cabeçalho 'Ativo'.
    """
    df = pd.read_excel(carteira_path)

    col_ativo = "Unnamed: 1"
    if col_ativo not in df.columns:
        raise ValueError(f"Coluna '{col_ativo}' não encontrada em {carteira_path}")

    ativos = (
        df[col_ativo]
        .dropna()
        .astype(str)
        .str.strip()
    )

    # Remove cabeçalho "Ativo" se existir
    ativos = ativos[ativos.str.upper() != "ATIVO"]

    # Padroniza para maiúsculas (resolve o problema do 'brav3')
    tickers = sorted(ativos.str.upper().unique())
    return tickers


# ===============================================================
# 2. Download históricos via yfinance (ações, FIIs e IBOV)
# ===============================================================

def download_price_history_yfinance(ticker: str,
                                    out_dir: Path,
                                    start: str = "2015-01-01") -> Path:
    """
    Baixa histórico diário de um ativo da B3 via yfinance e salva em XLSX.

    Para ativos da B3, usa o padrão '<TICKER>.SA', por exemplo:
    - BBAS3 -> BBAS3.SA
    - ALZR11 -> ALZR11.SA

    A função achata colunas MultiIndex, se existirem, antes de salvar.
    """
    ensure_dir(out_dir)

    yf_ticker = f"{ticker}.SA"
    print(f"[YF] Baixando histórico de {ticker} via yfinance ({yf_ticker})...")

    hist = yf.download(
        yf_ticker,
        start=start,
        auto_adjust=False,
        group_by="column",
        progress=False,
    )

    if hist.empty:
        print(f"[AVISO] yfinance não retornou dados para {ticker}. Salvando vazio.")
        df = pd.DataFrame()
    else:
        df = hist.reset_index()  # 'Date' vira coluna

        # Se vier MultiIndex nas colunas, achata
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                "_".join([str(c) for c in col if c not in (None, "", " ")])
                for col in df.columns.to_list()
            ]

    out_path = out_dir / f"{ticker}.xlsx"
    df.to_excel(out_path, index=False)
    time.sleep(0.3)  # pequena pausa para evitar rate limit
    print(f"[OK] {ticker} salvo em {out_path}")
    return out_path


def download_ibov_yfinance(out_dir: Path,
                           start: str = "2015-01-01") -> Path:
    """
    Baixa histórico do IBOVESPA via yfinance (ticker '^BVSP') e salva como IBOV.xlsx.
    """
    ensure_dir(out_dir)

    yf_ticker = "^BVSP"
    print(f"[YF] Baixando histórico do IBOV via yfinance ({yf_ticker})...")

    hist = yf.download(
        yf_ticker,
        start=start,
        auto_adjust=False,
        group_by="column",
        progress=False,
    )

    if hist.empty:
        print("[AVISO] yfinance não retornou dados para o IBOV. Salvando vazio.")
        df = pd.DataFrame()
    else:
        df = hist.reset_index()

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                "_".join([str(c) for c in col if c not in (None, "", " ")])
                for col in df.columns.to_list()
            ]

    out_path = out_dir / "IBOV.xlsx"
    df.to_excel(out_path, index=False)
    time.sleep(0.3)
    print(f"[OK] IBOV salvo em {out_path}")
    return out_path


# ===============================================================
# 3. Criar assets.yml
# ===============================================================

def build_assets_yaml(tickers: List[str]) -> Dict:
    """
    Gera o dicionário de configuração (assets.yml) no formato esperado pelo ETL.
    """
    assets = [{"ticker": t, "path": f"{t}.xlsx"} for t in tickers]
    benchmark = [{"name": "IBOV", "path": "IBOV.xlsx"}]
    return {"assets": assets, "benchmark": benchmark}


def save_assets_yaml(cfg: Dict, path: Path) -> None:
    ensure_dir(path.parent)
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(cfg, f, allow_unicode=True, sort_keys=False)
    print(f"[OK] assets.yml criado em {path}")


# ===============================================================
# 4. Pipeline completo
# ===============================================================

def run_build_assets_and_download(start: str = "2015-01-01") -> None:
    """
    Pipeline completo usando somente yfinance:

      1) Lê Carteira Ativos.xlsx
      2) Extrai tickers (maiúsculos)
      3) Baixa histórico de cada ticker via yfinance (TICKER.SA)
      4) Baixa histórico do IBOV (^BVSP)
      5) Gera configs/assets.yml
    """
    if not CARTEIRA_FILE.exists():
        raise FileNotFoundError(f"Arquivo da carteira não encontrado: {CARTEIRA_FILE}")

    print("[ETAPA] Extraindo tickers da carteira...")
    tickers = extract_tickers_from_carteira(CARTEIRA_FILE)
    print(f"[INFO] Tickers encontrados: {tickers}")

    raw_dir = DATA_DIR / "raw"

    print("\n[ETAPA] Baixando históricos dos ativos (yfinance):")
    for t in tickers:
        download_price_history_yfinance(t, raw_dir, start=start)

    print("\n[ETAPA] Baixando histórico do IBOV (yfinance):")
    download_ibov_yfinance(raw_dir, start=start)

    print("\n[ETAPA] Gerando assets.yml...")
    cfg = build_assets_yaml(tickers)
    save_assets_yaml(cfg, ASSETS_YML)

    print("\n[FINALIZADO] Pipeline via yfinance concluído com sucesso!")


if __name__ == "__main__":
    run_build_assets_and_download(start="2015-01-01")