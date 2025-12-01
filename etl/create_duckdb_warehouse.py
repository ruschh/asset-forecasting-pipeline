from pathlib import Path
import duckdb

# Caminhos base
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # sobe 2 níveis: etl/ -> template/
DATA_DIR     = PROJECT_ROOT / "data"
GOLD_DIR     = DATA_DIR / "gold"
DB_PATH      = DATA_DIR / "warehouse.duckdb"

def main():
    print(f"[INFO] Projeto raiz: {PROJECT_ROOT}")
    print(f"[INFO] Diretório GOLD: {GOLD_DIR}")
    print(f"[INFO] Banco DuckDB: {DB_PATH}")

    # Cria diretórios se necessário
    DATA_DIR.mkdir(exist_ok=True, parents=True)

    # Conecta (cria o arquivo se não existir)
    con = duckdb.connect(DB_PATH.as_posix())
    print("[INFO] Conectado ao DuckDB.")

    # === 1. Criar / recriar tabela asset_features_daily ===
    features_parquet = GOLD_DIR / "asset_features_daily.parquet"
    if features_parquet.exists():
        print(f"[INFO] Criando tabela asset_features_daily a partir de {features_parquet.name}")
        con.execute("""
            CREATE OR REPLACE TABLE asset_features_daily AS
            SELECT *
            FROM read_parquet(?);
        """, [features_parquet.as_posix()])
    else:
        print(f"[WARN] Arquivo {features_parquet} não encontrado. Tabela asset_features_daily não será criada.")

    # === 2. Criar / recriar tabela asset_kpis_summary ===
    kpis_parquet = GOLD_DIR / "asset_kpis_summary.parquet"
    if kpis_parquet.exists():
        print(f"[INFO] Criando tabela asset_kpis_summary a partir de {kpis_parquet.name}")
        con.execute("""
            CREATE OR REPLACE TABLE asset_kpis_summary AS
            SELECT *
            FROM read_parquet(?);
        """, [kpis_parquet.as_posix()])
    else:
        print(f"[WARN] Arquivo {kpis_parquet} não encontrado. Tabela asset_kpis_summary não será criada.")

    # (Opcional) listar tabelas criadas
    tables = con.execute("SHOW TABLES").df()
    print("\n[INFO] Tabelas no warehouse:")
    print(tables)

    con.close()
    print("\n[INFO] Warehouse DuckDB construído/atualizado com sucesso.")

if __name__ == "__main__":
    main()
