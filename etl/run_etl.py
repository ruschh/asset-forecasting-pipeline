# etl/run_etl.py

from prefect import flow, task

from etl.extract.extract_prices import run_extract_prices
from etl.extract.extract_benchmark import run_extract_benchmark
from etl.transform.build_silver_prices import run_build_silver_prices
from etl.transform.build_silver_benchmark import run_build_silver_benchmark
from etl.transform.build_gold_features_labels import run_build_gold_features_labels


# === Tasks (wrappers) ===

@task(name="Extract Prices")
def extract_prices_task() -> None:
    run_extract_prices()


@task(name="Extract Benchmark")
def extract_benchmark_task() -> None:
    run_extract_benchmark()


@task(name="Build SILVER Prices")
def build_silver_prices_task() -> None:
    run_build_silver_prices()


@task(name="Build SILVER Benchmark")
def build_silver_benchmark_task() -> None:
    run_build_silver_benchmark()


@task(name="Build GOLD Features & Labels")
def build_gold_features_labels_task() -> None:
    run_build_gold_features_labels()


# === Flow principal ===

@flow(name="ETL Previsão de Ativos")
def etl_previsao_ativos_flow() -> None:
    """
    Flow principal do Prefect para orquestrar o pipeline de ETL.
    Ordem:

    1) RAW -> BRONZE
    2) BRONZE -> SILVER
    3) SILVER -> GOLD
    """
    # RAW -> BRONZE
    extract_prices_task()
    extract_benchmark_task()

    # BRONZE -> SILVER
    build_silver_prices_task()
    build_silver_benchmark_task()

    # SILVER -> GOLD
    build_gold_features_labels_task()


if __name__ == "__main__":
    # Permite rodar localmente com: python -m etl.run_etl
    etl_previsao_ativos_flow()
