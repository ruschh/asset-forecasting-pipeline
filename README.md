[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![CI](https://github.com/ruschh/asset-forecasting-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/ruschh/asset-forecasting-pipeline/actions/workflows/ci.yml)

# asset-forecasting-pipeline

Pipeline de **ETL → EDA → Modelagem → Deploy** para previsão de ativos financeiros a partir de séries temporais de preços e indicadores de mercado.

O objetivo do projeto é construir uma solução de ponta a ponta que:
- Extrai dados financeiros brutos (por exemplo, via APIs de mercado);
- Organiza e limpa os dados em uma arquitetura reproduzível;
- Realiza análise exploratória (EDA) focada em risco, retorno e volatilidade;
- Treina modelos de Machine Learning para prever movimento futuro de preço (subida/queda ou retorno);
- Disponibiliza as previsões em um dashboard interativo.

---

## Estrutura de diretórios

A estrutura atual do projeto (podendo ser expandida depois) é:

```bash
asset-forecasting-pipeline/
├── app/                # Código do dashboard (por exemplo, Streamlit, Plotly, etc.)
├── configs/            # Arquivos de configuração (YAML/JSON) para ETL, modelos, paths, etc.
├── data/               # Dados brutos, intermediários e processados (normalmente não versionados)
│    ├── raw/           # Dados brutos (extraídos da fonte, sem tratamento)
│    ├── bronze/        # Dados parcialmente tratados / features intermediárias
│    ├── silver/
│    └── gold/          # Bases finais prontas para modelagem e deploy
│
├── etl/                # Módulos de extração, transformação e carga de dados
├── models/             # Modelos treinados (.joblib, métricas, artefatos de ML)
├── notebooks/          # Notebooks de EDA, experimentos e protótipos
├── __pycache__/        # Arquivos gerados pelo Python (não versionados)
├── README.md
└── .gitignore
```

## Instalação

Clone o repositório via SSH (ajuste o usuário se necessário):

```bash
git clone git@github.com:ruschh/asset-forecasting-pipeline.git
cd asset-forecasting-pipeline
```

Crie e ative um ambiente virtual (exemplo com venv):

```bash
python -m venv .venv
source .venv/bin/activate      # Linux/Mac
# .venv\Scripts\activate       # Windows (PowerShell)
```

Instale as dependências:
```bash
pip install -r requirements.txt
```


