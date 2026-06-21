# iFood Data Engineering Case: NYC TLC Pipeline

## 📖 Visão Geral
Este repositório contém a solução completa de Engenharia de Dados para o desafio do iFood, focado no processamento em escala do dataset **NYC TLC (Yellow Taxi)**. A arquitetura foi desenvolvida priorizando modularidade, testabilidade offline, tolerância a falhas (Schema Drift) e governança através de configuração por código (YAML).

## 🏗️ Arquitetura e Tecnologias

A esteira de dados implementa a rigorosa **Medallion Architecture (Raw -> Bronze -> Silver -> Gold)**, operando em um ecossistema Cloud-Native:

- **Orquestração**: Apache Airflow (com padrão de DAG Factory dinâmico via YAML).
- **Processamento Distribuído**: Databricks (PySpark) usando Jobs parametrizados.
- **Storage**: Amazon S3 e Delta Lake.
- **Qualidade de Código**: Testes Unitários Offline via Pytest, formatação estrita com Black/Flake8 e Tipagem Estática rigorosa (Type Hinting).

### Fluxo de Dados:
`Airflow (Sensor/Ingestion) -> S3 (Raw) -> Databricks (Bronze: Delta Upsert) -> Databricks (Silver: Rules/Qualidade) -> Databricks (Gold: Data Marts)`

## 📂 Estrutura do Repositório

```bash
├── src/
│   ├── airflow/
│   │   ├── dags/nyc_tlc_ingestion.py        # DAG unificada que orquestra Databricks dinamicamente
│   │   └── include/nyc_tlc_datasets.yaml    # Configuração YAML (Controla os datasets do DAG Factory)
│   ├── databricks/
│   │   ├── raw_to_bronze_job.py             # Ingestão agnóstica com blindagem contra Schema Drift
│   │   ├── bronze_to_silver_job.py          # Job de limpeza e injeção do motor de regras
│   │   ├── silver_to_gold_job.py            # Job de agregação parametrizável para Data Marts
│   │   ├── rules/                           # Motor de Regras de Negócio (ex: NYCYellowTaxiRule)
│   │   └── marts/                           # Motor Analítico / Agregações (ex: MonthlyRevenueMart)
├── tests/                                   # Suite de Testes Pytest (Roda puramente na Memória/RAM)
├── analysis/                                # EDA e Respostas SQL das perguntas de Negócio
└── ARCHITECTURE_EVOLUTION.md                # Reflexões Arquiteturais e próximos passos
```

## 🚀 Como Executar

### 1. Pré-requisitos
- Conta AWS (IAM permissions para gravação/leitura no S3).
- Workspace Databricks (Unity Catalog ou Hive Metastore configurado).
- Ambiente Airflow.
- Python 3.10+ e Conda instalados localmente.

### 2. Configurando o Ambiente e Rodando Testes Unitários
Graças ao design pattern de injeção de dependência via Classes (`TransformationRule` e `DataMart`), a lógica pesada de Spark pode ser testada localmente de forma offline, sem encostar em um cluster em nuvem.

```bash
# Crie e ative um ambiente limpo
conda create -n ifood-test python=3.10 -y
conda activate ifood-test

# Instale os requisitos e a engine do Spark local
pip install -r requirements.txt pyspark pytest

# Execute a Suite de Testes com Pytest
python3 -m pytest tests/ -v
```

### 3. Deploy no Databricks
Para fazer o deploy dos Jobs Python no Workspace, utilizamos a Databricks CLI (`databricks.yml`):
```bash
databricks bundle deploy -t prod
```

### 4. Orquestração (Airflow)
Na UI do Airflow, configure as conexões base:
- `nyc_tlc_connection`: (HTTP) Endpoint base dos arquivos da prefeitura (https://d37ci6vzurychx.cloudfront.net)
- `databricks_default`: Token de Autenticação do seu Workspace Databricks.
- `aws_default`: Credenciais de acesso ao S3 Raw Layer.

Ative a DAG `nyc_tlc_ingestion_pipeline`. Graças ao **DAG Factory**, novos datasets são ingeridos automaticamente assim que registrados no `nyc_tlc_datasets.yaml`.

## 🛡️ Principais Decisões e Destaques de Engenharia

1. **Resolução Agnóstica de Schema Drift**: Arquivos da TLC costumam mudar tipos subitamente ao longo dos meses (ex: `passenger_count` pulando de Integer para Double). O job `raw_to_bronze` detecta os metadados da tabela Delta existente no catálogo e força um _safe cast_ antes do upsert, evitando a temida falha `[DELTA_FAILED_TO_MERGE_FIELDS]`.
2. **Design Pattern: DAG Factory**: O Airflow aqui atua apenas como orquestrador. Para adicionar uma nova camada da TLC (como For-Hire Vehicles), não é necessário tocar em código Python. Basta injetar as propriedades no arquivo YAML e a arquitetura expande suas tasks sozinha.
3. **Desacoplamento e Clean Code**: Todo o código está estritamente tipado e aderente ao PEP-8 / PEP-257, com injeção de dependência garantindo a qualidade e facilidade de novos desenvolvedores darem manutenção à plataforma.

---
_Desenvolvido para o case técnico de Engenharia de Dados do iFood._