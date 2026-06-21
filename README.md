# iFood Data Engineering Case: NYC TLC Pipeline

## Visão Geral
Este repositório contém a solução de Engenharia de Dados para o desafio do iFood, baseada no processamento do dataset NYC TLC (Yellow Taxi). A arquitetura tem foco em modularidade, testes offline, tratamento de Schema Drift e governança por código (YAML).

## Arquitetura e Tecnologias

A pipeline de dados utiliza a Medallion Architecture (Raw -> Bronze -> Silver -> Gold):

- **Orquestração e Metadados**: Apache Airflow e YAML (Pipeline 100% Metadata-Driven: define fluxos, regras da Silver e Data Marts da Gold via configuração).
- **Processamento Distribuído**: Databricks (PySpark) e Databricks Asset Bundles.
- **Storage**: Amazon S3 (Delta Lake).
- **Testes e Qualidade**: Pytest, Black e Flake8.

### Fluxo de Dados:
`Airflow (HttpToS3Operator) -> S3 (Raw) -> Databricks (Bronze) -> Databricks (Silver) -> Databricks (Gold)`

## Estrutura do Repositório

```bash
├── src/
│   ├── airflow/
│   │   ├── dags/nyc_tlc_ingestion.py        # DAG unificada que orquestra tarefas dinamicamente
│   │   └── include/nyc_tlc_datasets.yaml    # Configuração de datasets para geração dinâmica
│   ├── databricks/
│   │   ├── raw_to_bronze_job.py             # Ingestão de dados brutos com conversão de tipos
│   │   ├── bronze_to_silver_job.py          # Limpeza de dados e aplicação de regras de negócio
│   │   ├── silver_to_gold_job.py            # Agregação de Data Marts
│   │   ├── rules/                           # Motor de Regras de Negócio (ex: NYCYellowTaxiRule)
│   │   └── marts/                           # Motor Analítico (ex: MonthlyRevenueMart)
├── tests/                                   # Suite de testes (Pytest)
├── analysis/
│   ├── eda_nyc_tlc.ipynb                    # Notebook de Análise Exploratória de Dados (EDA)
│   └── respostas.sql                        # Queries SQL correspondentes às perguntas do case
├── airflow/                                 # Ambiente local do Apache Airflow (Docker Compose)
├── databricks.yml                           # Configurações de deploy no Databricks
├── pyproject.toml                           # Configurações de ferramentas (Black, Pytest)
├── requirements.txt                         # Dependências do projeto
└── .flake8                                  # Configuração de Linter
```

## Execução

### 1. Pré-requisitos
- Conta AWS (permissões de gravação/leitura no S3).
- Workspace Databricks configurado.
- Ambiente Airflow local ou em nuvem.
- Python 3.10+ e Conda instalados.

### 2. Configurando o Ambiente e Testes
A lógica de Spark foi modelada usando injeção de dependência (`TransformationRule` e `DataMart`), permitindo testes locais sem a necessidade de instanciar um cluster remoto.

```bash
# Crie um ambiente isolado com Python e o Java 17 (requerido pelo Spark 4.0+)
conda create -n ifood-test python=3.10 openjdk=17 -c conda-forge -y
conda activate ifood-test

# Instale os requisitos
pip install -r requirements.txt

# Execute os testes
python3 -m pytest tests/ -v

# Valide a formatação de código
flake8 src/ tests/
```

### 3. Deploy no Databricks
Para fazer o deploy dos Jobs no Workspace, utilize o Databricks CLI (`databricks.yml`):
```bash
databricks bundle deploy -t prod
```

### 4. Orquestração (Airflow)
Para executar o pipeline localmente, inicie os containers do Airflow:
```bash
cd airflow/
docker compose up -d
```
Acesse a interface gráfica em `http://localhost:8080` (usuário/senha padrão: `airflow`).

Na UI do Airflow, configure as conexões base:
- `nyc_tlc_connection`: Tipo HTTP com Host `https://d37ci6vzurychx.cloudfront.net`
- `databricks_default`: Conexão com o seu Workspace Databricks.
- `aws_default`: Credenciais de acesso ao S3.

Em seguida, ative a DAG `nyc_tlc_ingestion`. Graças à arquitetura orientada a metadados, novos fluxos, datasets (ex: For-Hire Vehicles), motores de regras da camada Silver e Data Marts da camada Gold podem ser adicionados na pipeline editando apenas o arquivo `nyc_tlc_datasets.yaml`. Nenhuma modificação no código fonte Python é necessária!

## Decisões Técnicas

1. **Tratamento de Schema Drift**: Os arquivos da TLC costumam alterar os tipos de dados nativos. O job `raw_to_bronze` coleta o esquema atual da tabela Delta e realiza casts dinâmicos antes do _upsert_, evitando falhas de compatibilidade do Delta Lake (`DELTA_FAILED_TO_MERGE_FIELDS`).
2. **Pipeline Orientada a Metadados (Metadata-Driven)**: A orquestração não possui regras _hardcoded_. A DAG lê o arquivo `nyc_tlc_datasets.yaml` não apenas para instanciar as tarefas de ingestão, mas também para definir **qual regra de negócio rodar na Silver** e **quais Data Marts instanciar na Gold**. Para adicionar novos fluxos (como _Green Taxis_ ou _For-Hire Vehicles_), basta atualizar o YAML. Nenhuma alteração em código Python é necessária!
3. **Padrão de Código**: O repositório segue tipagem nativa, docstrings em padrão PEP-257 e regras de formatação consolidadas pelo `black` e `flake8`.