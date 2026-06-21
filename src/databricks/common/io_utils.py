"""
Utilitários genéricos para operações de Input/Output (Leitura e Escrita) de dados
no Apache Spark / Databricks.
"""

from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    """
    Lê arquivos Parquet de um caminho especificado no Data Lake.

    Args:
        spark (SparkSession): Sessão ativa do Spark.
        path (str): Caminho absoluto ou relativo para os arquivos (ex: s3a://...).

    Returns:
        DataFrame: O DataFrame contendo os dados lidos.
    """
    return spark.read.parquet(path)


def write_delta_upsert(
    df: DataFrame,
    path: str,
    table_name: str,
    partition_cols: List[str],
    replace_condition: Optional[str] = None,
) -> None:
    """
    Grava um DataFrame no formato Delta, registrando-o como uma tabela externa.
    Suporta sobrescrita idempotente de partições através do parâmetro `replaceWhere`.

    Args:
        df (DataFrame): DataFrame a ser escrito.
        path (str): Caminho físico no Data Lake onde os dados residirão.
        table_name (str): Nome da tabela no catálogo (ex: default.silver_nyc_tlc).
        partition_cols (List[str]): Lista de colunas pelas quais os dados serão particionados.
        replace_condition (Optional[str]): Condição SQL para sobrescrever apenas dados específicos.
            Exemplo: "ano = '2023' AND mes = '01'". Se None, faz overwrite completo.
    """
    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .partitionBy(*partition_cols)
    )

    if replace_condition:
        writer = writer.option("replaceWhere", replace_condition)

    writer.option("path", path).saveAsTable(table_name)
