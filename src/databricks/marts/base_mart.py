"""
Módulo base para construção de Data Marts na camada Gold.
"""

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession


class DataMart(ABC):
    """
    Interface abstrata para criação de tabelas da camada Gold (Marts).
    """

    @abstractmethod
    def build(self, spark: SparkSession, df_fact: DataFrame) -> DataFrame:
        """
        Constrói o Data Mart aplicando lógicas de negócio, joins e agregações.
        
        Args:
            spark (SparkSession): Sessão do Spark (permite ler outras tabelas se necessário).
            df_fact (DataFrame): DataFrame contendo a tabela fato principal já filtrada.
            
        Returns:
            DataFrame: O DataFrame final do Data Mart.
        """
        pass
