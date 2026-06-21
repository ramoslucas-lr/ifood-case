"""
Módulo base para regras de transformação de dados.
Define a interface comum (TransformationRule) para todos os processadores da camada Silver.
"""

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class TransformationRule(ABC):
    """
    Interface abstrata para aplicação de regras de negócio em DataFrames.
    Todas as regras de datasets específicos devem herdar desta classe.
    """

    @abstractmethod
    def apply(self, df: DataFrame) -> DataFrame:
        """
        Aplica regras de limpeza, formatação e qualidade aos dados.

        Args:
            df (DataFrame): O DataFrame bruto extraído da camada Bronze.

        Returns:
            DataFrame: O DataFrame processado, pronto para a camada Silver.
        """
        pass
