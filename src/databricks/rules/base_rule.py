from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class TransformationRule(ABC):
    @abstractmethod
    def apply(self, df: DataFrame) -> DataFrame:
        pass
