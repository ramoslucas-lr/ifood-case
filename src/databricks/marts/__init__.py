"""
Registry Factory para resolver e instanciar os Data Marts da camada Gold.
"""

from .base_mart import DataMart
from .monthly_revenue import MonthlyRevenueMart
from .hourly_passengers import HourlyPassengersMart


def get_mart(mart_name: str) -> DataMart:
    """
    Retorna a instância correta de DataMart baseada no nome.
    
    Args:
        mart_name (str): Identificador do Data Mart.
        
    Returns:
        DataMart: A instância da classe apropriada.
        
    Raises:
        ValueError: Caso o mart não seja encontrado no registry.
    """
    marts = {
        "monthly_revenue": MonthlyRevenueMart(),
        "hourly_passengers": HourlyPassengersMart()
    }
    
    if mart_name not in marts:
        raise ValueError(f"Data Mart '{mart_name}' não encontrado no registry.")
        
    return marts[mart_name]
