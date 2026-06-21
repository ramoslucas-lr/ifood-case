"""
Registry Factory para resolver e instanciar as regras de transformação de acordo com o dataset.
"""

from .base_rule import TransformationRule
from .nyc_yellow_taxi import NYCYellowTaxiRule


def get_rule(dataset_name: str) -> TransformationRule:
    """
    Retorna a instância correta de TransformationRule baseada no nome do dataset.

    Args:
        dataset_name (str): O identificador lógico do dataset (ex: 'yellow').

    Returns:
        TransformationRule: A instância da classe de regra apropriada.

    Raises:
        ValueError: Caso o dataset não possua uma regra mapeada no registry.
    """
    rules = {"yellow": NYCYellowTaxiRule()}

    if dataset_name not in rules:
        raise ValueError(
            f"Regra para o dataset '{dataset_name}' não encontrada no registry."
        )

    return rules[dataset_name]
