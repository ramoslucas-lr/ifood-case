"""
Registry Factory para resolver e instanciar as regras de transformação de acordo com o dataset.
"""

from .base_rule import TransformationRule
from .nyc_yellow_taxi import NYCYellowTaxiRule


def get_rule(rule_name: str) -> TransformationRule:
    """
    Retorna a instância correta de TransformationRule baseada no nome da regra.

    Args:
        rule_name (str): O identificador lógico da regra (ex: 'nyc_yellow_taxi').

    Returns:
        TransformationRule: A instância da classe de regra apropriada.

    Raises:
        ValueError: Caso a regra não seja encontrada no registry.
    """
    rules = {"nyc_yellow_taxi": NYCYellowTaxiRule()}

    if rule_name not in rules:
        raise ValueError(
            f"Regra '{rule_name}' não encontrada no registry."
        )

    return rules[rule_name]
