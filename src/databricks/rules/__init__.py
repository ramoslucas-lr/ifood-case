from .base_rule import TransformationRule
from .nyc_yellow_taxi import NYCYellowTaxiRule

def get_rule(dataset_name: str) -> TransformationRule:
    rules = {
        "yellow_taxi": NYCYellowTaxiRule()
    }
    
    if dataset_name not in rules:
        raise ValueError(f"Regra para o dataset '{dataset_name}' não encontrada no registry.")
        
    return rules[dataset_name]
