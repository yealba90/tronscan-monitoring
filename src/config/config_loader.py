import yaml
from pathlib import Path

def load_wallets(config_path: str = "configs/wallets.yml") -> list[str]:
    """
    Carga las billeteras desde un archivo YAML.
    """
    path = Path(config_path)
    if not path.is_file():
        raise FileNotFoundError(f"El archivo de configuración {config_path} no existe.")
    
    with open(path, 'r') as file:
        config = yaml.safe_load(file)
    
    return config.get("wallets", [])