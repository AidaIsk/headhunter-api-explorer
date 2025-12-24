import yaml
from pathlib import Path

def load_enabled_search_profiles():
    """
    Загружает search_profiles.yaml и возвращает
    только профили с enabled = true
    """
    config_path = Path("configs/search_profiles.yaml")

    with open(config_path, "r", encoding="UTF-8") as file:
        data = yaml.safe_load(file)

    profiles = data.get("profiles", [])
    enabled_profiles = [p for p in profiles if p.get("enabled")]

    return enabled_profiles

