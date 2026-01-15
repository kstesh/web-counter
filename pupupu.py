from typing import Literal
from yaml import safe_load


def _load_all_configs(path: str) -> dict:
    with open(path, "r") as f:
        return safe_load(f)


def load_config(config_type: Literal["backend", "client"], path="config.yaml") -> dict:
    configs = _load_all_configs(path)
    if config_type not in ["backend", "client"]:
        raise KeyError(f"Unknown config type: {config_type}")

    if config_type == "backend":
        return configs["server"] | configs["storage"]
    elif config_type == "client":
        return configs["client"] | configs["server"]
    return {}