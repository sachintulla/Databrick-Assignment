"""
Configuration loader for the SuperStore Analytics System.
Reads config.yaml and exposes typed accessors.
"""

import os
from pathlib import Path
from typing import Any

import yaml

from src.utils.logger import get_logger

logger = get_logger(__name__)

_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config.yaml"


def load_config(config_path: str = None) -> dict:
    """
    Load YAML configuration file.

    Args:
        config_path: Optional override path to config file.

    Returns:
        Dictionary containing all configuration values.

    Raises:
        FileNotFoundError: If the config file does not exist.
        yaml.YAMLError: If the config file contains invalid YAML.
    """
    path = Path(config_path) if config_path else _CONFIG_PATH

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    logger.info(f"Configuration loaded from: {path}")
    return config


def get_path(key: str, config: dict = None) -> str:
    """
    Resolve a path from config relative to the project root.

    Args:
        key: Key within config['paths'] section.
        config: Optional pre-loaded config dict.

    Returns:
        Absolute path string.
    """
    cfg = config or load_config()
    project_root = Path(__file__).resolve().parents[2]
    relative = cfg["paths"].get(key, "")
    return str(project_root / relative)


def get_value(section: str, key: str, config: dict = None) -> Any:
    """
    Retrieve a configuration value by section and key.

    Args:
        section: Top-level section name in config.yaml.
        key: Key within the section.
        config: Optional pre-loaded config dict.

    Returns:
        Configuration value.

    Raises:
        KeyError: If section or key is not found.
    """
    cfg = config or load_config()
    if section not in cfg:
        raise KeyError(f"Config section '{section}' not found.")
    if key not in cfg[section]:
        raise KeyError(f"Config key '{key}' not found in section '{section}'.")
    return cfg[section][key]
