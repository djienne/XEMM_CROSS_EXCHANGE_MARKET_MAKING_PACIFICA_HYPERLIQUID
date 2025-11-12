"""
Configuration loader for XEMM Bot.

Loads configuration from config.json and credentials from .env file.
Supports environment variable substitution in config values.
"""

import json
import os
import re
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv


def load_config(config_path: str = "config.json") -> Dict[str, Any]:
    """
    Load configuration from JSON file and environment variables.

    Loads credentials from .env file and substitutes ${VAR_NAME} placeholders
    in the config with actual environment variable values.

    Args:
        config_path: Path to configuration JSON file

    Returns:
        Dictionary with configuration

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If required environment variables are missing
        json.JSONDecodeError: If config file is invalid JSON
    """
    # Load .env file from xemm directory (project root)
    project_root = Path(__file__).parent.parent
    env_path = project_root / ".env"

    if env_path.exists():
        load_dotenv(env_path)
        print(f"Loaded environment variables from {env_path}")
    else:
        print(f"Warning: .env file not found at {env_path}")

    # Load config file
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_file, 'r') as f:
        config = json.load(f)

    # Substitute environment variables
    config = _substitute_env_vars(config)

    # Validate required fields
    _validate_config(config)

    return config


def _substitute_env_vars(obj: Any) -> Any:
    """
    Recursively substitute ${VAR_NAME} with environment variable values.

    Args:
        obj: Object to process (dict, list, str, or other)

    Returns:
        Object with substituted values
    """
    if isinstance(obj, dict):
        return {key: _substitute_env_vars(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [_substitute_env_vars(item) for item in obj]
    elif isinstance(obj, str):
        # Find all ${VAR_NAME} patterns
        pattern = r'\$\{([^}]+)\}'
        matches = re.findall(pattern, obj)

        result = obj
        for var_name in matches:
            env_value = os.getenv(var_name)
            if env_value is None:
                raise ValueError(f"Environment variable not found: {var_name}")
            result = result.replace(f"${{{var_name}}}", env_value)

        return result
    else:
        return obj


def _validate_config(config: Dict[str, Any]):
    """
    Validate that required configuration fields are present.
    Credentials are validated from environment variables, not config.

    Args:
        config: Configuration dictionary

    Raises:
        ValueError: If required fields are missing
    """
    # Validate credentials are in environment variables
    required_env_vars = [
        "HL_WALLET",
        "HL_PRIVATE_KEY",
        "SOL_WALLET",
        "API_PUBLIC",
        "API_PRIVATE"
    ]

    missing_vars = []
    for var in required_env_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)

    if missing_vars:
        raise ValueError(
            f"Missing required environment variables in .env: {', '.join(missing_vars)}\n"
            f"Please create a .env file with these variables."
        )

    # Check for required trading parameters
    if "symbols" not in config:
        raise ValueError("Missing required parameter: symbols")
    if not isinstance(config["symbols"], list) or len(config["symbols"]) == 0:
        raise ValueError("symbols must be a non-empty list")

    if "order_size_usd" not in config:
        raise ValueError("Missing required parameter: order_size_usd")

    print("Configuration validated successfully")
    print(f"Credentials loaded from environment variables")


def get_hyperliquid_credentials() -> Dict[str, str]:
    """
    Get Hyperliquid credentials from environment variables.

    Returns:
        Dictionary with Hyperliquid credentials

    Raises:
        ValueError: If required environment variables are missing
    """
    wallet = os.getenv("HL_WALLET")
    private_key = os.getenv("HL_PRIVATE_KEY")

    missing = []
    if not wallet:
        missing.append("HL_WALLET")
    if not private_key:
        missing.append("HL_PRIVATE_KEY")

    if missing:
        raise ValueError(f"Missing Hyperliquid credentials in .env: {', '.join(missing)}")

    return {
        "address": wallet,
        "private_key": private_key
    }


def get_pacifica_credentials() -> Dict[str, str]:
    """
    Get Pacifica credentials from environment variables.

    Returns:
        Dictionary with Pacifica credentials

    Raises:
        ValueError: If required environment variables are missing
    """
    sol_wallet = os.getenv("SOL_WALLET")
    api_public = os.getenv("API_PUBLIC")
    api_private = os.getenv("API_PRIVATE")

    missing = []
    if not sol_wallet:
        missing.append("SOL_WALLET")
    if not api_public:
        missing.append("API_PUBLIC")
    if not api_private:
        missing.append("API_PRIVATE")

    if missing:
        raise ValueError(f"Missing Pacifica credentials in .env: {', '.join(missing)}")

    return {
        "sol_wallet": sol_wallet,
        "api_public": api_public,
        "private_key": api_private  # Note: using 'private_key' for consistency
    }


