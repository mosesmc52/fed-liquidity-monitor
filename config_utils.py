# config_utils.py
from __future__ import annotations

import os
import re
from typing import Any, Dict

from dotenv import load_dotenv

_ENV_PATTERN = re.compile(r"^\$\{([A-Z0-9_]+)\}$")
load_dotenv()


def expand_env_vars(obj: Any) -> Any:
    """
    Recursively replace values like "${VAR_NAME}" with os.environ["VAR_NAME"] if present.
    Leaves value unchanged if env var is missing.
    """
    if isinstance(obj, dict):
        return {k: expand_env_vars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [expand_env_vars(v) for v in obj]
    if isinstance(obj, str):
        m = _ENV_PATTERN.match(obj.strip())
        if m:
            return os.getenv(m.group(1), obj)
        return obj
    return obj
