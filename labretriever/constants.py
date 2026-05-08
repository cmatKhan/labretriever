import os
from pathlib import Path

from huggingface_hub.constants import HF_HUB_CACHE


def get_cache_dir() -> Path:
    """
    Return the HuggingFace cache directory.

    Reads ``HF_CACHE_DIR`` at call time so that the environment variable can be
    set after module import (e.g. from a CLI flag) and still be respected.

    :returns: Resolved cache directory path.
    :rtype: Path

    """
    return Path(os.getenv("HF_CACHE_DIR", HF_HUB_CACHE))


def get_hf_token() -> str | None:
    """Get HuggingFace token from environment variable."""
    return os.getenv("HF_TOKEN")
