import os
from pathlib import Path

from huggingface_hub.constants import HF_HUB_CACHE

CACHE_DIR = Path(os.getenv("HF_CACHE_DIR", HF_HUB_CACHE))


def get_hf_token() -> str | None:
    """Get HuggingFace token from environment variable."""
    return os.getenv("HF_TOKEN")
