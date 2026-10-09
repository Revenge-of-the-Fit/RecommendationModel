"""Paths to the repository's existing dataset, trained model, and profile cache."""

import os
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from dataset import DATA_DIRECTORY
from preferences import CACHE_DIRECTORY
from recommender import MODEL_PATH
from storage.database import DEFAULT_STORAGE_PATH


class ServingSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    data_directory: Path = DATA_DIRECTORY
    model_path: Path = MODEL_PATH
    cache_directory: Path = CACHE_DIRECTORY
    storage_path: Path = DEFAULT_STORAGE_PATH
    request_log_queue_size: int = Field(default=1024, gt=0)
    storage_busy_timeout: float = Field(default=1.0, gt=0, allow_inf_nan=False)
    storage_max_bytes: int = Field(default=16 * 1024**3, gt=0)
    storage_min_free_bytes: int = Field(default=1024**3, ge=0)
    request_log_shutdown_timeout: float = Field(default=5.0, gt=0, allow_inf_nan=False)
    live_enabled: bool = False
    live_source_id: str = "cmu-movielog"
    live_topic: str = "movielog2"
    metadata_base_url: str = "http://128.2.24.239:8080"
    profile_refresh_seconds: float = Field(default=86400, gt=0, allow_inf_nan=False)

    @classmethod
    def from_environment(cls) -> "ServingSettings":
        defaults = cls()
        return cls(**{
            name: os.environ.get(name.upper(), getattr(defaults, name))
            for name in cls.model_fields
        })
