"""Paths to the repository's existing dataset, trained model, and profile cache."""

import os
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from dataset import DATA_DIRECTORY
from preferences import CACHE_DIRECTORY
from recommender import MODEL_PATH


class ServingSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    data_directory: Path = DATA_DIRECTORY
    model_path: Path = MODEL_PATH
    cache_directory: Path = CACHE_DIRECTORY
    storage_path: Path = DATA_DIRECTORY / "live" / "events.sqlite3"
    request_log_queue_size: int = Field(default=1024, gt=0)
    storage_busy_timeout: float = Field(default=1.0, gt=0, allow_inf_nan=False)
    request_log_shutdown_timeout: float = Field(default=5.0, gt=0, allow_inf_nan=False)

    @classmethod
    def from_environment(cls) -> "ServingSettings":
        defaults = cls()
        return cls(**{
            name: os.environ.get(name.upper(), getattr(defaults, name))
            for name in cls.model_fields
        })
