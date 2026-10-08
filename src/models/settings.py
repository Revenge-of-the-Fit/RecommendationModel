"""Paths to the repository's existing dataset, trained model, and profile cache."""

import os
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from dataset import DATA_DIRECTORY
from preferences import CACHE_DIRECTORY
from recommender import MODEL_PATH


class ServingSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    data_directory: Path = DATA_DIRECTORY
    model_path: Path = MODEL_PATH
    cache_directory: Path = CACHE_DIRECTORY

    @classmethod
    def from_environment(cls) -> "ServingSettings":
        defaults = cls()
        return cls(**{
            name: os.environ.get(name.upper(), getattr(defaults, name))
            for name in cls.model_fields
        })
