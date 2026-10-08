"""Data passed to an adapter subprocess and back."""
from dataclasses import dataclass, field

import pandas as pd


@dataclass
class Job:
    adapter: str
    params: dict
    interactions: pd.DataFrame   # training interactions only
    events: pd.DataFrame         # raw watch/rating events restricted to training pairs
    movies: pd.DataFrame         # full catalog
    users: pd.DataFrame
    user_ids: list[int]          # users to produce recommendations for
    k: int
    repo_dir: str                # external/<repo>, or "" for in-harness adapters
    data_dir: str
    cache_dir: str               # persistent across runs
    work_dir: str = ""           # filled in by the runner; temporary


@dataclass
class JobResult:
    recommendations: dict[int, list[str]]
    failed_users: dict[int, str] = field(default_factory=dict)
    notes: dict = field(default_factory=dict)

    def to_json(self) -> dict:
        return {
            "recommendations": {str(u): list(m) for u, m in self.recommendations.items()},
            "failed_users": {str(u): reason for u, reason in self.failed_users.items()},
            "notes": self.notes,
        }

    @classmethod
    def from_json(cls, data: dict) -> "JobResult":
        return cls(
            {int(u): list(m) for u, m in data["recommendations"].items()},
            {int(u): reason for u, reason in data["failed_users"].items()},
            dict(data["notes"]),
        )
