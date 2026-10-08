"""Where each model's code lives: external clones, or this repo when it contains the model."""
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
# Models whose code may live in the repo hosting the harness, with a file that proves it
LOCAL_MARKERS = {"Helixan": "src/recommender.py"}


def resolve_repo(repo: str | None, external_dir: Path, project_root: Path = PROJECT_ROOT) -> Path | None:
    if repo is None:
        return None
    marker = LOCAL_MARKERS.get(repo)
    if marker and (project_root / marker).is_file():
        return project_root
    return Path(external_dir) / repo
