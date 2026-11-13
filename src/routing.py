"""Small, persistent traffic configuration shared by the router and deployment CLI."""
import hashlib
import json
import os
import tempfile
from pathlib import Path


SLOTS = ("blue", "green")
BACKENDS = {"blue": "http://api:8082", "green": "http://api-green:8082"}
MODEL_DIRECTORY = Path("/app/models")


def other_slot(slot):
    if slot not in SLOTS:
        raise ValueError("Model slot must be blue or green")
    return "green" if slot == "blue" else "blue"


def read_routing(path):
    config = json.loads(Path(path).read_text(encoding="utf-8"))
    other_slot(config["primary"])
    percentage = config["secondary_percent"]
    if type(percentage) is not int or not 0 <= percentage <= 100:
        raise ValueError("Secondary traffic percentage must be an integer from 0 to 100")
    for key in ("generation", "salt"):
        if not isinstance(config.get(key), str) or not config[key].strip():
            raise ValueError(f"Routing configuration needs {key}")
    if config.get("experiment") is not None:
        from experiments.config import experiment_routing, validate_plan
        plan = validate_plan(config["experiment"])
        if any(config[key] != value for key, value in experiment_routing(plan).items()):
            raise ValueError("Routing must match the fixed experiment plan")
    return config


def selected_slot(config, user_id):
    digest = hashlib.sha256(f"{config['salt']}:{user_id}".encode()).digest()
    bucket = int.from_bytes(digest[:8], "big") % 10000
    return other_slot(config["primary"]) if bucket < config["secondary_percent"] * 100 else config["primary"]


def write_json(path, value):
    """Readers see the complete old or new configuration, never a partial write."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as output:
        temporary = Path(output.name)
        json.dump(value, output, indent=2, allow_nan=False)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
