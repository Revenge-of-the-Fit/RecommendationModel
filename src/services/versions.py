import hashlib
from pathlib import Path


def file_version(path: Path) -> str:
    with path.open("rb") as source:
        return "sha256:" + hashlib.file_digest(source, "sha256").hexdigest()


def code_version(directory: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(directory.rglob("*.py")):
        digest.update(path.relative_to(directory).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes().replace(b"\r\n", b"\n"))
        digest.update(b"\0")
    return "sha256:" + digest.hexdigest()


def dataset_version(directory: Path) -> str:
    digest = hashlib.sha256()
    for filename in ("events.csv.gz", "movies.csv.gz", "users.csv.gz"):
        digest.update(filename.encode("utf-8"))
        digest.update(file_version(directory / filename).encode("ascii"))
    return "sha256:" + digest.hexdigest()
