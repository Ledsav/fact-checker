from pathlib import Path


def get_project_root() -> Path:
    return Path(__file__).parent.parent


def get_datasets_dir(path: str) -> Path:
    return get_project_root() / "datasets" / path


def get_firebase_key_path() -> Path:
    return get_project_root() / "key_firebase.json"
