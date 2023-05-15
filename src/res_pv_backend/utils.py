import json
from pathlib import Path


def get_auth() -> dict:
    return json.load(open(Path(__file__).parent / "auth.json"))
