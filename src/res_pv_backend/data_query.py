import json
from pathlib import Path

import pandas as pd
import requests

URL_SOLCAST = "https://api.solcast.com.au"


def query_solcast():
    auth_data = json.load(open(Path(__file__).parent / "auth.json"))
    site_id_solcast = auth_data["site_id_solcast"]
    api_key_solcast = auth_data["api_key_solcast"]

    url = f"{URL_SOLCAST}/rooftop_sites/{site_id_solcast}/estimated_actuals"
    params = {
        "format": "json",
        "api_key": api_key_solcast,
        "hours": 168,
    }

    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(
            f"Error querying Solcast: {response.status_code}, {response.text}"
        )

    json_data = response.json()

    df = pd.DataFrame(json_data["estimated_actuals"])
    df["period_end"] = pd.to_datetime(df["period_end"])
    df = df.set_index("period_end")
    df.index = df.index.tz_localize(None)
    df = df.drop(columns=["period"], errors="ignore")

    return df
