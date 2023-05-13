import json
from pathlib import Path

import pandas as pd
import requests

from res_pv_backend.data_query import utils

BASE_URL = "https://api.solcast.com.au"


def get_pv_estimate() -> pd.DataFrame:
    auth_data = json.load(open(Path(__file__).parent.parent / "auth.json"))
    api_key = auth_data["api_key_solcast"]
    site_id = auth_data["site_id_solcast"]

    url = f"{BASE_URL}/rooftop_sites/{site_id}/estimated_actuals"
    params = {
        "api_key": api_key,
        "format": "json",
        "hours": 168,
    }
    response = requests.get(url, params=params)
    utils.check_response(response)

    data = response.json()

    df = pd.DataFrame(data["estimated_actuals"])
    df["period_end"] = pd.to_datetime(df["period_end"])
    df = df.set_index("period_end")
    df = df.sort_index()

    # Subtract 30 minutes from index to make interval beginning
    df.index = df.index - pd.Timedelta(minutes=30)
    df.index.name = "timestamp"

    df.index = df.index.tz_localize(None)
    df = df.drop(columns=["period"], errors="ignore")

    assert df.shape[1] == 1, "More than one column in Solcast data"

    return df
