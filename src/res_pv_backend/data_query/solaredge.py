import json
from pathlib import Path

import pandas as pd
import requests
from pandas.tseries.offsets import DateOffset

from res_pv_backend.data_query import utils

BASE_URL = "https://monitoringapi.solaredge.com"
INVERTER_MODE_MAP = {
    "OFF": 0,
    "SLEEPING": 1,
    "STARTING": 2,
    "MPPT": 3,
    "THROTTLED": 4,
    "SHUTTING_DOWN": 5,
    "FAULT": 6,
    "STANDBY": 7,
    "LOCKED_STDBY": 8,
    "LOCKED_FIRE_FIGHTERS": 9,
    "LOCKED_FORCE_SHUTDOWN": 10,
    "LOCKED_COMM_TIMEOUT": 11,
    "LOCKED_INV_TRIP": 12,
    "LOCKED_INV_ARC_DETECTED": 13,
    "LOCKED_DG": 14,
    "LOCKED_PHASE_BALANCER": 15,
    "LOCKED_PRE_COMMISSIONING": 16,
    "LOCKED_INTERNAL": 17,
}
OPERATION_MODE_MAP = {
    0: "On-grid",
    1: "Operating in off-grid mode using PV or battery",
    2: "Operating in off-grid mode when generator is present",
}
TZ_LOCAL = "America/Boise"


# List of available API endpoints:
#   - NOT USED: Site List (redundant because only one site)
#   - COMPLETE: Site Details
#   - COMPLETE: Site Data: Start and End Dates
#   - COMPLETE: Site Energy
#   - COMPLETE: Site Energy - Time Period
#   - COMPLETE: Site Power
#   - COMPLETE: Site Overview
#   - NOT USED: Site Power - Detailed (not needed because all power is exported)
#   - NOT USED: Site Energy - Detailed (not needed because all energy is exported)
#   - NOT USED: Site Power Flow (not needed because all power is exported)
#   - NOT USED: Storage Information (not needed because no storage)
#   - NOT USED: Site Image
#   - COMPLETE: Site Environmental Benefits
#   - NOT USED: Installer Logo Image
#   - NOT USED: Components List (redundant with Inventory)
#   - COMPLETE: INventory
#   - Inverter Technical Data
#   - NOT USED: Equipment Change Log
#   - NOT USED: Accounts List
#   - NOT USED: Meters Data (only one meter)
#   - NOT USED: Sensors List (no additional sensors)
#   - NOT USED: Sensors Data (no additional sensors)


def get_auth_data() -> tuple:
    auth_data = json.load(open(Path(__file__).parent.parent / "auth.json"))
    api_key = auth_data["api_key_solaredge"]
    site_id = auth_data["site_id_solaredge"]
    return api_key, site_id


def get_inverter_technical_data(startTime, endTime):
    api_key, site_id = get_auth_data()

    url = f"{BASE_URL}/equipment/{site_id}/740DB3E5-19/data"
    params = {
        "api_key": api_key,
        "startTime": startTime,
        "endTime": endTime,
    }
    response = requests.get(url, params=params)
    utils.check_response(response)

    data = response.json()["data"]["telemetries"]

    index = []
    data_formatted = []
    for record in data:
        # Remove nesting of L1Data data
        L1_Data_data = record.pop("L1Data")
        record.update(L1_Data_data)

        date = record.pop("date")
        index.append(date)
        data_formatted.append(record)

    df = pd.DataFrame(index=index, data=data_formatted)

    df.index = pd.to_datetime(df.index)
    df.index = df.index.tz_localize("America/Boise").tz_convert("UTC")

    # Remove anything later than now
    now_floor_15min = pd.Timestamp.utcnow().floor("15min")
    df = df[df.index < now_floor_15min]

    df.index = df.index.tz_localize(None)
    df.index.name = "timestamp"

    # Convert inverterMode to integer
    df["inverterMode"] = df["inverterMode"].map(INVERTER_MODE_MAP)

    return df


def get_site_data_period() -> dict:
    api_key, site_id = get_auth_data()

    url = f"{BASE_URL}/site/{site_id}/dataPeriod"
    params = {"api_key": api_key}
    response = requests.get(url, params=params)
    utils.check_response(response)

    data = response.json()["dataPeriod"]

    return data


def get_site_details() -> dict:
    api_key, site_id = get_auth_data()

    url = f"{BASE_URL}/site/{site_id}/details"
    params = {"api_key": api_key}
    response = requests.get(url, params=params)
    utils.check_response(response)

    data = response.json()["details"]
    records_to_drop = [
        "id",
        "name",
        "accountId",
        "status",
        "lastUpdateTime",
        "ptoDate",
        "notes",
        "location",
        "uris",
        "publicSettings",
    ]
    data = {k: v for k, v in data.items() if k not in records_to_drop}

    # Remove nesting of primaryModule data
    primary_module_data = data.pop("primaryModule")
    primary_module_data = {
        f"primaryModule_{k}": v for k, v in primary_module_data.items()
    }
    data.update(primary_module_data)

    return data


def get_site_energy(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """Get site energy.

    Args:
        start_date (str): Local start date in format YYYY-MM-DD (inclusive).
        end_date (str): Local end date in format YYYY-MM-DD (inclusive).

    Returns:
        pd.DataFrame: Site energy in kWh.
    """
    if start_date is None and end_date is None:
        end_date = pd.Timestamp.now(tz=TZ_LOCAL).floor("D") - DateOffset(days=1)
        start_date = end_date - DateOffset(days=6)
        end_date = end_date.strftime("%Y-%m-%d")
        start_date = start_date.strftime("%Y-%m-%d")

    api_key, site_id = get_auth_data()

    url = f"{BASE_URL}/site/{site_id}/energy"
    params = {
        "api_key": api_key,
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": "QUARTER_OF_AN_HOUR",
    }
    response = requests.get(url, params=params)
    utils.check_response(response)

    data = response.json()["energy"]
    unit = data["unit"]
    assert unit == "Wh", f"Unexpected unit: {unit}"
    data = data["values"]

    df = pd.DataFrame(data)

    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    df.index = df.index.tz_localize(TZ_LOCAL).tz_convert("UTC")

    # Remove anything later than now
    now_floor_15min = pd.Timestamp.utcnow().floor("15min")
    df = df[df.index < now_floor_15min]

    df.index = df.index.tz_localize(None)
    df.index.name = "timestamp"
    df = df.rename(columns={"value": "energy"})

    assert df.shape[1] == 1, "More than one column in Site Energy data"

    return df


def get_site_energy_time_period(start_date: str, end_date: str) -> pd.DataFrame:
    api_key, site_id = get_auth_data()

    url = f"{BASE_URL}/site/{site_id}/timeFrameEnergy"
    params = {
        "api_key": api_key,
        "startDate": start_date,
        "endDate": end_date,
    }
    response = requests.get(url, params=params)
    utils.check_response(response)

    data = response.json()["timeFrameEnergy"]
    unit = data["unit"]
    assert unit == "Wh", f"Unexpected unit: {unit}"
    data = data["energy"]

    # Convert to kWh
    data /= 1000

    return data


def get_site_environmental_benefits():
    api_key, site_id = get_auth_data()

    url = f"{BASE_URL}/site/{site_id}/envBenefits"
    params = {
        "api_key": api_key,
        "systemUnits": "Metrics",
    }
    response = requests.get(url, params=params)
    utils.check_response(response)

    unit = response.json()["envBenefits"]["gasEmissionSaved"]["units"]
    assert unit == "kg", f"Unexpected unit: {unit}"
    data = response.json()["envBenefits"]

    # Remove nesting of gasEmissionSaved data
    gas_emission_saved_data = data.pop("gasEmissionSaved")
    gas_emission_saved_data = {
        f"gasEmissionSaved_{k}": v
        for k, v in gas_emission_saved_data.items()
        if k != "units"
    }
    data.update(gas_emission_saved_data)

    return data


def get_site_inventory() -> dict:
    api_key, site_id = get_auth_data()

    url = f"{BASE_URL}/site/{site_id}/inventory"
    params = {"api_key": api_key}
    response = requests.get(url, params=params)
    utils.check_response(response)

    data = response.json()["Inventory"]

    data = {
        "inverter": data["inverters"][0],
        "meter": data["meters"][0],
    }

    return data


def get_site_overview() -> dict:
    api_key, site_id = get_auth_data()

    url = f"{BASE_URL}/site/{site_id}/overview"
    params = {"api_key": api_key}
    response = requests.get(url, params=params)
    utils.check_response(response)

    data = response.json()["overview"]

    return data


def get_site_power(start_time: str = None, end_time: str = None) -> pd.DataFrame:
    """Get site power.

    Args:
        start_time (str): Local start time in format YYYY-MM-DD HH:MM:SS (inclusive).
        end_time (str): Local end time in format YYYY-MM-DD HH:MM:SS (exclusive).

    Returns:
        pd.DataFrame: Site power in kW.
    """
    if start_time is None and end_time is None:
        end_time = pd.Timestamp.now(tz=TZ_LOCAL).floor("D")
        start_time = end_time - DateOffset(days=7)
        end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
        start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")

    api_key, site_id = get_auth_data()

    url = f"{BASE_URL}/site/{site_id}/power"
    params = {
        "api_key": api_key,
        "startTime": start_time,
        "endTime": end_time,
    }
    response = requests.get(url, params=params)
    utils.check_response(response)

    data = response.json()["power"]
    unit = data["unit"]
    assert unit == "W", f"Unexpected unit: {unit}"
    data = data["values"]

    df = pd.DataFrame(data)

    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    df.index = df.index.tz_localize(TZ_LOCAL).tz_convert("UTC")

    # Remove anything later than now
    now_floor_15min = pd.Timestamp.utcnow().floor("15min")
    df = df[df.index < now_floor_15min]

    df.index = df.index.tz_localize(None)
    df.index.name = "timestamp"
    df = df.rename(columns={"value": "power"})

    assert df.shape[1] == 1, "More than one column in Site Power data"

    return df
