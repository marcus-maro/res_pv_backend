import pandas as pd
import psycopg
from res_pv_backend.db_query import db_query
from res_pv_backend.utils import get_auth


def insert_df(df_in: pd.DataFrame) -> None:
    df = df_in.copy()

    sensor_id_map = db_query.get_name_raw_sensor_id_map()
    unit_scaling_map = db_query.get_name_raw_unit_scaling_map()

    # Convert units
    for col in df:
        unit_scaling = unit_scaling_map[col]
        df[col] = df[col] * unit_scaling

    # Reshape DataFrame to have 3 columns, timestamp, data, column name
    df = df.unstack().reset_index().dropna()
    df.columns = ["name_raw", "timestamp", "value"]
    df["sensor_id"] = df["name_raw"].map(sensor_id_map)
    df = df.drop(columns=["name_raw"])
    df["value"] = df["value"].round(3)

    auth_data = get_auth()
    db_connection = auth_data["db_connection"]

    with psycopg.connect(db_connection) as conn:
        with conn.cursor() as cur:
            data = [
                (row["timestamp"], row["sensor_id"], row["value"])
                for _, row in df.iterrows()
            ]
            query = """
            INSERT INTO sensor_data (timestamp, sensor_id, value)
            VALUES (%s, %s, %s)
            ON CONFLICT (timestamp, sensor_id) DO UPDATE SET value = EXCLUDED.value;
            """
            cur.executemany(query, data)
