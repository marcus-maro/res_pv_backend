import numpy as np
import psycopg

from res_pv_backend.utils import get_auth


def get_name_raw_sensor_id_map():
    auth_data = get_auth()
    db_connection = auth_data["db_connection"]

    with psycopg.connect(db_connection) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name_raw, sensor_id FROM sensors")

            sensor_metadata = {}
            for row in cur.fetchall():
                name_raw, sensor_id = row
                sensor_metadata[name_raw] = sensor_id

            return sensor_metadata


def get_name_raw_unit_scaling_map():
    auth_data = get_auth()
    db_connection = auth_data["db_connection"]

    with psycopg.connect(db_connection) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name_raw, unit_scaling FROM sensors")

            sensor_metadata = {}
            for row in cur.fetchall():
                name_raw, unit_scaling = row
                if np.isnan(unit_scaling):
                    unit_scaling = 1

                sensor_metadata[name_raw] = unit_scaling

            return sensor_metadata
