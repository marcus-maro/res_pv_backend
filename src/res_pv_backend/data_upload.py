import logging
import os
from pathlib import Path
from traceback import format_exc

import pandas as pd
from gfuncs import drive, gmail
from pandas.tseries.offsets import DateOffset

from res_pv_backend.data_insert.data_insert import insert_df
from res_pv_backend.data_query import solaredge, solcast
from res_pv_backend.utils import send_sms

logging.basicConfig(
    filename=Path(__file__).parent / "data_upload.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s : %(message)s",
)

logging.info("=" * 15)
logging.info("Starting data upload script")


def upload_df(df, upload_path_parent):
    for date, df_date in df.groupby(df.index.date):
        year, month = date.year, date.month
        date_str = date.strftime("%Y-%m-%d")
        file_name = Path(f"{date_str}.csv")
        file_name_existing = Path(f"{date_str}_existing.csv")
        upload_path = upload_path_parent / str(year) / f"{month:02d}"

        if drive.file_exists(upload_path / file_name):
            drive.download_file(file_name_existing, upload_path / file_name)
            df_date_existing = pd.read_csv(
                file_name_existing, index_col=0, parse_dates=True
            )
            df_date_existing = df_date_existing.combine_first(df_date)
            df_date_existing.sort_index().to_csv(file_name)

            os.remove(file_name_existing)
        else:
            df_date.sort_index().to_csv(file_name)

        drive.upload_file(file_name, upload_path)

        print(upload_path / file_name)
        os.remove(file_name)


try:
    now_local = pd.Timestamp.now(tz=solaredge.TZ_LOCAL)
    end_time = now_local.floor("1D")
    start_time = end_time - DateOffset(days=3)
    end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
    start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")

    df_solcast = solcast.get_pv_estimate()
    df_solaredge_power = solaredge.get_site_power()
    df_solaredge_energy = solaredge.get_site_energy()
    df_solaredge_inv_tech = solaredge.get_inverter_technical_data(start_time, end_time)

    insert_df(df_solcast)
    insert_df(df_solaredge_power)
    insert_df(df_solaredge_energy)

    upload_path_parent_solcast = Path("res_pv/data/solcast")
    upload_path_parent_solaredge_power = Path("res_pv/data/solaredge/site_power")
    upload_path_parent_solaredge_energy = Path("res_pv/data/solaredge/site_energy")

    upload_df(df_solcast, upload_path_parent_solcast)
    upload_df(df_solaredge_power, upload_path_parent_solaredge_power)
    upload_df(df_solaredge_energy, upload_path_parent_solaredge_energy)

    inv_tech_index_seconds = sorted(list(df_solaredge_inv_tech.index.second.unique()))
    if inv_tech_index_seconds != [0, 59]:
        raise ValueError("Unexpected inv_tech_index_seconds")
    else:
        df_solaredge_inv_tech.index = df_solaredge_inv_tech.index.round("1T")
        insert_df(df_solaredge_inv_tech)
        upload_parent_path_solaredge_inv_tech = Path(
            "res_pv/data/solaredge/inverter_technical_data"
        )
        upload_df(df_solaredge_inv_tech, upload_parent_path_solaredge_inv_tech)

except Exception as e:
    tb = format_exc()
    logging.error(tb)
    try:
        gmail.send_email(
            subject="Error during data upload",
            body=tb,
        )
    except Exception as e:
        logging.error("Error sending email")
        logging.error(format_exc())
        try:
            send_sms("res_pv_backend: Error during data upload")
        except Exception as e:
            logging.error("Error sending SMS")
            logging.error(format_exc())

logging.info("Finished data upload script")
