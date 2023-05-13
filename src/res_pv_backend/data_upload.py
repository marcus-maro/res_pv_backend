import logging
import os
from pathlib import Path
from traceback import format_exc

import pandas as pd
from gfuncs import drive, gmail
from pandas.tseries.offsets import DateOffset

from res_pv_backend.data_query import solaredge, solcast

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
    df_solcast = solcast.get_pv_estimate()
    upload_path_parent_solcast = Path("res_pv/data/solcast")
    upload_df(df_solcast, upload_path_parent_solcast)

    df_solaredge_power = solaredge.get_site_power()
    upload_path_parent_solaredge = Path("res_pv/data/solaredge/site_power")
    upload_df(df_solaredge_power, upload_path_parent_solaredge)

    df_solaredge_energy = solaredge.get_site_energy()
    upload_path_parent_solaredge = Path("res_pv/data/solaredge/site_energy")
    upload_df(df_solaredge_energy, upload_path_parent_solaredge)

except Exception as e:
    tb = format_exc()
    logging.error(tb)
    gmail.send_email(
        subject="Error during data upload",
        body=tb,
    )

logging.info("Finished data upload script")
