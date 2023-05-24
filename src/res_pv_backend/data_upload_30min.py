import logging
from pathlib import Path
from traceback import format_exc

import pandas as pd
from gfuncs import gmail
from pandas.tseries.offsets import DateOffset

from res_pv_backend.data_insert.data_insert import insert_df
from res_pv_backend.data_query import solaredge
from res_pv_backend.utils import send_sms

logging.basicConfig(
    filename=Path(__file__).parent / "data_upload.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s : %(message)s",
)

logging.info("=" * 15)
logging.info("Starting hourly data upload script")


try:
    now_local = pd.Timestamp.now(tz=solaredge.TZ_LOCAL)
    end_time = now_local.ceil("15T")
    start_time = end_time - DateOffset(minutes=90)
    end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
    start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")

    df_solaredge_power = solaredge.get_site_power(start_time, end_time)
    insert_df(df_solaredge_power)

except Exception as e:
    tb = format_exc()
    logging.error(tb)
    try:
        gmail.send_email(
            subject="Error during hourly data upload",
            body=tb,
        )
    except Exception as e:
        logging.error("Error sending email")
        logging.error(format_exc())
        try:
            send_sms("res_pv_backend: Error during hourly data upload")
        except Exception as e:
            logging.error("Error sending SMS")
            logging.error(format_exc())

logging.info("Finished hourly data upload script")
