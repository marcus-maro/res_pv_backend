import logging

import requests


def check_response(response: requests.Response):
    if response.status_code != 200:
        error_message = (
            f"Error querying {response.url}: {response.status_code}, {response.text}"
        )
        logging.error(error_message)
        raise Exception(error_message)
