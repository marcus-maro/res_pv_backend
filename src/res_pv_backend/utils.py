import json
from pathlib import Path

from twilio.rest import Client


def get_auth() -> dict:
    return json.load(open(Path(__file__).parent / "auth.json"))


def send_sms(message: str) -> None:
    auth_data = get_auth()
    
    account_sid = auth_data["twilio_account_sid"]
    auth_token = auth_data["twilio_auth_token"]
    from_ = auth_data["twilio_number"]
    to = auth_data["twilio_my_number"]
    client = Client(account_sid, auth_token)

    message = client.messages.create(
        body=message,
        from_=from_,
        to=to,
    )
