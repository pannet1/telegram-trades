# https://github.com/jerokpradeep/pya3
import logging
from omspy_brokers.alice_blue import AliceBlue


logging.basicConfig(level=logging.INFO)


def get_broker(BRKR):
    """
        login and authenticate
        return broker object
    """
    api = AliceBlue(user_id=BRKR['username'], api_key=BRKR["api_secret"])
    if api and api.authenticate():
        print(f"Authenticated with token {api.token}")
        return api
    else:
        print("Failed to authenticate")
