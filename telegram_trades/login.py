# https://github.com/jerokpradeep/pya3
from omspy_brokers.alice_blue import AliceBlue
from constants import logging


def get_broker(BRKR):
    """
        login and authenticate
        return broker object
    """
    api = AliceBlue(user_id=BRKR['username'], api_key=BRKR["api_secret"])
    if api.authenticate():
        # get attributes of api object
        logging.debug(f"Authenticated with token {vars(api)}")
    else:
        logging.error("Authentication failed")
        __import__("sys").exit(1)
    return api
