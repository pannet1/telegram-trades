# pip install pya3
# https://github.com/jerokpradeep/pya3
from pya3 import *
from constants import BRKR
import logging
logging.basicConfig(level=logging.DEBUG)

def get_broker():
    """
        login and authenticate
        return broker object
    """
    brkr = Aliceblue(user_id=BRKR['username'],api_key=BRKR["api_key"])
    _ = brkr.get_session_id()
    print(brkr.get_profile())
    return brkr
