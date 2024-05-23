# https://github.com/jerokpradeep/pya3
from numpy import argsort
from omspy_brokers.alice_blue import AliceBlue
from constants import logging

dct = dict(
    side="B", order_type="SL", price=57000.05, trigger_price=57000.0, product="MIS"
)


def get_broker(BRKR):
    """
    login and authenticate
    return broker object
    """
    api = AliceBlue(user_id=BRKR["username"], api_key=BRKR["api_secret"])
    if api.authenticate():
        # get attributes of api object
        logging.debug(f"Authenticated with token {vars(api)}")
    else:
        logging.error("Authentication failed")
        __import__("sys").exit(1)
    return api


def place(api, args):
    args = {**args, **dct}
    print(args)
    resp = api.order_place(**args)
    print(resp)


def modify(api, args):
    print(args)
    resp = api.order_modify(**args)
    print(resp)


if __name__ == "__main__":
    from constants import BRKR, DATA
    import pandas as pd

    api = get_broker(BRKR)

    """
    # args = dict(symbol="NFO:INFY30MAY24C1460", quantity=400, remarks="testing")
    args = dict(symbol="BFO:BANKEX24MAYFUT", quantity=15, remarks="testing")
    place(api, args)
    """
    args = {
        "symbol": "BFO:BANKEX24MAYFUT",
        "order_id": "24052300372082",
        "order_type": "MKT",
        "quantity": 15,
        "side": "B",
        "product": "MIS",
    }
    resp = modify(api, args)

    resp = api.orders
    pd.DataFrame(resp).to_csv(DATA + "orders.csv", index=False)

    resp = api.positions
    pd.DataFrame(resp).to_csv(DATA + "positions.csv", index=False)
