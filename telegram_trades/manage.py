from toolkit.display import Regative
from login import get_broker
from constants import BRKR
from rich import print

db_conn = "db connection object"


def display():
    pass


def load_orders(db_conn):
    """
        load orders table and 
        returns order details
    """
    pass


def get_filled_order(dct_orders):
    """
        check if entries are filled
        without corresponding exit order
    """


def exit_order(api, order):
    """
        place oco order for exit
    """


def remove_order(api, order):
    """
        remove orders from table
        if the target or stop loss is hit
    """


api = get_broker(BRKR)
dump = api.broker.get_contract_master("NFO")
print(type(dump))
while True:
    dct_orders = load_orders(db_conn)
    dct_entry = get_filled_order(dct_orders)
    exit_order(api, dct_entry)
    dct_orders = load_orders(api)
    remove_order(api, dct_orders)
    print(dct_orders)
