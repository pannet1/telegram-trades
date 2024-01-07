from login import get_broker

db_conn = "db connection object"


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


def exit_order(brkr, order):
    """
        place oco order for exit
    """


def remove_order(brkr, order):
    """
        remove orders from table
        if the target or stop loss is hit
    """


brkr = get_broker()
while True:
    dct_orders = load_orders(db_conn)
    dct_entry = get_filled_order(dct_orders)
    exit_order(brkr, dct_entry)
    dct_orders = load_orders(brkr)
    remove_order(brkr, dct_orders)
    print(dct_orders)
