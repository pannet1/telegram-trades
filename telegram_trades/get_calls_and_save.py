from constants import TGRM
from dataclasses import dataclass
from typing import Dict
from login import get_broker


def save_entry(order):
    pass


@dataclass
class Order:
    symbol: str
    quantity: int
    side: str
    order_type: str
    product_type: str
    status: str

    def __post__init(self):
        # create order dictionary and save it to db
        pass


def read_calls() -> Dict:
    """
        reads calls from telegram
        yields latest message
    """
    pass


def classify_call(call: Dict) -> Dict:
    """
        classify call
        return order
    """
    pass


def order_place(brkr, dct_order):
    """
        place order
    """
    brkr.order_place(**dct_order)


brkr = get_broker()
while True:
    call = read_calls()
    dct_order = classify_call(call)
    order_place(brkr, dct_order)
    if any(dct_order):
        # creater an order object that stores order
        Order(**dct_order)
