from constants import TGRM
from dataclasses import dataclass
from typing import Dict


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


while True:
    call = read_calls()
    dct_order = classify_call(call)
    if any(dct_order):
        # creater an order object that stores order
        Order(**dct_order)
