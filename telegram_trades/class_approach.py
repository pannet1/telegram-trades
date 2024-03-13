from dataclasses import dataclass
from time import sleep


@dataclass
class Order:
    id: int
    product: str
    price: float
    trigger_price: float
    order_type: str
    symbol: str
    status: str
    quantity: int
    tag: str


class Orders:
    def __init__(self):
        self.book = []

    def update(self, order):
        ids = [o.id for o in self.book]
        if order.id in ids:
            for o in self.book:
                if o.id == order.id:
                    o = order
        else:
            self.book.append(order)

    def read(self):
        pass


class ApiOrders:
    def __init__(self, api):
        self.api = api

    def ltp(self, exchange, symbol):
        return self.api.broker.get_ltp(exchange, symbol)

    def buy_within_range(self, **args):
        pass

    def sell_outside_range(self, **args):
        pass


ord = Order(1, 'BTC', 100, 0, 'LIMIT', 'BTC', 'NEW', 1, 'test')
print(ord)
order_book = Orders()
order_book.update(ord)
print(order_book.book)
while True:
    ord.quantity = ord.quantity - 1
    order_book.update(ord)
    print(order_book.book)
    sleep(5)
