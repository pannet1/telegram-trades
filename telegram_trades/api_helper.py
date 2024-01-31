from typing import List, Dict

trade_keys = [
    "order_id",
    "exchange",
    "filled_timestamp",
    "filled_quantity",
    "exchange_order_id",
    "price",
    "side"
]
position_keys = [
    "symbol",
    "quantity",
    "product",
    "last_price",
    "exchange",
    "day_buy_value",
    "day_sell_value",
    "MtoM",
]
order_keys = [
    "symbol",
    "quantity",
    "side",
    "price",
    "trigger_price",
    "average_price",
    "filled_quantity",
    "exchange",
    "order_id",
    "broker_timestamp",
    "product",
    "order_type",
    "Status",
]


def filter_by_keys(keys: List, lst: List[Dict]) -> List[Dict]:
    new_lst = [{}]
    if lst and any(lst):
        for dct in lst:
            new_dct = {}
            for key in keys:
                if dct.get(key, None):
                    new_dct[key] = dct[key]
            new_lst.append(new_dct)
    return new_lst


def modify_order(order: Dict, updates: Dict) -> Dict:
    order["symbol"] = order["exchange"] + ":" + order["symbol"]
    order.update(updates)
    return order


if __name__ == "__main__":
    from constants import BRKR, UTIL
    from login import get_broker
    import pandas as pd
    from rich import print

    api = get_broker(BRKR)

    def positions(api):
        lst = filter_by_keys(position_keys, api.positions)
        return lst

    lst = positions(api)
    print(pd.DataFrame(lst).set_index("symbol"))

    def orders(api):
        lst = filter_by_keys(order_keys, api.orders)
        lst = [order for order in lst if order.get(
            'symbol', "") == "TRIDENT-EQ"]
        return lst

    lst = orders(api)
    print(pd.DataFrame(lst).set_index("order_id"))

    updates = {
        "order_type": "MKT",
    }

    """
    print(lst)
    for order in lst:
        UTIL.slp_til_nxt_sec()
        if any(order):
            resp = api.order_modify(**order)
            if not isinstance(resp, dict):
                print("something is wrong")
            elif isinstance(resp, dict) and resp.get("status", "Not_Ok") == "Not_Ok":
                print(resp)
    args = dict(
        symbol="NSE:TRIDENT",
        side="Sell",
        quantity=1,
        price=40.00,
        trigger_price=40.05,
        order_type="SL",
        product="MIS"
    )
    resp = api.order_place(**args)
    if resp and resp.get("NOrdNo", None):
        print(f"{resp['NOrdNo']} successfully placed")
    else:
        print(f"{resp=}")

    order_cancelled = api.order_cancel("24012400365338")
    print(f"{order_cancelled=}")
    """
    def order_modify(lst, order_id):
        order = {}
        for order in lst:
            if order.get("order_id", None) == order_id:
                return order

    order_id = "24012400368866"
    norder = order_modify(lst, order_id)
    if any(norder):
        print(norder)
        norder["symbol"] = norder["exchange"] + ":" + norder["symbol"]
        norder["quantity"] = 2
        norder["order_type"] = "MKT"
        norder.pop("price", None)
        norder.pop("trigger_price", None)
        modified_order = api.order_modify(**norder)
        print(f"{modified_order=}")
