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
    "order_type"
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
    lst = filter_by_keys(order_keys, api.orders)
    print(lst)
    updates = {
        "order_type": "SL-L",
        "price": float("0.05"),
        "trigger_price": float("0.05"),
    }
    for order in lst:
        UTIL.slp_til_nxt_sec()
        if any(order):
            resp = api.order_modify(**order)
            if not isinstance(resp, dict):
                print("something is wrong")
            elif isinstance(resp, dict) and resp.get("status", "Not_Ok") == "Not_Ok":
                print(resp)
    print(pd.DataFrame(lst).set_index("order_id"))
    print(pd.DataFrame(api.positions).set_index("symbol"))
