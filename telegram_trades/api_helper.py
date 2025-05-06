from typing import List, Dict
from constants import FUTL, UTIL, logging

SECS = 0.5

trade_keys = [
    "order_id",
    "exchange",
    "filled_timestamp",
    "filled_quantity",
    "exchange_order_id",
    "price",
    "side",
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
    "remarks",
    "order_type",
    "Status",
]


def update_lst_of_dct_with_vals(lst_of_dct, key, **kwargs):
    updated = False
    for dct in lst_of_dct:
        if dct.get(key, None) == kwargs[key]:
            dct.update(**kwargs)
            updated = True
            break
    if not updated:
        lst_of_dct.append(kwargs)
    return lst_of_dct


def filter_by_keys(keys: List, lst: List[Dict]) -> List[Dict]:
    new_lst = []
    if lst and isinstance(lst, list) and any(lst):
        for dct in lst:
            new_dct = {}
            for key in keys:
                if dct.get(key, None):
                    new_dct[key] = dct[key]
            new_lst.append(new_dct)
    return new_lst


def filtered_positions(api):
    UTIL.slp_for(SECS)
    lst = filter_by_keys(position_keys, api.positions)
    return lst


def filtered_orders(api):
    UTIL.slp_for(SECS)
    # print(lst[0].keys())
    lst = filter_by_keys(order_keys, api.orders)
    return lst


def get_order_from_book(api, order_id_or_resp):
    dct_order = {"Status": "E-ORDERBOOK"}
    if isinstance(order_id_or_resp, dict):
        order_1 = order_id_or_resp.get("NOrdNo", None)
        order_2 = order_id_or_resp.get("nestOrderNumber", None)
        order_id_or_resp = order_1 if order_1 else order_2
    if order_id_or_resp is not None:
        lst = filtered_orders(api)
        if any(lst):
            lst_order = [
                order for order in lst if order["order_id"] == order_id_or_resp
            ]
            if any(lst_order):
                temp = lst_order[0]
                dct_order = temp if any(temp) else dct_order
    dct_order["order_id"] = order_id_or_resp
    return dct_order


def square_off(api, order_id, symbol, quantity):
    args = dict(
        order_id=order_id,
        symbol=symbol,
        side="S",
        quantity=quantity,
        price=0.0,
        order_type="MKT",
        product="N",
    )
    resp = api.order_modify(**args)
    logging.info(f"modify SL {args} to tgt got {resp}")


def modify_order(api, order: Dict) -> Dict:
    args = dict(
        order_id=order["order_id"],
        symbol=order["symbol"],
        side=order["side"],
        quantity=order["quantity"],
        price=order["price"],
        trigger_price=order["trigger_price"],
        order_type=order["order_type"],
        product=order["product"],
    )
    resp = api.order_modify(**args)
    return resp


def market_order(api, order, action: str):
    """
    input:
        api: broker object
        order: order details
        action: condition for param switch
    output:
        resp
    """
    if action == "opposite":
        side = "S"
        price = 0.0
        trigger_price = 0.0
        order_type = "MKT"

    args = dict(
        side=side,
        price=price,
        trigger_price=trigger_price,
        order_type=order_type,
        symbol=order["exchange"] + ":" + order["symbol"],
        quantity=order["quantity"],
        product="N",
        remarks=order["remarks"],
    )
    resp = api.order_place(**args)
    logging.info(f"order {action} {args} got {resp}")
    return resp


def download_masters(broker):
    exchanges = ["NFO", "BFO"]
    for exchange in exchanges:
        if FUTL.is_file_not_2day(f"./{exchange}.csv"):
            broker.get_contract_master(exchange)
            UTIL.slp_for(SECS)


def get_ltp(broker, exchange, symbol):
    obj_inst = broker.get_instrument_by_symbol(exchange, symbol)
    if obj_inst is not None and isinstance(obj_inst, dict):
        UTIL.slp_for(SECS)
        return float(broker.get_scrip_info(obj_inst.get(["Ltp"], 0.0)))


if __name__ == "__main__":
    from constants import BRKR, DATA
    from login import get_broker
    import pandas as pd
    from rich import print

    api = get_broker(BRKR)

    ttl = 0
    lst = filtered_positions(api)
    if any(lst):
        pos = pd.DataFrame(lst).set_index("symbol")
        print(pos)
        pos.to_csv(DATA + "pos.csv")
        for item in lst:
            ttl += float(item["MtoM"])
        print(f"{ttl=}")

    lst = filtered_orders(api, None)
    if any(lst):
        ord = pd.DataFrame(lst).set_index("order_id")
        print(ord)
        ord.to_csv(DATA + "ord.csv")
