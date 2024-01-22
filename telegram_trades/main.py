from constants import DIRP, BRKR, FUTL, UTIL, logging
from login import get_broker
from rich import print
import traceback
import pandas as pd


def download_masters(broker):
    exchanges = ["NFO", "BFO"]
    for exchange in exchanges:
        if FUTL.is_file_not_2day(f"./{exchange}.csv"):
            broker.get_contract_master(exchange)


def str_to_callable(fn: str, task=None):
    """
    converts string to callable function
    does not work if imported
    """
    func = eval(fn)
    if isinstance(task, dict):
        return func(**task)
    return func()


def get_ltp(broker, exchange, symbol):
    obj_inst = broker.get_instrument_by_symbol(exchange, symbol)
    return float(broker.get_scrip_info(obj_inst)["Ltp"])


def entry(**task):
    """
    place order on broker terminal
    input: task details
    {'symbol': 'BANKNIFTY 47300 CE',
    'ltp_range': '570',
    'target_range': '30 | 70 | 100 | 200 | 250',
    'sl': '540 | 570 | 610'}
    """
    lst_symbol = task["symbol"].split(":")
    last_price = get_ltp(task["api"].broker, lst_symbol[0], lst_symbol[1])
    lst_ltp = list(map(float, task["ltp_range"].split("|")))
    if task["side"][0].upper() == "B":
        if max(lst_ltp) < last_price:
            price = max(lst_ltp)
            order_type = "LMT"
        elif min(lst_ltp) > last_price:
            price = min(lst_ltp)
            trigger_price = price - 0.05
            order_type = "SL-L"
    else:
        if min(lst_ltp) > last_price:
            price = min(lst_ltp)
            order_type = "LMT"
        elif max(lst_ltp) < last_price:
            price = max(lst_ltp)
            trigger_price = price + 0.05
            order_type = "SL-L"
    args = dict(
        symbol=task["symbol"],
        side=task["side"],
        quantity=int(task["quantity"]),
        price=price,
        order_type=order_type,
        product=task["product"]
    )
    if "trigger_price" in locals() and trigger_price is not None:
        args["trigger_price"] = trigger_price
    resp = task["api"].order_place(**args)
    if resp and resp.get("order_id", None):
        task["orders"] = [resp]
        task["fn"] = "is_entry"
        update_task(task)


def is_entry(**task):
    lst_order_ids = [order["order_id"] for order in task["orders"]]
    lst_orders = [order for order in task["api"].orders if any(order)]
    if any(lst_order_ids) and all(order["status"] == "COMPLETE"
                                  for order in lst_orders
                                  if order["order_id"] in lst_order_ids):
        task["fn"] = "stop1"
        update_task(task)


def stop1(**task):
    task["fn"] = "is_stop1_or_target1"
    update_task(task)


def is_stop1_or_target1(**task):
    ltp = get_ltp(task["api"].broker, task["symbol"].split(
        ":")[0], task["symbol"].split(":")[1])
    is_true = (task['side'].upper() == "B" and ltp < float(task["sl"])) or (
        task['side'].upper() != "B" and ltp > float(task["sl"]))
    if is_true:
        task["fn"] = "modify_stop1_to_target1"
        update_task(task)


def modify_stop1_to_target1(**task):
    task["fn"] = "trail_stop1_to_entry"
    update_task(task)


def trail_stop1_to_cost(**task):
    task["fn"] = "is_cost_or_target2"
    update_task(task)


def is_cost_or_target2(**task):
    task["fn"] = "COMPLETE"
    update_task(task)


def read_tasks():
    return FUTL.json_fm_file("fake_tasks")


def update_task(updated_task):
    updated_task.pop("api", None)
    logging.debug(f'Updating task: {updated_task}')
    tasks = FUTL.json_fm_file("fake_tasks")
    [task.update(updated_task)
     for task in tasks if task["id"] == updated_task["id"]]
    FUTL.save_file(tasks, "./fake_tasks")


def run():
    lst_ignore = ["COMPLETED", "CANCELED"]
    try:
        api = get_broker(BRKR)
        download_masters(api.broker)
        while True:
            tasks = read_tasks()
            print(pd.DataFrame(tasks).set_index("id"))
            UTIL.slp_til_nxt_sec()
            for task in tasks:
                task["api"] = api
                fn: str = task.pop("fn")
                if fn not in lst_ignore:
                    str_to_callable(fn, task)
    except Exception as e:
        logging.error(e)
        print(traceback.print_exc())


run()
