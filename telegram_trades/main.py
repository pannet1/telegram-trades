from constants import DIRP, BRKR, FUTL, UTIL, logging
from login import get_broker
from rich import print
import os
import traceback
import pandas as pd


def download_masters(broker):
    exchanges = ["NFO", "BFO"]
    for exchange in exchanges:
        if FUTL.is_file_not_2day(f"{DIRP}{exchange}.csv"):
            broker.get_contract_master(exchange)
            try:
                os.rename(f"./{exchange}.csv", f"{DIRP}{exchange}.csv")
            except Exception as e:
                logging.warning(e)


def str_to_callable(fn: str, task=None):
    """
    converts string to callable function
    does not work if imported
    """
    func = eval(fn)
    if isinstance(task, dict):
        return func(**task)
    return func()


def entry(**task):
    """
    place order on broker terminal
    input: task details
    {'symbol': 'BANKNIFTY 47300 CE', 
    'ltp_range': '570', 
    'target_range': '30 | 70 | 100 | 200 | 250', 
    'sl': '540 | 570 | 610'}
    """
    obj_inst = task["api"]().broker.get_instrumemnt_by_symbol("NSE", "SBIN")
    info = task["api"]().broker.scripinfo(obj_inst)
    print(info)
    args = dict(
        symbol=task["symbol"],
        side=task["side"],
    )
    task["api"]().order_place(**args)
    task["fn"] = "is_entry"
    update_task(task)


def is_entry(**task):
    task["fn"] = "stop1"
    update_task(task)


def stop1(**task):
    task["fn"] = "target1"
    update_task(task)


def target1(**task):
    task["fn"] = "is_target1"
    update_task(task)


def is_target1(**task):
    task["fn"] = "is_stop2"
    update_task(task)


def is_stop2(**task):
    task["fn"] = "is_target2"
    update_task(task)


def is_target2(**task):
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
                str_to_callable(fn, task)
    except Exception as e:
        logging.error(e)
        print(traceback.print_exc())


run()
