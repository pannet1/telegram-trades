from constants import DIRP, BRKR, FUTL, UTIL, logging
from login import get_broker
from rich import print
import os


api = get_broker(BRKR)
exchanges = ["NFO", "BFO"]
for exchange in exchanges:
    if FUTL.is_file_not_2day(f"./{exchange}.csv"):
        api.broker.get_contract_master(exchange)
        try:
            os.rename(f"./{exchange}.csv", f"{DIRP}{exchange}.csv")
        except Exception as e:
            logging.warning(e)


def str_to_callable(str_func: str, task=None):
    fn = eval(str_func)
    if isinstance(task, dict):
        return fn(**task)
    else:
        return fn()


def entry(**task):
    """
    place order on broker terminal
    input: task details
    """
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
    tasks = FUTL.json_fm_file("fake_tasks")
    logging.debug(f'Updating task: {updated_task}')
    [task.update(updated_task)
     for task in tasks if task["id"] == updated_task["id"]]
    FUTL.save_file(tasks, "./fake_tasks")


while True:
    tasks = read_tasks()
    UTIL.slp_til_nxt_sec()
    for task in tasks:
        fn = task.pop("fn", None)
        print(task)
        str_to_callable(fn, task)
