from constants import DATA, BRKR, FUTL, UTIL, logging
from login import get_broker
from api_helper import filtered_orders, download_masters, get_ltp
from api_helper import update_lst_of_dct_with_vals
from rich import print
import traceback
import pandas as pd
from typing import Dict, List
import pendulum
import inspect

lst_ignore = ["unknown", "rejected", "cancelled", "t1_reached"]
F_SIGNAL = DATA + "signals.csv"
F_TASK = DATA + "tasks.json"
SECS = 1  # sleep time


"""
    helpers
"""


def log_exception(exception, locals, error_message=None):
    """Logs an exception with detailed information.

    Args:
        exception (Exception): The exception object itself.
        method_name (str): The name of the method where the exception occurred.
        params (dict, tuple, or list): The parameters passed to the method.
        error_message (str, optional): An optional additional message to log with the exception.
    """
    method_name = inspect.stack(
    )[1].function  # Get the name of the calling method

    # Format the message
    message = f"""
        Method Name: {method_name}
        Type: {type(exception).__name__}
        Parameters: {locals}
        Error message: {exception}
        {error_message if error_message else ''}
    """
    # Log the exception with appropriate level
    logging.error(message, exc_info=True)


"""
    string functions
"""


def trail_stop1_to_cost(**task):
    task["fn"] = "is_cost_or_target2"
    update_task(task)


def is_cost_or_target2(**task):
    task["fn"] = "COMPLETE"
    update_task(task)

    """
    tasks
    """


class Jsondb:
    def __init__(self, api) -> None:
        self.api = api
        # input file
        if FUTL.is_file_not_2day(F_SIGNAL):
            # return empty list if file is not modified today
            FUTL.write_file(filepath=F_TASK, content=[])
        # initate output task json file
        if FUTL.is_file_not_2day(F_TASK):
            FUTL.write_file(filepath=F_TASK, content=[])
        # output file
        self.tasks = FUTL.read_file(F_TASK)
        self.is_dirty = False
        self.lastsync = pendulum.now()

    def _str_to_func(self, fn: str, task):
        """
        converts string to callable function
        does not work if imported
        """
        # Check if the string represents a valid method name in the class
        if hasattr(self, fn):
            # Get the method corresponding to the provided string
            method = getattr(self, fn)
            # Call the method with the provided arguments and keyword arguments
            return method(**task)
        else:
            # Raise an error if the method does not exist
            raise AttributeError(
                f"{fn} is not a valid method in this class")

    def _task_to_order_args(self, **task):
        try:
            args = {}
            trigger_price = 0
            order_type = "LMT"
            lst_symbol = task["symbol"].split(":")
            last_price = get_ltp(self.api.broker,
                                 lst_symbol[0], lst_symbol[1])
            lst_ltp = list(map(float, task["entry_range"].split("|")))
            if (side := task["action"][0].upper()) == "B":
                if (price := min(lst_ltp)) > last_price:
                    trigger_price = price - 0.05
                    order_type = "SL-L"
            elif (side := task["action"][0].upper()) == "S":
                if (price := max(lst_ltp)) < last_price:
                    trigger_price = price + 0.05
                    order_type = "SL-L"

            if side in ["B", "S"]:
                args = dict(
                    symbol=task["symbol"],
                    side=side,
                    quantity=int(task["quantity"]),
                    price=price,
                    order_type=order_type,
                    product="N",
                    remarks="entry"
                )
                if trigger_price > 0:
                    args["trigger_price"] = float(trigger_price)
        except Exception as e:
            log_exception(e, locals())
            traceback.print_exc()
        finally:
            return args

    def _order_status(self, resp):
        try:
            dct_order = {"Status": "unknown"}
            if order_id := resp.get("NOrdNo", None):
                dct_order = filtered_orders(self.api, order_id)
                if any(dct_order) and \
                        dct_order["Status"] not in lst_ignore:
                    return dct_order
        except Exception as e:
            log_exception(e, locals())
            traceback.print_exc()
        finally:
            return dct_order

    def entry(self, **task):
        """
        place order on broker terminal
        input: task details
        {'symbol': 'BANKNIFTY 47300 CE',
        'ltp_range': '570',
        'target_range': '30 | 70 | 100 | 200 | 250',
        'sl': '540 | 570 | 610',
        'action': 'Buy',}
        """
        try:
            task.pop("fn")
            args = self._task_to_order_args(**task)
            if any(args):
                logging.debug(f"entry args: {args}")
                resp = self.api.order_place(**args)
                order_details = self._order_status(resp)
                if order_details["Status"] != "unknown":
                    task["entry"] = order_details
                    task["fn"] = "is_entry"
        except Exception as e:
            log_exception(e, locals())
            traceback.print_exc()
        finally:
            self._update(task)

    def is_entry(self, **task):
        try:
            que = task["entry"]
            que = filtered_orders(self.api, que["order_id"])
            if que["Status"] == "complete":
                task["fn"] = "stop1"
        except Exception as e:
            log_exception(e, locals())
            traceback.print_exc()
        finally:
            self._update(task)

    def stop1(self, **task):
        try:
            task['fn'] = "unknown"
            que = task["entry"]
            if que["side"] == "B":
                args = dict(
                    symbol=task["symbol"],
                    side="S",
                    quantity=que["quantity"],
                    price=float(task["sl"]),
                    order_type="SL-M",
                    product="N",
                    remarks="exit"
                )
                resp = self.api.order_place(**args)
                order_details = self._order_status(resp)
                if order_details["Status"] != "unknown":
                    task["stop"] = order_details
                    task['fn'] = "is_stop_or_target1"
        except Exception as e:
            log_exception(e, locals())
            traceback.print_exc()
        finally:
            self._update(task)

    def is_stop_or_target1(self, **task):
        try:
            que = task["stop"]
            que = filtered_orders(self.api, que["order_id"])
            if que["Status"] == "complete":
                task["fn"] = "STOPPED-OUT"
            else:
                t1 = float(task["target_range"].split("|")[0])
                ltp = get_ltp(task["api"].broker, task["symbol"].split(
                    ":")[0], task["symbol"].split(":")[1])
                is_true = (task['side'].upper() == "B" and ltp < float(task["sl"])) or (
                    task['side'].upper() != "B" and ltp > float(task["sl"]))
                if is_true:
                    raise Exception("Market jumped the Stop Los")
                elif (
                    (task['side'].upper() == "B" and ltp > t1) or
                    (task['side'].upper() == "S" and ltp < t1)
                ):
                    args = dict(
                        symbol=task["symbol"],
                        side="S",
                        # TODO reduce quantity
                        quantity=que["quantity"],
                        price=0,
                        order_type="MKT",
                        product="N",
                        remarks="exit"
                    )
                    resp = self.api.order_modify(*args)

                task["fn"] = "modify_stop_to_target1"
        except Exception as e:
            log_exception(e, locals())
            traceback.print_exc()
        finally:
            self._update(task)

    def modify_stop_to_target1(self, **task):
        task["fn"] = "trail_stop1_to_entry"
        self._update(task)

    """
        core functions
    """

    def _update(self, updated_task):
        logging.debug(f'UPDATING \n {updated_task}')
        self.tasks = update_lst_of_dct_with_vals(
            self.tasks, "id", **updated_task)
        FUTL.write_file(content=self.tasks, filepath=F_TASK)
        self.is_dirty = True

    def read_new_buy_fm_csv(self):
        if self.lastsync < pendulum.now().subtract(seconds=15):
            # get ids in current tasks
            ids = [task["id"] for task in self.tasks]
            columns = ['channel', 'id', 'symbol', 'quantity',
                       'entry_range', 'target_range', 'sl', 'action']
            df = pd.read_csv(F_SIGNAL,
                             names=columns,
                             index_col=None)
            lst_of_dct = df.to_dict(orient="records")
            lst_of_dct = [dct
                          for dct in lst_of_dct
                          if dct["id"] not in ids and dct["action"] == 'Buy']
            return lst_of_dct
        else:
            return []

    def sync(self, lst_of_dct):
        for dct in lst_of_dct:
            dct.pop("channel", None)
            dct["fn"] = "entry"
            print("sync new buy", dct)
            self._update(dct)
        self.lastsync = pendulum.now()

    def read(self) -> List[Dict or None]:
        logging.debug("READING")
        if self.is_dirty:
            self.tasks = FUTL.read_file(F_TASK)
            self.is_dirty = False
        for task in self.tasks:
            for k, v in task.items():
                if isinstance(v, dict):
                    print(f"\n{v}\n")
                else:
                    print(f"{k}: {v}")
        UTIL.slp_for(5)

    def process_que(self):
        if any(self.tasks):
            for task in self.tasks:
                fn: str = task["fn"]
                if fn not in lst_ignore:
                    self._str_to_func(fn, task)
                    UTIL.slp_for(SECS)


def run():
    api = get_broker(BRKR)
    download_masters(api.broker)
    #  initiate task object
    obj_db = Jsondb(api)
    while True:
        lst_calls = obj_db.read_new_buy_fm_csv()
        if any(lst_calls):
            # write the new calls to task file
            obj_db.sync(lst_calls)
        # read current task que
        obj_db.read()
        # process each task
        obj_db.process_que()


if __name__ == "__main__":
    run()
    lst_of_dct = [
        {"id": 1, "symbol": "BANKNIFTY:47300:CE", "quantity": 1, "action": "Buy"},
        {"id": 2, "symbol": "BANKNIFTY:47300:CE", "quantity": 1, "action": "Sell"},
    ]
    updated_val = {"id": 1, "symbol": "UPDATED",
                   "quantity": 500, "action": "Sell"}
    val = update_lst_of_dct_with_vals(lst_of_dct, "id", **updated_val)
    print(val)
    empty_lst = update_lst_of_dct_with_vals([], "id", **updated_val)
    print(f"{empty_lst=}")
