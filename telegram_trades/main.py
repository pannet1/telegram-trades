from constants import DATA, BRKR, FUTL, F_TASK, UTIL, logging
from login import get_broker
from api_helper import (
    download_masters,
    get_ltp,
    update_lst_of_dct_with_vals,
    get_order_from_book,
    square_off,
    market_order,
    modify_order,
)
from rich import print
import traceback
import pandas as pd
import inspect
from typing import List, Dict
import pendulum as pdlm

TTL_IN_MINUTES = 15

lst_ignore = [
    "rejected",
    "cancelled",
    "E-ORDERBOOK",
    "E-ENTRY",
    "E-TRAIL",
    "E-STOP",
    "IGNORED",
    "HARD-STOP",
    "XXX",
    "STOPPED-OUT",
    "TRAILED-OUT",
    "TRADES_COMPLETED",
]
F_SIGNAL = DATA + "signals.csv"
SECS = 0.5  # sleep time


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
    method_name = inspect.stack()[1].function  # Get the name of the calling method

    # Format the message
    message = f"""
        Method Name: {method_name}
        Type: {type(exception).__name__}
        Parameters: {locals}
        Error message: {exception}
        {error_message if error_message else ''}
    """
    # Log the exception with appropriate level
    logging.error(str(message), exc_info=True)


def task_to_order_args(last_price, **task):
    try:
        # defaults
        args = {}
        trigger_price = 0.0
        order_type = "LMT"
        task["entry_range"] = str(task["entry_range"])
        lst_bprc = list(map(float, task["entry_range"].split("|")))
        min_prc = min(lst_bprc)
        max_prc = max(lst_bprc)
        if (side := task["action"][0].upper()) == "B":
            price = max_prc if last_price > min_prc else min_prc
            if min_prc > last_price:
                trigger_price = price - 0.05
                order_type = "SL"
            """
            elif max_prc < last_price:
                price = 0.0
                order_type = "MKT"
            """
            args = dict(
                symbol=task["symbol"],
                side=side,
                quantity=task["tq"],
                price=price,
                trigger_price=trigger_price,
                order_type=order_type,
                product="N",
                remarks=task["channel"],
            )
            logging.info(args)
    except Exception as e:
        log_exception(e, locals())
        traceback.print_exc()
    finally:
        return max_prc, args


def is_key_val(dct, key, val):
    # Check if the key exists in the dictionary
    if key in dct:
        # Compare the value associated with the key to the provided value
        return dct[key] == val
    else:
        # If the key doesn't exist, return False
        return False


def show(task):
    for k, v in task.items():
        if isinstance(v, dict):
            print(f"{v}\n")
        else:
            print(f"{k: >20}: {v}")


def do_trail(ltp: float, order: Dict, target_range: List):
    args = {}
    target_range = list(map(float, target_range))
    if order["side"] == "S":
        # max target reached
        if ltp > target_range[-1]:
            order["price"] = 0.0
            order["trigger_price"] = 0.0
            order["order_type"] = "MKT"
            logging.info(f"ltp above max target for order {order}")
            return order

        # market within a new trail/target band ?
        for idx, t2 in enumerate(target_range):
            if idx > 0:
                intended_stop = target_range[idx - 1]
                trigger = float(order["trigger_price"])
                if ltp >= t2 and intended_stop > trigger:
                    order["order_type"] = "SL"
                    order["trigger_price"] = intended_stop
                    order["price"] = intended_stop - 0.05
                    logging.info(f"trailing args {order}")
                    args = order
                else:
                    logging.info(
                        f"{ltp=}>={t2=} {ltp>=t2} and {intended_stop=}>{trigger=}{intended_stop > trigger}"
                    )
    return args


class TaskFunc:
    def __init__(self, api):
        self.api = api

    def _str_to_func(self, task):
        """
        converts string to callable function
        does not work if imported
        """
        fn = task.get("fn", "no_func")
        # Check if the string represents a valid method name in the class
        if hasattr(self, fn):
            # Get the method corresponding to the provided string
            method = getattr(self, fn)
            # Call the method with the provided arguments and keyword arguments
            return method(**task)
        else:
            # Raise an error if the method does not exist
            raise AttributeError(f"{fn} is not a valid method in this class")

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
            task["fn"] = "E-ENTRY"
            ltp = get_ltp(
                self.api.broker,
                task["symbol"].split(":")[0],
                task["symbol"].split(":")[1],
            )
            task["ltp"] = ltp
            if ltp > 0:
                min_prc, args = task_to_order_args(ltp, **task)
                """ 
                if args["order_type"] == "MKT":
                    task["price"] = min_prc
                    logging.info("task price update for market order: " + str(min_prc))
                    # TODO
                else:
                    task["price"] = max(args["trigger_price"], args["price"])
                """
                task["price"] = max(args["trigger_price"], args["price"])
                if any(args):
                    # check order status based on resp
                    logging.info(f"entry args: {args}")
                    resp = self.api.order_place(**args)
                    logging.info(f"{resp=}")
                    if isinstance(resp, dict):
                        order_details = get_order_from_book(self.api, resp)
                        status = order_details["Status"]
                        if status not in lst_ignore:
                            task["entry"] = order_details
                            task["fn"] = "is_entry"
                        else:
                            logging.warning("entry status: " + status)
                            task["fn"] = status
        except Exception as e:
            log_exception(e, locals())
            traceback.print_exc()
        finally:
            return task

    def is_entry(self, **task):
        try:
            order = task["entry"]
            order = get_order_from_book(self.api, order["order_id"])
            if is_key_val(order, "Status", "complete"):
                task["entry"] = order
                logging.info(f"entry for {task['symbol']} is {order['Status']}")
                task["fn"] = "stop1"
            elif order["Status"] in lst_ignore:
                logging.error(f"is_entry: {order['Status']}")
                task["fn"] = order["Status"]
            else:
                ts = order["broker_timestamp"].split(" ")[1]
                if pdlm.now() >= pdlm.parse(ts).add(minutes=TTL_IN_MINUTES).set(
                    tz="Asia/Kolkata"
                ):
                    logging.info(f"trying to cancel {order}")
                    resp = self.api.order_cancel(order["order_id"])
                    logging.info(f"cancel response was {resp}")
                    task["cancel"] = resp
                    order = get_order_from_book(self.api, resp)
                    task["entry"] = order
                    if is_key_val(order, "Status", "cancelled"):
                        task["fn"] = "IGNORED"
        except Exception as e:
            log_exception(e, locals())
            traceback.print_exc()
        finally:
            return task

    def stop1(self, **task):
        try:
            args = dict(
                symbol=task["symbol"],
                side="S",
                quantity=task["tq"],
                price=task["sl"] - 0.05,
                trigger_price=float(task["sl"]),
                order_type="SL",
                product="N",
                remarks=task["channel"],
            )
            logging.info(f"stop args: {args}")
            resp = self.api.order_place(**args)
            if isinstance(resp, dict):
                stop_order = get_order_from_book(self.api, resp)
                task["stop"] = stop_order
                if stop_order["Status"] in lst_ignore:
                    task["fn"] = stop_order["Status"]
                else:
                    task["fn"] = "is_stop_or_target1"
            else:
                task["fn"] = "E-STOP"
        except Exception as e:
            log_exception(e, locals())
            traceback.print_exc()
        finally:
            return task

    def is_stop_or_target1(self, **task):
        try:
            stop_order = task["stop"]
            stop_order = get_order_from_book(self.api, stop_order["order_id"])
            task["stop"] = stop_order
            ltp = get_ltp(
                self.api.broker,
                task["symbol"].split(":")[0],
                task["symbol"].split(":")[1],
            )
            if ltp > 0:
                task["ltp"] = ltp
            else:
                ltp = task["ltp"]
            task["pnl"] = (ltp * task["tq"]) - (
                float(task["entry"]["price"]) * task["tq"]
            )

            # stopped or trail
            if is_key_val(stop_order, "Status", "complete"):
                task["fn"] = "STOPPED-OUT"
            else:
                tgt = float(task["target_range"].split("|")[0])

                is_stopped = stop_order["side"].upper() == "S" and ltp < float(
                    task["sl"]
                )
                is_target = stop_order["side"].upper() == "S" and ltp > tgt

                if is_stopped:
                    quantity = task["tq"]
                    task["fn"] = "HARD-STOP"
                    logging.info(f"market jumped the stop loss for {task['symbol']}")
                elif is_target:
                    quantity = task["q1"]
                    logging.info(f"target1 reached {task['symbol']}")

                if is_stopped or is_target:
                    resp = square_off(
                        self.api, stop_order["order_id"], task["symbol"], quantity
                    )
                    logging.info(f"target 1 or stop resp: {resp}")

                if is_target:
                    if task.get("q2", None):
                        # we  still have more legs to manage
                        price = task["price"]
                        args = dict(
                            symbol=task["symbol"],
                            side="S",
                            quantity=task["q2"],
                            price=price - 0.05,
                            trigger_price=price,
                            order_type="SL",
                            product="N",
                            remarks=task["channel"],
                        )
                        logging.info(f"stop loss order: {args}")
                        resp = self.api.order_place(**args)
                        if isinstance(resp, dict):
                            trail_order = get_order_from_book(self.api, resp)
                            task["trail"] = trail_order
                            if trail_order["Status"] in lst_ignore:
                                task["fn"] = stop_order["Status"]
                            else:
                                task["fn"] = "trail"
                        else:
                            logging.error("placing trailing stop", resp)
                            task["fn"] = "E-TRAIL"
                    else:
                        # target1 reached
                        task["fn"] = "TRADES_COMPLETED"

        except Exception as e:
            log_exception(e, locals())
            traceback.print_exc()
        finally:
            return task

    def trail(self, **task):
        try:
            trail_order = task["trail"]
            trail_order = get_order_from_book(self.api, trail_order["order_id"])
            if is_key_val(trail_order, "Status", "complete"):
                task["trail"] = trail_order
                task["fn"] = "TRAILED-OUT"
            else:
                ltp = get_ltp(
                    self.api.broker,
                    task["symbol"].split(":")[0],
                    task["symbol"].split(":")[1],
                )
                if ltp > 0:
                    task["ltp"] = ltp
                    logging.info(f"checking trailing for {task['symbol']}")
                    lst_of_targets = task["target_range"].split("|")
                    order_args = do_trail(task["ltp"], trail_order, lst_of_targets)
                    if any(order_args):
                        order_args.update({"symbol": task["symbol"]})
                        resp = modify_order(self.api, order_args)
                        logging.info(f"modify resp: {resp}")

                # get index of target range from the price
        except Exception as e:
            log_exception(e, locals())
            traceback.print_exc()
        finally:
            return task

    def do_cancel(self, tasks, channel, symbol):
        try:
            for task in tasks:
                if (
                    task["symbol"] == symbol
                    and task["channel"] == channel
                    and task["fn"] not in lst_ignore
                ):
                    """
                    task["ltp"] = get_ltp(
                        self.api.broker,
                        task["symbol"].split(":")[0],
                        task["symbol"].split(":")[1],
                    )
                    """
                    order = task.get("trail", None)
                    if order:
                        resp = square_off(
                            self.api, order["order_id"], task["symbol"], task["q2"]
                        )
                        logging.info(f"cancel trail: {resp}")
                        task["fn"] = "XXX"
                        return task

                    order = task.get("stop", None)
                    if order:
                        resp = square_off(
                            self.api, order["order_id"], task["symbol"], task["tq"]
                        )
                        logging.info(f"cancel stop: {resp}")
                        task["fn"] = "XXX"
                        return task

                    order = task.get("entry", None)
                    if order:
                        logging.info(f"trying to cancel {order}")
                        resp = self.api.order_cancel(order["order_id"])
                        logging.info(f"cancel response was {resp}")
                        task["cancel"] = resp
                        order = get_order_from_book(self.api, resp)
                        task["entry"] = order
                        if is_key_val(order, "Status", "complete"):
                            action = "opposite"
                            logging.info(f"entry {order} found for {action}")
                            resp = market_order(self.api, order, action)
                            logging.info(f"{action} says {resp}")
                            if isinstance(resp, dict):
                                task[action] = get_order_from_book(self.api, resp)
                            logging.info("not following this task anyway")
                        task["fn"] = "XXX"
                        return task
        except Exception as e:
            log_exception(e, locals())
            traceback.print_exc()


class Jsondb:
    def __init__(self) -> None:
        # input file
        if FUTL.is_file_not_2day(F_SIGNAL):
            # return empty list if file is not modified today
            FUTL.write_file(filepath=F_SIGNAL, content="")
        # initate output task json file
        if FUTL.is_file_not_2day(F_TASK):
            FUTL.write_file(filepath=F_TASK, content=[])
        # marker to find if json file is dirty

    def _update(self, updated_task, tasks):
        """to be removed"""
        tasks = update_lst_of_dct_with_vals(tasks, "id", **updated_task)
        FUTL.write_file(content=tasks, filepath=F_TASK)

    def _read_new_buy_fm_csv(self, lst_of_dct: List):
        ids = [task["id"] for task in lst_of_dct if isinstance(task, dict)]
        # TODO
        columns = [
            "channel",
            "id",
            "symbol",
            "entry_range",
            "target_range",
            "sl",
            "quantity",
            "action",
            "timestamp",
        ]
        df = pd.read_csv(F_SIGNAL, names=columns, index_col=None)
        lst_of_dct = df.to_dict(orient="records")
        lst_of_dct = [
            dct for dct in lst_of_dct if dct["id"] not in ids and dct["action"] == "Buy"
        ]
        return lst_of_dct

    def _read_cancellation_fm_csv(self, lst_of_dct: List):
        ids = [task["id"] for task in lst_of_dct if (isinstance(task, dict))]
        columns = [
            "channel",
            "id",
            "symbol",
            "entry_range",
            "target_range",
            "sl",
            "quantity",
            "action",
            "timestamp",
        ]
        df = pd.read_csv(F_SIGNAL, names=columns, index_col=None)
        lst_of_dct = df.to_dict(orient="records")
        lst_of_dct = [
            dct
            for dct in lst_of_dct
            if dct["id"] not in ids and dct["action"] == "Cancel"
        ]
        return lst_of_dct

    def sync(self, new_calls):
        """Sync function to process the list of calls."""
        for task in new_calls:
            if "|" in str(task["quantity"]):
                lst_qty = task["quantity"].split("|")
                task["q1"] = int(lst_qty[0])
                task["q2"] = int(lst_qty[1])
                task["tq"] = task["q1"] + task["q2"]
            else:
                task["q1"] = int(task["quantity"])
                task["tq"] = task["q1"]
            task["entry_range"] = str(task.pop("entry_range"))
            task["target_range"] = str(task.pop("target_range"))
            task["fn"] = "entry"
            yield task

    def read(self):
        try:
            all_calls = FUTL.read_file(F_TASK)
            new_calls = self._read_new_buy_fm_csv(all_calls)
            is_updated = False
            for task in self.sync(new_calls):
                print(f"{task=}")
                is_updated = True
                all_calls.append(task)
            if is_updated:
                FUTL.write_file(content=all_calls, filepath=F_TASK)
            return all_calls
        except Exception as e:
            print(e)
            traceback.print_exc()

    def read_cancellation(self):
        try:
            is_updated = False
            all_calls = FUTL.read_file(F_TASK)
            new_cancellations = self._read_cancellation_fm_csv(all_calls)
            if new_cancellations and any(new_cancellations):
                for dct in new_cancellations:
                    if isinstance(dct, dict):
                        is_updated = True
                        dct.update({"fn": "do_cancel"})
                        all_calls.append(dct)
            if is_updated:
                FUTL.write_file(content=all_calls, filepath=F_TASK)
            return all_calls
        except Exception as e:
            print(e)
            traceback.print_exc()


def run():
    try:
        api = get_broker(BRKR)
        download_masters(api.broker)
        #  initiate task object
        obj_db = Jsondb()
        obj_tasks = TaskFunc(api)
        while True:
            UTIL.slp_for(SECS)
            tasks = obj_db.read()
            if tasks and any(tasks):
                for task in tasks:
                    if task["fn"] not in lst_ignore and task["fn"] != "do_cancel":
                        show(task)
                        task = obj_tasks._str_to_func(task)
                        obj_db._update(task, tasks)

            # contains all task incl cancellation
            UTIL.slp_for(SECS)
            cancellations = obj_db.read_cancellation()
            if cancellations and any(cancellations):
                for cancellation in cancellations:
                    if cancellation["fn"] == "do_cancel":
                        show(cancellation)
                        closed_position = obj_tasks.do_cancel(
                            cancellations,
                            channel=cancellation["channel"],
                            symbol=cancellation["symbol"],
                        )
                        if closed_position and any(closed_position):
                            obj_db._update(closed_position, cancellations)
                            cancellation["fn"] = "XXX"
                            obj_db._update(cancellation, cancellations)
    except Exception as e:
        print(e)
        traceback.print_exc()


if __name__ == "__main__":
    run()
