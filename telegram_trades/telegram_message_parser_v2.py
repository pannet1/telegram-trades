import re
import glob
import pandas as pd
import math
import shutil
import csv
import os
import traceback
from datetime import datetime
from login import get_broker
# from constants import FUTL, CHANNEL_DETAILS, DATA, STRIKE_PRICE_DIFF
from constants import BRKR, FUTL, CHANNEL_DETAILS, DATA, STRIKE_PRICE_DIFF
from logzero import logger
import random
import numpy as np

zero_sl = "0.50"
DEFAULT_FOREX_QTY = 2
signals_csv_filename = DATA + "signals.csv"
forex_csv_filename = DATA + "forex.csv"
sl_tgt_csv_filename = DATA + "SL-TGT.csv"
if os.path.isfile(signals_csv_filename):
    shutil.move(signals_csv_filename, signals_csv_filename.removesuffix(
        ".csv")+f'_{datetime.now().strftime("%Y%m%d-%H%M%S")}.csv')
if os.path.isfile(forex_csv_filename):
    shutil.move(forex_csv_filename, forex_csv_filename.removesuffix(
        ".csv")+f'_{datetime.now().strftime("%Y%m%d-%H%M%S")}.csv')
signals_csv_file_headers = [
    "channel_name",
    "timestamp",
    "symbol",
    "ltp_range",
    "target_range",
    "sl",
    "quantity",
    "action",
    "normal_timestamp",
]
failure_csv_filename = DATA + "failures.csv"
failure_csv_file_headers = ["channel_name", "timestamp",
                            "message", "exception", "normal_timestamp",]
signals = []
spell_checks = {
    "F1NIFTY": "FINNIFTY",
    "N1FTY": "NIFTY",
    "NIFITY": "NIFTY",
    "MIDCAPNIFTY": "MIDCPNIFTY",
    "BANINIFTY": "BANKNIFTY",
    "MIDCAP": "MIDCPNIFTY",
    "MIDCP NIFTY": "MIDCPNIFTY",
    "NIFTU": "NIFTY",
    "NOFTY": "NIFTY",
    "FIN NIFTY": "FINNIFTY",
    "BANK NIFTY": "BANKNIFTY",
    "BNF": "BANKNIFTY",
    "BANNIFTY": "BANKNIFTY",
    "NF": "NIFTY",
    "MIDCPNIFTYNIFTY": "MIDCPNIFTY",
    "MIDCP": "MIDCPNIFTY",
    "BANKNIFT ": "BANKNIFTY ",
    "BAJAJAUTO": "BAJAJ-AUTO",
    "ADANIPORT ": "ADANIPORTS ",
    "FIN ": "FINNIFTY ",
    "C3 ": "CE ",
    "P3 ": "PE ",
    "SENSES": "SENSEX",
}
index_options = ('FINNIFTY', 'NIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX', 'BANKNIFTY')
close_words = ("CANCEL", "EXIT", "BREAK", "AVOID", "LOSS", "IGNORE", "CLOSE", "MULTIPLE","SAFE") # "BOOK","LOW RISK"


class CustomError(Exception):
    pass


def get_updated_strike_price(sym, strike, option_type):
    strike_p = STRIKE_PRICE_DIFF.get(sym)
    new_strike = int(strike) + (int(strike_p) * (1  if option_type == "PE" else -1))
    return new_strike 

def download_masters(broker):
    exchanges = ["NFO", "BFO"]
    for exchange in exchanges:
        if FUTL.is_file_not_2day(f"./{exchange}.csv"):
            broker.get_contract_master(exchange)


def get_multiplier(symbol, channel_config, num_of_targets=2, special_case=None):
    nfo_df = pd.read_csv("NFO.csv")
    bfo_df = pd.read_csv("BFO.csv")
    df = pd.concat([nfo_df, bfo_df])
    lot_size = df.loc[df['Trading Symbol'] == symbol, 'Lot Size'].iloc[0]
    if special_case == "HEDGE":
        if "BANKNIFTY" in symbol:
            return str(30*channel_config.get("BANKNIFTY_HEDGE", 1))
        elif "FINNIFTY" in symbol:
            return str(65*channel_config.get("FINNIFTY_HEDGE", 1))
        elif "MIDCPNIFTY" in symbol:
            return str(120*channel_config.get("MIDCPNIFTY_HEDGE", 1))
        elif "NIFTY" in symbol:
            return str(75 * channel_config.get("NIFTY_HEDGE", 1))
        elif "SENSEX" in symbol:
            return str(20*channel_config.get("SENSEX_HEDGE", 1))
        elif "BANKEX" in symbol:
            return str(30*channel_config.get("BANKEX_HEDGE", 1))
    elif special_case == "BTST":
        if "BANKNIFTY" in symbol:
            return "|".join([str(30 * channel_config.get("BANKNIFTY_BTST", 1))] * num_of_targets) if num_of_targets > 1 else str(30*channel_config.get("BANKNIFTY_BTST", 1))
        elif "FINNIFTY" in symbol:
            return "|".join([str(65 * channel_config.get("FINNIFTY_BTST", 1))]* num_of_targets) if num_of_targets > 1 else str(65*channel_config.get("FINNIFTY_BTST", 1))
        elif "MIDCPNIFTY" in symbol:
            return "|".join([str(120 * channel_config.get("MIDCPNIFTY_BTST", 1))]* num_of_targets) if num_of_targets > 1 else str(120*channel_config.get("MIDCPNIFTY_BTST", 1))
        elif "NIFTY" in symbol:
            return (
                "|".join([str(75 * channel_config.get("NIFTY_BTST", 1))] * num_of_targets)
                if num_of_targets > 1
                else str(75 * channel_config.get("NIFTY_BTST", 1))
            )
        elif "SENSEX" in symbol:
            return "|".join([str(20 * channel_config.get("SENSEX_BTST", 1))]* num_of_targets) if num_of_targets > 1 else str(20*channel_config.get("SENSEX_BTST", 1))
        elif "BANKEX" in symbol:
            return "|".join([str(30 * channel_config.get("BANKEX_BTST", 1))]* num_of_targets) if num_of_targets > 1 else str(30*channel_config.get("BANKEX_BTST", 1))
        return "|".join([str(lot_size * channel_config.get("STOCKOPTION_BTST", 1))] * num_of_targets) if num_of_targets > 1 else str(lot_size*channel_config.get("STOCKOPTION_BTST", 1))
    elif str(symbol).endswith("F"):
        return str(lot_size*channel_config.get("FUT", 1))
    elif "BANKNIFTY" in symbol:
        return "|".join([str(30 * channel_config.get("BANKNIFTY", 1))] * num_of_targets) if num_of_targets > 1 else str(30*channel_config.get("BANKNIFTY", 1))
    elif "FINNIFTY" in symbol:
        return "|".join([str(65 * channel_config.get("FINNIFTY", 1))]* num_of_targets) if num_of_targets > 1 else str(65*channel_config.get("FINNIFTY", 1))
    elif "MIDCPNIFTY" in symbol:
        return "|".join([str(120 * channel_config.get("MIDCPNIFTY", 1))]* num_of_targets) if num_of_targets > 1 else str(120*channel_config.get("MIDCPNIFTY", 1))
    elif "NIFTY" in symbol:
        return (
            "|".join([str(75 * channel_config.get("NIFTY", 1))] * num_of_targets)
            if num_of_targets > 1
            else str(75 * channel_config.get("NIFTY", 1))
        )
    elif "SENSEX" in symbol:
        return "|".join([str(20 * channel_config.get("SENSEX", 1))]* num_of_targets) if num_of_targets > 1 else str(20*channel_config.get("SENSEX", 1))
    elif "BANKEX" in symbol:
        return "|".join([str(30 * channel_config.get("BANKEX", 1))]* num_of_targets) if num_of_targets > 1 else str(30*channel_config.get("BANKEX", 1))
    return str(lot_size*channel_config.get("STOCKOPTION", 1))


def get_all_contract_details(exchange=None):
    """
    To be run only once possibly at the start of the day
    """
    dfs = []
    req_columns = [
        "Exch",
        "Symbol",
        "Option Type",
        "Strike Price",
        "Trading Symbol",
        "Expiry Date",
    ]
    pattern = "*.csv" if not exchange else f"*{exchange}*.csv"
    for file in glob.glob(pattern):
        df = pd.read_csv(file, index_col=None)
        if set(req_columns).issubset(df.columns):
            dfs.append(df[req_columns])
        else:
            print(f"Required columns not found in file {file}")

    df = pd.concat(dfs)
    return df


def write_signals_to_csv(_signal_details):
    with open(signals_csv_filename, "a", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=signals_csv_file_headers)
        _signal_details["normal_timestamp"] = datetime.fromtimestamp(
            int(_signal_details["timestamp"][2:])).strftime('%Y-%m-%d %H:%M:%S')
        _signal_details["timestamp"] = _signal_details["timestamp"] + ''.join(random.choices('0123456789', k=5))
        __sl = _signal_details["sl"]
        if (isinstance(__sl, int) or isinstance(__sl, float)) and __sl == 0 and "BH" not in _signal_details["action"]:
            _signal_details["sl"] = zero_sl
        elif isinstance(__sl, str) and __sl.isdigit() and  __sl in ("0", "00") and "BH" not in _signal_details["action"]:
            _signal_details["sl"] = zero_sl
        elif isinstance(__sl, str) and not __sl.strip() and "BH" not in _signal_details["action"]:
            _signal_details["sl"] = zero_sl
        writer.writerow(
            {k: str(_signal_details.get(k, ""))
             for k in signals_csv_file_headers}
        )
        logger.info(_signal_details)

def write_signals_to_forex_csv(_signal_details):
    with open(forex_csv_filename, "a", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=[i for i in signals_csv_file_headers if i!= 'channel_name'])
        _signal_details["normal_timestamp"] = datetime.fromtimestamp(
            int(_signal_details["timestamp"][2:])).strftime('%Y-%m-%d %H:%M:%S')
        _signal_details["timestamp"] = _signal_details["timestamp"] + ''.join(random.choices('0123456789', k=5))
        writer.writerow(
            {k: str(_signal_details.get(k, ""))
             for k in signals_csv_file_headers if k!= 'channel_name'}
        )
        logger.info(_signal_details)

def write_failure_to_csv(failure_details):
    with open(failure_csv_filename, "a", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=failure_csv_file_headers)
        failure_details["normal_timestamp"] = datetime.fromtimestamp(
            failure_details["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        writer.writerow(
            {k: str(failure_details.get(k, ""))
             for k in failure_csv_file_headers}
        )


def round_to_point_five(value):
    rounded_value = round((value*2)/2)
    return int(rounded_value) if rounded_value==int(rounded_value) else rounded_value

def get_sl_target_from_csv(symbol_name, max_ltp):
    try:
        df = pd.read_csv(sl_tgt_csv_filename, header=0)
        df = df[(df["Instrument"] == symbol_name) & (df["To"] >= int(max_ltp))]
        output = df.sort_values(by="To")[:1].to_dict(orient="records")[0]
        _ = output.pop('Instrument')
        _ = output.pop('To')
        sl = str(round_to_point_five(int(max_ltp) - (int(max_ltp) * output.pop('SL') / 100)))
        targets = [str(int(max_ltp) + round_to_point_five(i * int(max_ltp) / 100)) for i in list(output.values())]
        return sl, targets
    except:
        logger.error(traceback.format_exc())
        return None, None

api = get_broker(BRKR)
download_masters(api.broker)
scrip_info_df = get_all_contract_details()
all_symbols = set(scrip_info_df["Symbol"].to_list())

def get_closest_match(symbol):
    if symbol in spell_checks:
        symbol = spell_checks[symbol]
    if symbol in all_symbols:
        return symbol
    raise CustomError("Closest match is not found")


def get_fut_instrument_name(sym):
    try:
        sym = get_closest_match(sym)
        exch = "BFO" if sym in ["SENSEX", "BANKEX"] else "NFO"
        filtered_df = scrip_info_df[
            (scrip_info_df["Exch"] == exch)
            & (scrip_info_df["Symbol"] == sym)
            & (scrip_info_df["Option Type"] == "XX")
        ]
        sorted_df = filtered_df.sort_values(by="Expiry Date")
        sorted_df['Expiry Date'] = pd.to_datetime(sorted_df['Expiry Date'], format='%Y-%m-%d')
        sorted_df = sorted_df[sorted_df['Expiry Date'] >= np.datetime64(datetime.now().date())]
        first_row = sorted_df.head(1)
        return first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0]
    except:
        raise CustomError(traceback.format_exc())

def get_float_values(string_val, start_val):
    float_values = []
    if start_val not in string_val:
        return []
    v = string_val.split(start_val)
    for word in re.split(" |-|,|/",v[1]):
        if not word:
            continue
        if word.replace("+", "").replace("-", "").replace(".", "", 1).isdigit():
            float_values.append(word.replace("+", "").replace("-", ""))
        else:
            break
    return float_values

def get_reverse_float_values(string_val, start_val):
    float_values = []
    if start_val not in string_val:
        return []
    v = string_val.split(start_val)
    for word in re.split(" |-|,|/",v[0])[::-1]:
        if not word:
            continue
        if word.replace("+", "").replace("-", "").replace(".", "", 1).isdigit():
            float_values.append(word.replace("+", "").replace("-", ""))
        else:
            break
    return float_values


class PremiumFXG:
    # split_words = ["ABOVE", "ABOV", "NEAR", 'NEAT', "TARGET", "TARGE"]
    ignore_words = ['NEAR', 'BELOWE', 'BELOW', 'ONLY', 'QUANTITY', 'PLUS', 'ABOVE', 'CALL', 'INTRADAY', 'MUST', 'ORDER', 'AGAIN', 'ALL', 'FULL', 'FRESH', 'ZONE']
    symbols_to_extract = ["XAUUSD","BTCUSD","USOIL","USTEC", "ETH"]
    channel_details = CHANNEL_DETAILS["PremiumFXG"]
    channel_number = channel_details["channel_number"]


    def get_closest_match_for_buy(self, sentence):
        words = sentence.lower().split()
        for word in words:
            if word == "buy" or word == "byu" or word == "bu":
                return True
        return False

    def get_closest_match_for_sell(self, sentence):
        words = sentence.lower().split()
        for word in words:
            if word == "sell" or word == "sel":
                return True
        return False


    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        
        self.message = telegram_msg.strip()
        
        for word in PremiumFXG.ignore_words:
            if word in self.message.split():
                self.message = self.message.replace(word, "")
        for misspelt_word, right_word in spell_checks.items():
            if misspelt_word in self.message:
                self.message = self.message.replace(misspelt_word, right_word)
                
        logger.info(f"forex message in __init__ is {self.message}")

    def get_float_values(self, string_val, start_val=None):
        float_values = []

        v = string_val.split() if not start_val else string_val.split(start_val)
        if len(v) < 2:
            return float_values
        for word in re.split(" |-|,|/",v[1]):
            if not word:
                continue
            if word.replace("+", "").replace("-", "").replace(".", "", 1).isdigit():
                float_values.append(word.replace("+", "").replace("-", ""))
            else:
                break
        return float_values
    
    def get_ltps(self, statement_):
        float_values = []
        if "sl" in statement_:
            statement_ = statement_.split("sl")[0]
        if "target" in statement_:
            statement_ = statement_.split("target")[0]

        for word in re.split(" |-|,|/",statement_):
            if not word:
                continue
            if word.replace("+", "").replace("-", "").replace(".", "", 1).isdigit():
                float_values.append(word.replace("+", "").replace("-", ""))
                if float_values:
                    break
            
        return float_values

    def get_signal(self):
        try:
            statement = self.message
            is_reply_msg = '$$$$' in statement
            new_msg = self.message.upper().split('$$$$')[-1]
            is_close_msg = any([word in new_msg.split()
                               for word in close_words])
            if is_reply_msg and is_close_msg:
                # is a reply message and has close words in it:
                pass
            elif not is_reply_msg:
                # is not a reply message
                pass
            elif is_reply_msg:
                failure_details = {
                    "channel_name": "PremiumFXG",
                    "timestamp": self.msg_received_timestamp,
                    "message": self.message,
                    "exception": "Reply messsage without close words",
                }
                logger.error(failure_details)
                write_failure_to_csv(failure_details)
                return
            
            action = "BUY"
            if is_reply_msg and is_close_msg:
                action = "CANCEL"
            elif self.get_closest_match_for_buy(statement):
                action = "BUY"
            elif self.get_closest_match_for_sell(statement):
                action = "SELL"

            targets = self.get_float_values(statement.lower(), start_val='targets')
            if not targets:
                targets = self.get_float_values(statement.lower(), start_val='target')
            
            sl = self.get_float_values(statement.lower(), start_val='sl')

            ltps = self.get_ltps(statement.lower())

            for word in PremiumFXG.symbols_to_extract:
                if word in self.message.split():
                    symbol = word
                    break
            else:
                if ltps:
                    if len(ltps[0]) == 4:
                        symbol = "XAUUSD"
                    elif len(ltps[0]) in (5, 6):
                        symbol = "BTCUSD"
               
            if not targets or not sl:
                if symbol == "XAUUSD":
                    ltp_max = max(ltps)
                    sl = [ltp_max - 10.0]
                    targets = [ltp_max + 5.0, ltp_max + 10.0]
                elif symbol in  ("BTCUSD", "ETH"):
                    ltp_max = max(ltps)
                    sl = [ltp_max - 1000.0]
                    targets = [ltp_max + 500.0, ltp_max + 1000.0]
                


            
            __signal_details = {
                # "channel_name": "PremiumFXG",
                "symbol": symbol,
                "ltp_range": "|".join(ltps),
                "target_range":  "|".join(targets),
                "sl":  "|".join(sl),
                "quantity": DEFAULT_FOREX_QTY * 2 if 'DOUBLE' in self.message else DEFAULT_FOREX_QTY,
                "action": action
            }
            __signal_details["timestamp"] = f"{PremiumFXG.channel_number}{self.msg_received_timestamp}"
            write_signals_to_forex_csv(__signal_details)

        except:
            failure_details = {
                "channel_name": "PremiumFXG",
                "timestamp": self.msg_received_timestamp,
                "message": self.message,
                "exception": traceback.format_exc().strip(),
            }
            logger.error(failure_details)
            write_failure_to_csv(failure_details)

class SmsStockOptionsPremium:
    ignore_words = [ ]
    channel_details = CHANNEL_DETAILS["SmsStockOptionsPremium"]
    channel_number = channel_details["channel_number"]

    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        
        self.message = telegram_msg.strip()
        for misspelt_word, right_word in spell_checks.items():
            if misspelt_word in self.message:
                self.message = self.message.replace(misspelt_word, right_word)
                
        for word in SmsStockOptionsPremium.ignore_words:
            if word in self.message.split():
                self.message = self.message.replace(word, "")
        
        logger.info(f"SmsStockOptionsPremium message in __init__ is {self.message}")
    
    def get_float_values(self, string_val, start_val=None):
        float_values = []

        v = string_val.split() if not start_val else string_val.split(start_val)
        if len(v) < 2:
            return float_values
        for word in re.split(" |-|,|/",v[1]):
            if not word:
                continue
            if word.replace("+", "").replace("-", "").replace(".", "", 1).isdigit():
                float_values.append(word.replace("+", "").replace("-", ""))
            else:
                break
        return float_values
    
    def get_closest_match_for_buy(self, sentence):
        words = sentence.lower().split()
        for word in words:
            if word == "buy": # or word == "byu" or word == "bu":
                return True
        return False

    def get_closest_match_for_sell(self, sentence):
        words = sentence.lower().split()
        for word in words:
            if word == "sell": # or word == "sel":
                return True
        return False

    def get_instrument_name(self, sym, strike, option_type):
        # FinNifty 9 Jan 21450 PE
        try:
            
            exch = "BFO" if sym in ["SENSEX", "BANKEX"] else "NFO"
            filtered_df = scrip_info_df[
                (scrip_info_df["Exch"] == exch)
                & (scrip_info_df["Symbol"] == sym)
                & (scrip_info_df["Strike Price"] == float(strike))
                & (scrip_info_df["Option Type"] == option_type)
                # & (scrip_info_df["Expiry Date"] == f"2024-{month}-{date}")
            ]
            # print(filtered_df)
            filtered_df = filtered_df.sort_values(by="Expiry Date")
            filtered_df['Expiry Date'] = pd.to_datetime(filtered_df['Expiry Date'], format='%Y-%m-%d')
            filtered_df = filtered_df[filtered_df['Expiry Date'] >= np.datetime64(datetime.now().date())]
            first_row = filtered_df.head(1)
            return first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0], sym
        except:
            raise CustomError(traceback.format_exc())

    def get_signal(self):
        try:
            statement = self.message
            is_reply_msg = '$$$$' in statement
            new_msg = self.message.upper().split('$$$$')[-1]
            is_close_msg = any([word in new_msg.split()
                               for word in close_words])
            if is_reply_msg and is_close_msg:
                # is a reply message and has close words in it:
                pass
            elif not is_reply_msg:
                # is not a reply message
                pass
            elif is_reply_msg:
                failure_details = {
                    "channel_name": "SmsStockOptionsPremium",
                    "timestamp": self.msg_received_timestamp,
                    "message": self.message,
                    "exception": "Reply messsage without close words",
                }
                logger.error(failure_details)
                write_failure_to_csv(failure_details)
                return
            
            action = "BUY"
            if is_reply_msg and is_close_msg:
                action = "CANCEL"
            elif self.get_closest_match_for_buy(statement):
                action = "BUY"
            elif self.get_closest_match_for_sell(statement):
                action = "SELL"

            targets = self.get_float_values(statement.lower(), start_val='targets')
            if not targets:
                targets = self.get_float_values(statement.lower(), start_val='target')
            
            sl = self.get_float_values(statement.lower(), start_val='sl for trade')

            ltps = self.get_float_values(statement.lower(), start_val='only in range')

            symbol = statement.split("(")[0].strip()
            tmp = statement.split(")")
            tmp1 = tmp[1].split()
            strike = tmp1[0].strip().lower().removesuffix('ce').removesuffix('pe')
            for i in tmp1:
                if i.lower().endswith('ce'):
                    option_type = 'CE'
                elif i.lower().endswith('pe'):
                    option_type = 'PE'
                
                
            symbol_dict, sym = self.get_instrument_name(symbol, strike, option_type)
            
            
            __signal_details = {
                "channel_name": "SmsStockOptionsPremium",
                "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                "ltp_range": "|".join(ltps),
                "target_range":  "|".join(targets),
                "sl":  "|".join(sl),
                "quantity":  get_multiplier(symbol_dict["Trading Symbol"], SmsStockOptionsPremium.channel_details),
                "action": action
            }
            __signal_details["timestamp"] = f"{SmsStockOptionsPremium.channel_number}{self.msg_received_timestamp}"
            write_signals_to_csv(__signal_details)

        except:
            failure_details = {
                "channel_name": "SmsStockOptionsPremium",
                "timestamp": self.msg_received_timestamp,
                "message": self.message,
                "exception": traceback.format_exc().strip(),
            }
            logger.error(failure_details)
            write_failure_to_csv(failure_details)

            
class PremiumJackpot:
    split_words = ["ABOVE", "ABOV", "NEAR", 'NEAT', "TARGET", "TARGE"]
    channel_details = CHANNEL_DETAILS["PremiumJackpot"]
    channel_number = channel_details["channel_number"]

    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        telegram_msg = telegram_msg.replace("ONLY ", "")
        if telegram_msg.strip().startswith("BUY "):
            self.message = (
                    telegram_msg.upper().split("BUY ")[1].replace("-", " ").replace(",", " ").replace("/", " ")
                )
        else:
            self.message = telegram_msg
        self.message = self.message.strip().removeprefix("BUY").removeprefix("SELL").strip().removeprefix("#")
        for misspelt_word, right_word in spell_checks.items():
            if misspelt_word in self.message:
                self.message = self.message.replace(misspelt_word, right_word)
                

    def get_closest_match(self, symbol):
        if symbol in spell_checks:
            symbol = spell_checks[symbol]
        if symbol in all_symbols:
            return symbol
        raise CustomError("Closest match is not found")

    def get_instrument_name(self, symbol_from_tg):
        try:
            sym_split = symbol_from_tg.split()
            if len(sym_split) == 3:
                sym, strike, option_type = sym_split
            elif len(sym_split) == 2:
                sym = sym_split[0]
                strike = sym_split[1].strip()[:-2]
                option_type = sym_split[1].strip()[-2:]
            elif len(sym_split) > 3:
                sym, strike, option_type, *_ = sym_split
            sym = self.get_closest_match(sym)
            exch = "BFO" if sym in ["SENSEX", "BANKEX"] else "NFO"
            filtered_df = scrip_info_df[
                (scrip_info_df["Exch"] == exch)
                & (scrip_info_df["Symbol"] == sym)
                & (scrip_info_df["Strike Price"] == float(strike))
                & (scrip_info_df["Option Type"] == option_type)
            ]
            sorted_df = filtered_df.sort_values(by="Expiry Date")
            sorted_df['Expiry Date'] = pd.to_datetime(sorted_df['Expiry Date'], format='%Y-%m-%d')
            sorted_df = sorted_df[sorted_df['Expiry Date'] >= np.datetime64(datetime.now().date())]
            first_row = sorted_df.head(1)
            return first_row[["Exch", "Trading Symbol", "Strike Price"]].to_dict(orient="records")[0], sym
        except:
            raise CustomError(traceback.format_exc())
    
    def get_signal(self):
        try:
            statement = self.message
            self.message = self.message.replace("TARGETS", "TARGET")
            print(self.message)
            is_reply_msg = '$$$$' in statement
            new_msg = self.message.upper().split('$$$$')[-1]
            is_close_msg = any([word in new_msg.split()
                               for word in close_words])
            if is_reply_msg and is_close_msg:
                # is a reply message and has close words in it:
                pass
            elif not is_reply_msg:
                # is not a reply message
                pass
            elif is_reply_msg and not is_close_msg:
                # is a reply message but not having close words
                # duplicate or junk
                failure_details = {
                    "channel_name": "Premium jackpot",
                    "timestamp": self.msg_received_timestamp,
                    "message": self.message,
                    "exception": "is a reply message but not having close words. Possible duplicate or junk",
                }
                logger.error(failure_details)
                write_failure_to_csv(failure_details)
                return
            
            if "BTST" in self.message:
                if int(PremiumJackpot.channel_details.get('INCLUDE_BTST','1')) == 0:
                    raise CustomError(f"BTST is turned off as per config")
                statement = self.message.strip().removeprefix("BUY").strip().removeprefix("#").strip().replace("-", " ")
                s = [i for i in statement.split() if i.strip()]
                sym, strike, option_type, *_ = s
                ltps = get_float_values(statement, "ABOVE")
                if not ltps:
                    raise CustomError(f"ltps is not found in {statement}")
                
                sym = self.get_closest_match(sym)
                exch = "BFO" if sym in ["SENSEX", "BANKEX"] else "NFO"
                filtered_df = scrip_info_df[
                    (scrip_info_df["Exch"] == exch)
                    & (scrip_info_df["Symbol"] == sym)
                    # & (scrip_info_df["Strike Price"] == float(strike))
                    & (scrip_info_df["Strike Price"] == float(get_updated_strike_price(sym, strike, option_type)))
                    & (scrip_info_df["Option Type"] == option_type)
                    # & (scrip_info_df["Expiry Date"] == f"2024-{month}-{date}")
                ]
                filtered_df = filtered_df.sort_values(by="Expiry Date")
                filtered_df['Expiry Date'] = pd.to_datetime(filtered_df['Expiry Date'], format='%Y-%m-%d')
                filtered_df = filtered_df[filtered_df['Expiry Date'] >= np.datetime64(datetime.now().date())]
                first_row = filtered_df.head(1)
                symbol_dict = first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0]
                __signal_details = {
                        "channel_name": "PremiumJackpot",
                        "symbol": symbol_dict["Exch"]+":"+symbol_dict["Trading Symbol"],
                        "ltp_range": "|".join(ltps),
                        "target_range": "0",
                        "sl": "",
                        "quantity": get_multiplier(symbol_dict["Trading Symbol"], PremiumJackpot.channel_details, special_case="BTST"),
                        "action": "BTST-Cancel"
                                if is_reply_msg and is_close_msg
                                else "BTST-Buy"
                    }
                if __signal_details in signals:
                    raise CustomError("Signal already exists")
                else:
                    signals.append(__signal_details)
                signal_details = __signal_details.copy()
                signal_details["timestamp"] = f"{PremiumJackpot.channel_number}{self.msg_received_timestamp}"
                write_signals_to_csv(signal_details)
            elif " FUT " in self.message or " FUTURES " in self.message:
                if int(PremiumJackpot.channel_details.get('INCLUDE_FUT','1')) == 0:
                    raise CustomError(f"Future is turned off as per config")
                statement = self.message.strip().removeprefix("BUY").removeprefix("SELL").strip().removeprefix("#")
                symbol_dict = get_fut_instrument_name(statement.split()[0])
                ltps = get_float_values(statement, "ABOVE")
                if not ltps:
                    ltps = get_float_values(statement, "NEAR")
                if not ltps:
                    raise CustomError(f"ltps is not found in {statement}")
                targets = get_float_values(statement, "TARGET")
                if not targets:
                    targets = get_float_values(statement, "TRT")
                if not targets:
                    targets = get_float_values(statement, "TGT")
                if not targets:
                    targets = get_float_values(statement, "TARGE")
                if not targets:
                    raise CustomError(f"targets is not found in {statement}")
                sl = get_float_values(statement, "SL")
                if not sl:
                    raise CustomError(f"SL is not found in {statement}")
                sl = sl[0]
                if self.message.startswith('SELL'):
                    action = "SHORT"
                elif self.message.startswith('BUY'):
                    action = "BUY"
                else:
                    action = "BUY"
                if is_reply_msg and is_close_msg:
                    action = "CANCEL"
                
                if not action:
                    raise CustomError(f"could not decide action in {statement}")

                __signal_details = {
                    "channel_name": "Premium jackpot",
                    "symbol": symbol_dict["Exch"]+":"+symbol_dict["Trading Symbol"],
                    "ltp_range": "|".join(ltps),
                    "target_range": "|".join(targets),
                    "sl": sl,
                    "quantity": get_multiplier(symbol_dict["Trading Symbol"], PremiumJackpot.channel_details),
                    "action": action,
                }
                if __signal_details in signals:
                    raise CustomError("Signal already exists")
                else:
                    signals.append(__signal_details)
                signal_details = __signal_details.copy()
                signal_details["timestamp"] = f"{PremiumJackpot.channel_number}{self.msg_received_timestamp}"
                write_signals_to_csv(signal_details)
            else:
                for word in PremiumJackpot.split_words:
                    statement = statement.replace(word, "|")
                # print(f"{statement=}")
                parts = statement.split("|")
                symbol_from_tg = parts[0].strip().removeprefix("#")
                symbol_dict, sym = self.get_instrument_name(symbol_from_tg)
                # print(parts)
                # for i in parts:
                #     if "SL" in i:
                #         ltps = re.findall(r"\d+\.\d+|\d+", i.split("SL")[0])
                #         ltps = [ltp for ltp in ltps if int(ltp) != int(symbol_dict["Strike Price"])]
                #         break
                #     if "TARGET"
                # print(self.message.split())
                target_index = self.message.split().index("TARGET")
                if target_index < self.message.split().index("SL"):
                    ltps = get_reverse_float_values(self.message, "TARGET")
                else:
                    ltps = get_reverse_float_values(self.message, "SL")
                # prinltps = sl_ltps
                ltp_max = max([float(ltp) for ltp in ltps
                            if ltp.replace('.', '', 1).isdigit()])
                sl, targets = None, None
                try:
                    # for i in parts:
                    #     if "SL" in i:
                    # targets = re.findall(r"\d+\.\d+|\d+", parts[1])
                    # print(statement)
                    targets = get_float_values(self.message, "TARGETS")
                    # print(f"{targets=}")
                    if not targets:
                        targets = get_float_values(self.message, "TARGET")
                            # break
                    
                        
                    if float(targets[0]) < float(ltps[0]):
                        targets = [str(float(target) + ltp_max)
                                    for target in targets if target.replace('.', '', 1).isdigit()]
                    sl = get_float_values(statement, "SL")[0]
                except:
                    targets, sl = None, None
                if sym in index_options:
                    key_to_chk = f"INCLUDE_INDEX_OPTION_{sym}"
                    if int(PremiumJackpot.channel_details.get(key_to_chk,'1')) == 0:
                        raise CustomError(f"{sym} is turned off as per config")
                    if not sl or not targets:
                        sl, targets = get_sl_target_from_csv(sym, ltp_max)
                        print(sl, targets)
                else:
                    if int(PremiumJackpot.channel_details.get('INCLUDE_STOCK_OPTIONS','1')) == 0:
                        raise CustomError(f"STOCK_OPTIONS is turned off as per config")
                if not targets or not sl:
                    raise CustomError("Target and SL is not availble in msg")
                __signal_details = {
                    "channel_name": "Premium jackpot",
                    "symbol": symbol_dict["Exch"]+":"+symbol_dict["Trading Symbol"],
                    "ltp_range": "|".join(ltps),
                    "target_range": "|".join(targets),
                    "sl": sl,
                    "quantity": get_multiplier(symbol_dict["Trading Symbol"], PremiumJackpot.channel_details),
                    "action": "Cancel"
                    if is_reply_msg and is_close_msg
                    else "Buy",
                }
                if __signal_details in signals:
                    raise CustomError("Signal already exists")
                else:
                    signals.append(__signal_details)
                signal_details = __signal_details.copy()
                signal_details["timestamp"] = f"{PremiumJackpot.channel_number}{self.msg_received_timestamp}"
                write_signals_to_csv(signal_details)
        except:
            failure_details = {
                "channel_name": "Premium jackpot",
                "timestamp": self.msg_received_timestamp,
                "message": self.message,
                "exception": traceback.format_exc().strip(),
            }
            logger.error(failure_details)
            write_failure_to_csv(failure_details)


class SmsOptionsPremium:
    split_words = ["BUY", "ONLY IN RANGE @", "ABOVE @", "TARGET", "SL FOR TRADE @ "]
    channel_details = CHANNEL_DETAILS["SmsOptionsPremium"]
    spot_sl = channel_details["spot_sl"]
    channel_number = channel_details["channel_number"]

    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        self.message = telegram_msg
        for misspelt_word, right_word in spell_checks.items():
            if misspelt_word in self.message:
                self.message = self.message.replace(misspelt_word, right_word)

    def get_closest_match(self, symbol):
        if symbol in spell_checks:
            symbol = spell_checks[symbol]
        if symbol in all_symbols:
            return symbol
        raise CustomError("Closest match is not found")

    def get_instrument_name(self, symbol_from_tg):
        # FinNifty 9 Jan 21450 PE
        try:
            # sym, date, month, strike, option_type = symbol_from_tg.split()
            # pos = re.findall(r"\d+", date)
            # if pos:
            #     date_int = int(pos[0])
            #     date = f"{date_int:02d}"
            # else:
            #     raise CustomError(f"date is not found in {date}")
            # try:
            #     date_obj = datetime.strptime(month.strip(), "%b")
            #     month = f"{date_obj.month:02d}"
            # except:
            #     raise CustomError(traceback.format_exc())
            sym_split = symbol_from_tg.split()
            if sym_split[-1].upper().strip() in ('CE', 'PE'):
                sym = sym_split[0]
                if len(sym_split[-1].upper().strip()) == 2:
                    strike = sym_split[-2].strip()
                    option_type = sym_split[-1].strip()
                else:
                    strike = sym_split[-1].strip()[:-2]
                    option_type = sym_split[-1].strip()[-2:]
            sym = self.get_closest_match(sym)
            exch = "BFO" if sym in ["SENSEX", "BANKEX"] else "NFO"
            filtered_df = scrip_info_df[
                (scrip_info_df["Exch"] == exch)
                & (scrip_info_df["Symbol"] == sym)
                & (scrip_info_df["Strike Price"] == float(strike))
                & (scrip_info_df["Option Type"] == option_type)
                # & (scrip_info_df["Expiry Date"] == f"2024-{month}-{date}")
            ]
            filtered_df = filtered_df.sort_values(by="Expiry Date")
            filtered_df['Expiry Date'] = pd.to_datetime(filtered_df['Expiry Date'], format='%Y-%m-%d')
            filtered_df = filtered_df[filtered_df['Expiry Date'] >= np.datetime64(datetime.now().date())]
            first_row = filtered_df.head(1)
            return first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0], sym
        except:
            raise CustomError(traceback.format_exc())

    def get_float_values(self, string_val, start_val):
        float_values = []
        v = string_val.split(start_val)
        for word in v[1].split():
            if word.replace("+", "").replace(".", "", 1).isdigit():
                float_values.append(word)
            else:
                break
        return float_values

    def get_spot_signal(self, message):
        # Now If Spot BankNifty Crosses & Sustains Above 45728.85 We May See A Short Covering Rally Of 60 - 90 - 150 Plus Points
        # BankNifty 7 Feb 45800 CE If Crosses & Sustain Only Above 253.85 Will Try To Hit Targets @ 275 300 330 360 400 & Above

        # split_words = ('ABOVE ', 'TARGETS @ ')
        message = message.replace("   ", "|")

        for statement in message.split("|"):
            try:
                statement = statement.strip()
                if not statement:
                    continue
                symbol_d = None
                for i, word in enumerate(statement.upper().split()):
                    if word.upper() in ('PE', 'CE') and i < 5:
                        symbol_d = " ".join(statement.split()[:i+1])
                        break
                if not symbol_d:
                    continue
                # symbol_d = " ".join(statement.split()[:5])
                symbol_dict, sym = self.get_instrument_name(symbol_d)
                key_to_chk = f"INCLUDE_INDEX_OPTION_{sym}"
                if int(SmsOptionsPremium.channel_details.get(key_to_chk,'1')) == 0:
                    raise CustomError(f"{sym} is turned off as per config")
                else:
                    if int(SmsOptionsPremium.channel_details.get('INCLUDE_STOCK_OPTIONS','1')) == 0:
                        raise CustomError(f"STOCK_OPTIONS is turned off as per config")
                ltp_range = self.get_float_values(statement, "ABOVE ")
                
                ltps = self.get_float_values(statement, "ABOVE ")
                ltp_max = max(
                    [float(ltp) for ltp in ltps if ltp.replace('.', '', 1).isdigit()])
                sl, targets = None, None
                if sym in index_options:
                    sl, targets = get_sl_target_from_csv(sym, ltp_max)
                if not sl and not targets:
                    targets = self.get_float_values(statement, "TARGETS @ ")
                    if float(targets[0]) < float(ltps[0]):
                        targets = [str(float(target) + ltp_max)
                                for target in targets if target.replace('.', '', 1).isdigit()]
                    sl = math.floor(float(ltp_range[0]) * (1 - SmsOptionsPremium.spot_sl))
                _signal_details = {
                    "channel_name": "SmsOptionsPremium",
                    "symbol": symbol_dict["Exch"]+":"+symbol_dict["Trading Symbol"],
                    "ltp_range": "|".join(ltps),
                    "target_range": "|".join(targets),
                    "sl": sl if sl and float(sl) > 0 else 0.05,
                    "quantity": get_multiplier(symbol_dict["Trading Symbol"], SmsOptionsPremium.channel_details),
                    "action": "Buy"
                }
                if _signal_details in signals:
                    raise CustomError("Signal already exists")
                else:
                    signals.append(_signal_details)
                signal_details = _signal_details.copy()
                signal_details["timestamp"] = f"{SmsOptionsPremium.channel_number}{self.msg_received_timestamp}"
                write_signals_to_csv(signal_details)
            except:
                failure_details = {
                    "channel_name": "SmsOptionsPremium",
                    "timestamp": self.msg_received_timestamp,
                    "message": statement,
                    "exception": traceback.format_exc().strip(),
                }
                logger.error(failure_details)
                write_failure_to_csv(failure_details)
    
    def extract_percentages_split(self, sentence):
        words = sentence.split()
        for word in words:
            if '%' in word:
                numeric_value = float(word.rstrip('%'))
                if numeric_value >= 80:
                    return True
        return False
    
    def get_hedge_trade(self, statement):
        try:
            is_reply_msg = '$$$$' in statement 
            is_reply_greater_than_80 = False
            if is_reply_msg:
                is_reply_greater_than_80 = self.extract_percentages_split(statement.split('$$$$')[-1])
            statements = [s.strip() for s in statement.split("             ")]
            details = {}
            for stmt in statements:
                stmt = stmt.strip().removeprefix("BUY").strip()
                _s = stmt.split()
                sym = None
                strike = None
                option_type = None
                if str(_s[0]).isdigit() and _s[1] == "LOT":
                    sym = _s[2]
                    for i, __s in enumerate(_s[3:]):
                        if str(__s).isdigit():
                            strike = __s
                            option_type = _s[4+i]
                            break
                        else:
                            continue
                else:
                    sym = _s[0]
                    if str(_s[1]).isdigit() and int(_s[1]) > 30 :
                        strike = _s[1] 
                        option_type = _s[2]
                    elif str(_s[2]).isdigit() and int(_s[2]) > 30 :
                        strike = _s[2] 
                        option_type = _s[3]
                    elif str(_s[3]).isdigit() and int(_s[3]) > 30 :
                        strike = _s[3] 
                        option_type = _s[4]
                if not sym:
                    raise CustomError(f"sym is not found in {statement}")
                if not strike:
                    raise CustomError(f"strike is not found in {statement}")
                if not option_type:
                    raise CustomError(f"option_type is not found in {statement}")
                sym = self.get_closest_match(sym)
                exch = "BFO" if sym in ["SENSEX", "BANKEX"] else "NFO"
                # print(sym, option_type, strike, get_updated_strike_price(sym, strike, option_type))
                filtered_df = scrip_info_df[
                    (scrip_info_df["Exch"] == exch)
                    & (scrip_info_df["Symbol"] == sym)
                    # & (scrip_info_df["Strike Price"] == float(strike))
                    & (scrip_info_df["Strike Price"] == float(get_updated_strike_price(sym, strike, option_type)))
                    & (scrip_info_df["Option Type"] == option_type)
                    # & (scrip_info_df["Expiry Date"] == f"2024-{month}-{date}")
                ]
                filtered_df = filtered_df.sort_values(by="Expiry Date")
                filtered_df['Expiry Date'] = pd.to_datetime(filtered_df['Expiry Date'], format='%Y-%m-%d')
                filtered_df = filtered_df[filtered_df['Expiry Date'] >= np.datetime64(datetime.now().date())]
                first_row = filtered_df.head(1)
                symbol_dict = first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0]
                _sym = symbol_dict["Exch"]+":"+symbol_dict["Trading Symbol"] if not details else details['symbol'] + "|" + symbol_dict["Exch"]+":"+symbol_dict["Trading Symbol"]
                if not details:
                    quantity = get_multiplier(symbol_dict["Trading Symbol"], SmsOptionsPremium.channel_details, special_case="HEDGE")
                else:
                    quantity = quantity + "|" + get_multiplier(symbol_dict["Trading Symbol"], SmsOptionsPremium.channel_details, special_case="HEDGE")
                details = {
                        "channel_name": "SmsOptionsPremium",
                        "symbol": _sym,
                        "ltp_range": 0,
                        "target_range": 0,
                        "sl": 0,
                        "quantity": quantity,
                        "action": "BHEDGE" if not is_reply_greater_than_80 else "BHXXX",
                        "timestamp": f"{SmsOptionsPremium.channel_number}{self.msg_received_timestamp}"
                    }
                # logger.info(details)
            write_signals_to_csv(details)
                    
        except:
            failure_details = {
                "channel_name": "SmsOptionsPremium",
                "timestamp": self.msg_received_timestamp,
                "message": statement,
                "exception": traceback.format_exc().strip(),
            }
            logger.error(failure_details)
            write_failure_to_csv(failure_details)
    
    def get_signal(self):
        statement = self.message.strip().upper()
        new_msg = self.message.strip().upper().split('$$$$')[-1]
        is_close_msg = any([word in new_msg.split() for word in close_words])
        is_sl_message = "SL FOR TRADE @ " in statement.split('$$$$')[-1]
        is_spot_message = "SPOT" in statement
        is_reply_msg = '$$$$' in statement
        if is_spot_message:
            self.get_spot_signal(statement)
            return
        elif len(statement.split("BUY ")) >= 3 and len(statement.upper().split("RANGE")) >= 3:
            if int(SmsOptionsPremium.channel_details.get('INCLUDE_HEDGE','1')) == 0:
                raise CustomError(f"Hedge is turned off as per config")
            self.get_hedge_trade(statement)
            return
        elif is_reply_msg and (is_close_msg or is_sl_message):
            # is a reply message and has close words in it:
            pass
        elif not is_reply_msg:
            # is not a reply message
            failure_details = {
                "channel_name": "SmsOptionsPremium",
                "timestamp": self.msg_received_timestamp,
                "message": self.message,
                "exception": "Not a reply message",
            }
            logger.error("Not a reply message")
            write_failure_to_csv(failure_details)
            return
        elif is_reply_msg and not is_close_msg and not is_sl_message:
            # is a reply message but not having close words
            # duplicate or junk
            failure_details = {
                "channel_name": "SmsOptionsPremium",
                "timestamp": self.msg_received_timestamp,
                "message": self.message,
                "exception": "is a reply message but not having close words and sl message. Possible duplicate or junk",
            }
            write_failure_to_csv(failure_details)
            return

        for word in SmsOptionsPremium.split_words:
            statement = statement.replace(word, "|")
        parts = statement.split("|")
        try:
            if is_reply_msg and is_close_msg:
                sl = zero_sl
            # else:
            #     sl = re.findall(r"(\d+)?", parts[4])[0]
            #     if not sl:
            #         sl = re.findall(r"(\d+)?", statement.split("$$$$")[-1])[0]
            #         if not sl:
            #             raise CustomError(f"SL is not found in {parts[4]}")
            symbol_dict, sym = self.get_instrument_name(parts[1].upper().strip())
            ltps = re.findall(r"\d+\.\d+|\d+", parts[2])
            ltp_max = max([float(ltp) for ltp in ltps
                          if ltp.replace('.', '', 1).isdigit()])
            
            sl, targets = None, None
            try:
                targets = self.get_float_values(
                    self.message.strip().upper(), "TARGET")
                
                if float(targets[0]) < float(ltps[0]):
                    targets = [str(float(target) + ltp_max)
                            for target in targets if target.replace('.', '', 1).isdigit()]
                if is_reply_msg and is_close_msg:
                    sl = zero_sl
                else:
                    sl = re.findall(r"(\d+)?", parts[4])[0]
                    if not sl:
                        sl = re.findall(r"(\d+)?", statement.split("$$$$")[-1])[0]
                        if not sl:
                            raise CustomError(f"SL is not found in {parts[4]}")
            except:
                sl, targets = None, None

            if sym in index_options:
                key_to_chk = f"INCLUDE_INDEX_OPTION_{sym}"
                if int(SmsOptionsPremium.channel_details.get(key_to_chk,'1')) == 0:
                    raise CustomError(f"{sym} is turned off as per config")
                if not sl or not targets:
                    sl, targets = get_sl_target_from_csv(sym, ltp_max)
            else:
                if int(SmsOptionsPremium.channel_details.get('INCLUDE_STOCK_OPTIONS','1')) == 0:
                    raise CustomError(f"STOCK_OPTIONS is turned off as per config")
                
            if not sl or not targets:
                raise CustomError("Target and SL is not availble in msg")
            
            _signal_details = {
                "channel_name": "SmsOptionsPremium",
                "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                "ltp_range": "|".join(ltps),
                "target_range": "|".join(targets),
                "sl": sl,
                "quantity": get_multiplier(symbol_dict["Trading Symbol"], SmsOptionsPremium.channel_details),
                "action": "Cancel" if is_reply_msg and is_close_msg else "Buy",
            }
            if _signal_details in signals:
                raise CustomError("Signal already exists")
            signals.append(_signal_details)
            signal_details = _signal_details.copy()
            signal_details["timestamp"] = f"{SmsOptionsPremium.channel_number}{self.msg_received_timestamp}"
            write_signals_to_csv(signal_details)
        except:
            failure_details = {
                "channel_name": "SmsOptionsPremium",
                "timestamp": self.msg_received_timestamp,
                "message": self.message,
                "exception": traceback.format_exc().strip(),
            }
            logger.error(failure_details)
            write_failure_to_csv(failure_details)


class PaidCallPut:
    channel_details = CHANNEL_DETAILS["PaidCallPut"]
    channel_number = channel_details["channel_number"]

    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        self.message = telegram_msg
        if "EXPIRY" in self.message.upper():
            self.message = self.message.replace("EXPIRY", "")
        self.message = self.message.removeprefix("#").strip()
        
        for misspelt_word, right_word in spell_checks.items():
            if misspelt_word in self.message:
                self.message = self.message.replace(misspelt_word, right_word)
                
        
        for word in self.message.split("BUY ")[0].split():
            print(f"{word=}")
            if word in index_options or word in all_symbols:
                self.message = self.message.replace("BUY ", "BUY "+word+" ")
                break
        if "  BUY " in self.message:
            self.message = "BUY " + self.message.split("  BUY ")[1]
        print(f"{self.message=}")

    def get_symbol_from_message(self, message):
        for word in message.upper().split():
            if word in spell_checks:
                word = spell_checks[word]
            word = word.strip()
            if word in all_symbols:
                return word
        return "BANKNIFTY"

    def get_float_values(self, string_val, start_val):
        float_values = []
        v = string_val.split(start_val)
        for word in v[1].split():
            if word.replace(".", "", 1).isdigit():
                float_values.append(word)
            else:
                break
        return float_values

    def coin_option_name(self, df, symbol, strike, option_type):
        exch = "BFO" if symbol in ["SENSEX", "BANKEX"] else "NFO"
        filtered_df = df[
            (df["Exch"] == exch)
            & (df["Symbol"] == symbol)
            & (df["Strike Price"] == float(strike))
            & (df["Option Type"] == option_type)
            # & (df["Expiry Date"] == f"2024-{month}-{date}")
        ]
        filtered_df = filtered_df.sort_values(by="Expiry Date")
        filtered_df['Expiry Date'] = pd.to_datetime(filtered_df['Expiry Date'], format='%Y-%m-%d')
        filtered_df = filtered_df[filtered_df['Expiry Date'] >= np.datetime64(datetime.now().date())]
        first_row = filtered_df.head(1)
        return first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0]

    def get_target_values(self, string_val, start_val):
        float_values = []
        try:
            v = string_val.upper().replace("-", " ").replace("+",
                                                             " ").replace("/", " ").split(start_val)
            for word in v[1].strip().split():
                if word.replace(".", "", 1).isdigit():
                    float_values.append(word)
                else:
                    break
        except:
            pass
        return float_values

    def get_signal(self):
        try:
            new_msg = self.message.strip().upper().split('$$$$')[-1]
            is_close_msg = any([word in new_msg.split()
                               for word in close_words])
            is_reply_msg = '$$$$' in self.message
            if is_reply_msg and is_close_msg:
                # is a reply message and has close words in it:
                pass
            elif not is_reply_msg:
                # is not a reply message
                pass
            elif is_reply_msg and not is_close_msg:
                # is a reply message but not having close words
                # duplicate or junk
                failure_details = {
                    "channel_name": "PaidCallPut",
                    "timestamp": self.msg_received_timestamp,
                    "message": self.message,
                    "exception": "is a reply message but not having close words. Possible duplicate or junk",
                }
                logger.error(failure_details)
                write_failure_to_csv(failure_details)
                return

            symbol = self.get_symbol_from_message(self.message)
            # req_content = self.message.split("expiry")
            # req_content_list = req_content[0].strip().split()
            # if len(req_content_list) >= 2:
            #     pos = re.findall(r"\d+", req_content_list[-2])
            #     if pos:
            #         date_int = int(pos[0] )
            #         date = f"{date_int:02d}"
            #     else:
            #         raise CustomError(f"Date is not found in {req_content_list[-2]}")
            #     try:
            #         date_obj = datetime.strptime(req_content_list[-1].strip(), "%b")
            #         month = f"{date_obj.month:02d}"
            #     except:
            #         raise CustomError(traceback.format_exc())
            # else:
            #     raise CustomError(f"Date and month is not found in {req_content_list}")
            req_content = self.message.split()
            strike = None
            option = None
            for i, word in enumerate(req_content):
                if (
                    word.upper().strip() == "BUY"
                    and i + 2 <= len(req_content) + 1
                    and strike == None
                ):
                    strike = req_content[i + 1].strip() if req_content[i + 1].strip().isdigit() else req_content[i + 2].strip()
                    option = req_content[i + 2].strip() if req_content[i + 1].strip().isdigit() else req_content[i + 3].strip()
                # if word.upper().strip().startswith("SL-"):
                #     sl = re.findall(r"SL-(\d+)?", word.upper().strip())[0]
            if strike == None or option == None:
                raise CustomError("Strike or Option is None")
            
            symbol_dict = self.coin_option_name(
                # scrip_info_df, symbol, date, month, strike, option
                scrip_info_df, symbol, strike, option
            )
            ltp_words = ('ABV', 'CMP', 'ABOVE')
            ltp_range = None
            for word in ltp_words:
                ltp_range = self.get_target_values(self.message, word)
                if ltp_range:
                    break
            else:
                raise CustomError("ltp_range values is not found")
            ltps = ltp_range
            ltp_max = max([float(ltp) for ltp in ltps
                          if ltp.replace('.', '', 1).isdigit()])
            sl, targets = None, None
            try:
                sl_list = self.get_float_values(
                    self.message.strip().upper().replace("-", ""), "SL")
                if sl_list:
                    sl = sl_list[0]
                else:
                    raise CustomError("SL is not available")

                targets = self.get_target_values(
                    self.message.replace("TARGTE", "TARGET"), "TARGET")
                if float(targets[0]) < float(ltps[0]):
                    targets = [str(float(target) + ltp_max)
                            for target in targets if target.replace('.', '', 1).isdigit()]
            except:
                sl, targets = None, None
            if symbol in index_options:
                key_to_chk = f"INCLUDE_INDEX_OPTION_{symbol}"
                if int(PaidCallPut.channel_details.get(key_to_chk,'1')) == 0:
                    raise CustomError(f"{symbol} is turned off as per config")
                if not sl or not targets:
                    sl, targets = get_sl_target_from_csv(symbol, ltp_max)
            else:
                if int(PaidCallPut.channel_details.get('INCLUDE_STOCK_OPTIONS','1')) == 0:
                    raise CustomError(f"STOCK_OPTIONS is turned off as per config")
            if not sl or not targets:
                raise CustomError("Target and SL is not availble in msg")
            _signal_details = {
                "channel_name": "PaidCallPut",
                "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                "ltp_range": "|".join(ltps),
                "target_range": "|".join(targets),
                "sl": sl,
                "quantity": get_multiplier(symbol_dict["Trading Symbol"], PaidCallPut.channel_details),
                "action": "Cancel" if is_reply_msg and is_close_msg else "Buy",
            }
            if _signal_details in signals:
                raise CustomError("Signal already exists")
            else:
                signals.append(_signal_details)
            signal_details = _signal_details.copy()
            signal_details["timestamp"] = f"{PaidCallPut.channel_number}{self.msg_received_timestamp}"
            write_signals_to_csv(signal_details)
        except:
            failure_details = {
                "channel_name": "PaidCallPut",
                "timestamp": self.msg_received_timestamp,
                "message": self.message,
                "exception": traceback.format_exc().strip(),
            }
            logger.error(failure_details)
            write_failure_to_csv(failure_details)


class PaidStockIndexOption:
    channel_details = CHANNEL_DETAILS["PaidStockIndexOption"]
    channel_number = channel_details["channel_number"]
    sl = channel_details["default_sl"]

    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        self.message = telegram_msg
        try:
            self.message_upper = telegram_msg.upper().split("BUY ")[1].replace(
                "  ", " ").replace("\n", " ").replace("-", " ").replace("/", " ").strip()
        except:
            self.message_upper = telegram_msg.upper()
        for misspelt_word, right_word in spell_checks.items():
            if misspelt_word in self.message:
                self.message = self.message.replace(misspelt_word, right_word)
                

    def get_target_values(self, string_val, start_val):
        float_values = []
        try:
            v = string_val.upper().replace("-", " ").replace("+",
                                                             " ").replace("/", " ").split(start_val)
            for word in v[1].strip().split():
                if word.replace(".", "", 1).isdigit():
                    float_values.append(word)
                else:
                    break
        except:
            pass
        return float_values

    def coin_option_name(self, df, symbol, strike, option_type):
        exch = "BFO" if symbol in ["SENSEX", "BANKEX"] else "NFO"
        if symbol in spell_checks:
            symbol = spell_checks[symbol]
        filtered_df = df[
            (df["Exch"] == exch)
            & (df["Symbol"] == symbol)
            & (df["Strike Price"] == float(strike))
            & (df["Option Type"] == option_type)
        ]
        filtered_df = filtered_df.sort_values(by="Expiry Date")
        filtered_df['Expiry Date'] = pd.to_datetime(filtered_df['Expiry Date'], format='%Y-%m-%d')
        filtered_df = filtered_df[filtered_df['Expiry Date'] >= np.datetime64(datetime.now().date())]
        first_row = filtered_df.head(1)
        return first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0]

    def get_signal(self):
        try:
            new_msg = self.message.strip().upper().split('$$$$')[-1]
            is_close_msg = any([word in new_msg.split()
                               for word in close_words])
            is_reply_msg = '$$$$' in self.message
            if is_reply_msg and is_close_msg:
                # is a reply message and has close words in it:
                pass
            elif not is_reply_msg:
                # is not a reply message
                pass
            elif is_reply_msg and not is_close_msg:
                # is a reply message but not having close words
                # duplicate or junk
                failure_details = {
                    "channel_name": "PaidStockIndexOption",
                    "timestamp": self.msg_received_timestamp,
                    "message": self.message,
                    "exception": "is a reply message but not having close words. Possible duplicate or junk",
                }
                logger.error(failure_details)
                write_failure_to_csv(failure_details)
                return
            
            if "FUTURE TRADE" in self.message:
                if int(PaidStockIndexOption.channel_details.get('INCLUDE_FUT','1')) == 0:
                    raise CustomError(f"Future is turned off as per config")
                sym_fut = self.message.split('RANGE')
                sym = sym_fut[0].split()[-1]
                symbol_dict = get_fut_instrument_name(sym)
                ltps = get_float_values(self.message, "RANGE")
                if not ltps:
                    raise CustomError(f"ltps is not found in {self.message}")
                targets = get_float_values(self.message, "TRG")
                if not targets:
                    raise CustomError(f"targets is not found in {self.message}")
                sl = get_float_values(self.message, "SL")
                if not sl:
                    raise CustomError(f"SL is not found in {self.message}")
                sl = sl[0]
                if 'SELL' in self.message.split():
                    action = "SHORT"
                elif 'BUY' in self.message.split():
                    action = "BUY"
                else:
                    action = None
                if is_reply_msg and is_close_msg:
                    action = "CANCEL"
                
                if not action:
                    raise CustomError(f"could not decide action in {self.message}")

                __signal_details = {
                    "channel_name": "PaidStockIndexOption",
                    "symbol": symbol_dict["Exch"]+":"+symbol_dict["Trading Symbol"],
                    "ltp_range": "|".join(ltps),
                    "target_range": "|".join(targets),
                    "sl": sl,
                    "quantity": get_multiplier(symbol_dict["Trading Symbol"], PaidStockIndexOption.channel_details),
                    "action": action,
                }
                if __signal_details in signals:
                    raise CustomError("Signal already exists")
                else:
                    signals.append(__signal_details)
                signal_details = __signal_details.copy()
                signal_details["timestamp"] = f"{PaidStockIndexOption.channel_number}{self.msg_received_timestamp}"
                write_signals_to_csv(signal_details)
            else:
                msg_split = [m.strip() for m in self.message_upper.split()]
                sym = msg_split[0]
                if str(msg_split[1]).endswith('PE'):
                    option_type = "PE"
                    strike = str(msg_split[1])[:-2]
                elif str(msg_split[1]).endswith('CE'):
                    option_type = "CE"
                    strike = str(msg_split[1])[:-2]
                elif ("BAJAJ" == msg_split[0] and "AUTO" == msg_split[1]) or ("MCDOWELL" == msg_split[0] and "N" == msg_split[1]):
                    sym=f"{msg_split[0]}-{msg_split[1]}"
                    option_type = msg_split[3]
                    strike = str(msg_split[2])
                else:
                    option_type = str(msg_split[2])
                    strike = msg_split[1]
                symbol_dict = self.coin_option_name(
                    scrip_info_df, sym, strike, option_type
                )

                ltp_words = ["RANGE", "ABOVE", "NEAR LEVEL", "NEAR"]
                target_words = ["TARGET", "TRG"]
                sl_words = ["SL", "STOPLOSS"]

                ltp_range = None
                for word in ltp_words:
                    ltp_range = self.get_target_values(self.message_upper, word)
                    if ltp_range:
                        break
                else:
                    raise CustomError("ltp_range values is not found")

                ltps = ltp_range
                ltp_max = max([float(ltp) for ltp in ltps
                            if ltp.replace('.', '', 1).isdigit()])
                sl, targets = None, None
                try:
                    sl_range = None
                    for word in sl_words:
                        sl_range = self.get_target_values(self.message_upper, word)
                        if sl_range:
                            break
                    if not sl_range:
                        for word in sl_words:
                            if word in self.message_upper:
                                v = self.message_upper.split(word)[1]
                                if not v.strip():
                                    continue
                                v_list = [v_.strip() for v_ in v.split() if v_.strip()]
                                if v_list[0] in ("TOMORROW", "PAID"):
                                    sl_range = [
                                        str(math.floor(float(ltp_range[0]) * (1 - PaidStockIndexOption.sl)))]
                                    break
                    # if not sl_range:
                    #     raise CustomError("sl_range values is not found")
                    sl = sl_range[0]
                    target_range = None
                    for word in target_words:
                        target_range = self.get_target_values(self.message_upper, word)
                        if target_range:
                            break
                    # else:
                    #     raise CustomError("target_range values is not found")
                    targets = target_range
                    if float(targets[0]) < float(ltps[0]):
                        targets = [str(float(target) + ltp_max)
                                for target in targets if target.replace('.', '', 1).isdigit()]
                except:
                    sl, targets = None, None

                if sym in index_options:
                    key_to_chk = f"INCLUDE_INDEX_OPTION_{sym}"
                    if int(PaidStockIndexOption.channel_details.get(key_to_chk,'1')) == 0:
                        raise CustomError(f"{sym} is turned off as per config")
                    if not sl or not targets:
                        sl, targets = get_sl_target_from_csv(sym, ltp_max)
                else:
                    if int(PaidStockIndexOption.channel_details.get('INCLUDE_STOCK_OPTIONS','1')) == 0:
                        raise CustomError(f"STOCK_OPTIONS is turned off as per config")
                if not sl or not targets:
                    raise CustomError("Target and SL is not availble in msg")
                    # else:
                    #     if sym in ('SENSEX', 'BANKEX', 'NIFTY', 'BANKNIFTY', 'MIDCPNIFTY', 'FINNIFTY'):
                    #         sl_range = [str(float(sl)/2) for sl in sl_range]
                    
                    
                _signal_details = {
                    "channel_name": "PaidStockIndexOption",
                    "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                    "ltp_range": "|".join(ltps),
                    "target_range": "|".join(targets),
                    "sl": sl,
                    "quantity": get_multiplier(symbol_dict["Trading Symbol"], PaidStockIndexOption.channel_details),
                    "action": "Cancel" if is_reply_msg and is_close_msg else "Buy",
                }
                if _signal_details in signals:
                    raise CustomError("Signal already exists")
                else:
                    signals.append(_signal_details)
                signal_details = _signal_details.copy()
                signal_details["timestamp"] = f"{PaidStockIndexOption.channel_number}{self.msg_received_timestamp}"
                write_signals_to_csv(signal_details)
        except:
            failure_details = {
                "channel_name": "PaidStockIndexOption",
                "timestamp": self.msg_received_timestamp,
                "message": self.message,
                "exception": traceback.format_exc().strip(),
            }
            logger.error(failure_details)
            write_failure_to_csv(failure_details)


class BnoPremium:
    split_words = ["BUY", "ABOVE", "NEAR", "SL", "TGT", "TARGET", "TARGE", "     "]
    channel_details = CHANNEL_DETAILS["BnoPremium"]
    channel_number = channel_details["channel_number"]

    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        self.message = telegram_msg
        for misspelt_word, right_word in spell_checks.items():
            if misspelt_word in self.message:
                self.message = self.message.replace(misspelt_word, right_word)
                
    def get_closest_match(self, symbol):
        if symbol in spell_checks:
            symbol = spell_checks[symbol]
        if symbol in all_symbols:
            return symbol
        raise CustomError("Closest match is not found")

    def get_instrument_name(self, symbol_from_tg):
        try:
            sym_split = symbol_from_tg.split()
            if len(sym_split) == 3:
                sym, strike, option_type = sym_split
            elif len(sym_split) > 3:
                sym, strike, option_type, *_ = sym_split
            elif len(sym_split) == 2:
                sym = sym_split[0]
                strike = sym_split[1].strip()[:-2]
                option_type = sym_split[1].strip()[-2:]
            sym = self.get_closest_match(sym)
            exch = "BFO" if sym in ["SENSEX", "BANKEX"] else "NFO"
            filtered_df = scrip_info_df[
                (scrip_info_df["Exch"] == exch)
                & (scrip_info_df["Symbol"] == sym)
                & (scrip_info_df["Strike Price"] == float(strike))
                & (scrip_info_df["Option Type"] == option_type)
            ]
            sorted_df = filtered_df.sort_values(by="Expiry Date")
            sorted_df['Expiry Date'] = pd.to_datetime(filtered_df['Expiry Date'], format='%Y-%m-%d')
            sorted_df = sorted_df[sorted_df['Expiry Date'] >= np.datetime64(datetime.now().date())]
            first_row = sorted_df.head(1)
            return first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0], sym
        except:
            raise CustomError(traceback.format_exc())

    def get_signal(self):
        try:
            statement = self.message
            
            is_reply_msg = "$$$$" in statement
            new_msg = self.message.upper().split("$$$$")[-1]
            is_close_msg = any([word in new_msg.split() for word in close_words])
            if is_reply_msg and is_close_msg:
                # is a reply message and has close words in it:
                pass
            elif not is_reply_msg:
                # is not a reply message
                pass
            elif is_reply_msg and not is_close_msg:
                # is a reply message but not having close words
                # duplicate or junk
                failure_details = {
                    "channel_name": "BnoPremium",
                    "timestamp": self.msg_received_timestamp,
                    "message": self.message,
                    "exception": "is a reply message but not having close words. Possible duplicate or junk",
                }
                logger.error(failure_details)
                write_failure_to_csv(failure_details)
                return
            statement = statement.split("$$$$")[0]
            for word in BnoPremium.split_words:
                statement = statement.replace(word, "|")
            parts = [i for ind, i in enumerate(statement.split("|")) if ind == 0 or (ind!=0 and i.strip())]
            symbol_from_tg = parts[1].strip().removeprefix("#")
            symbol_dict, sym = self.get_instrument_name(symbol_from_tg)
            parts_statement = statement.replace("CE", "|").replace("PE", "|").split("|")
            if "ABOVE" in self.message or "NEAR" in self.message:
                ltps = re.findall(r"\d+\.\d+|\d+", parts_statement[3].strip())
            else:
                ltps = re.findall(r"\d+\.\d+|\d+", parts_statement[2].strip())
            ltp_max = max(
                [float(ltp) for ltp in ltps if ltp.replace(".", "", 1).isdigit()]
            )
            sl, targets = None, None
            try:
                targets = re.findall(r"\d+\.\d+|\d+", parts[3])
                if float(targets[0]) < float(ltps[0]):
                    targets = [
                        str(float(target) + ltp_max)
                        for target in targets
                        if target.replace(".", "", 1).isdigit()
                    ]
                sl = re.findall(r"\d+\.\d+|\d+", parts[2])[0]
            except:
                targets = None
                sl = None
            if sym in index_options:
                key_to_chk = f"INCLUDE_INDEX_OPTION_{sym}"
                if int(BnoPremium.channel_details.get(key_to_chk,'1')) == 0:
                    raise CustomError(f"{sym} is turned off as per config")
                if not sl or not targets:
                    sl, targets = get_sl_target_from_csv(sym, ltp_max)
            else:
                if int(BnoPremium.channel_details.get('INCLUDE_STOCK_OPTIONS','1')) == 0:
                    raise CustomError("STOCK_OPTIONS is turned off as per config")
            if not targets or not sl:
                raise CustomError("Target and SL is not availble in msg")
            
            __signal_details = {
                "channel_name": "BnoPremium",
                "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                "ltp_range": "|".join(ltps),
                "target_range": "|".join(targets),
                "sl": sl,
                "quantity": get_multiplier(
                    symbol_dict["Trading Symbol"],
                    BnoPremium.channel_details,
                ),
                "action": "Cancel" if is_reply_msg and is_close_msg else "Buy",
            }
            if __signal_details in signals:
                raise CustomError("Signal already exists")
            else:
                signals.append(__signal_details)
            signal_details = __signal_details.copy()
            signal_details["timestamp"] = (
                f"{BnoPremium.channel_number}{self.msg_received_timestamp}"
            )
            write_signals_to_csv(signal_details)
        except:
            failure_details = {
                "channel_name": "BnoPremium",
                "timestamp": self.msg_received_timestamp,
                "message": self.message,
                "exception": traceback.format_exc().strip(),
            }
            logger.error(failure_details)
            write_failure_to_csv(failure_details)


class StockPremium:
    split_words = ["ABOVE", "SL", "TGT"]
    channel_details = CHANNEL_DETAILS["StockPremium"]
    channel_number = channel_details["channel_number"]

    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        self.message = telegram_msg.strip().removeprefix("#").strip().removeprefix('BUY').strip()
        for misspelt_word, right_word in spell_checks.items():
            if misspelt_word in self.message:
                self.message = self.message.replace(misspelt_word, right_word)
                
    def get_closest_match(self, symbol):
        if symbol in spell_checks:
            symbol = spell_checks[symbol]
        if symbol in all_symbols:
            return symbol
        raise CustomError("Closest match is not found")

    def get_instrument_name(self, symbol_from_tg):
        try:
            sym_split = symbol_from_tg.split()
            if len(sym_split) == 3:
                sym, strike, option_type = sym_split
            elif len(sym_split) > 3:
                sym, strike, option_type, *_ = sym_split
            elif len(sym_split) == 2:
                sym = sym_split[0]
                strike = sym_split[1].strip()[:-2]
                option_type = sym_split[1].strip()[-2:]
            sym = self.get_closest_match(sym)
            exch = "BFO" if sym in ["SENSEX", "BANKEX"] else "NFO"
            filtered_df = scrip_info_df[
                (scrip_info_df["Exch"] == exch)
                & (scrip_info_df["Symbol"] == sym)
                & (scrip_info_df["Strike Price"] == float(strike))
                & (scrip_info_df["Option Type"] == option_type)
            ]
            sorted_df = filtered_df.sort_values(by="Expiry Date")
            sorted_df['Expiry Date'] = pd.to_datetime(filtered_df['Expiry Date'], format='%Y-%m-%d')
            sorted_df = sorted_df[sorted_df['Expiry Date'] >= np.datetime64(datetime.now().date())]
            first_row = sorted_df.head(1)
            return first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0], sym
        except:
            raise CustomError(traceback.format_exc())

    def get_signal(self):
        try:
            statement = self.message
            is_reply_msg = "$$$$" in statement
            new_msg = self.message.upper().split("$$$$")[-1]
            is_close_msg = any([word in new_msg.split() for word in close_words])
            if is_reply_msg and is_close_msg:
                # is a reply message and has close words in it:
                pass
            elif not is_reply_msg:
                # is not a reply message
                pass
            elif is_reply_msg and not is_close_msg:
                # is a reply message but not having close words
                # duplicate or junk
                failure_details = {
                    "channel_name": "StockPremium",
                    "timestamp": self.msg_received_timestamp,
                    "message": self.message,
                    "exception": "is a reply message but not having close words. Possible duplicate or junk",
                }
                logger.error(failure_details)
                write_failure_to_csv(failure_details)
                return

            for word in StockPremium.split_words:
                statement = statement.replace(word, "|")
            parts = statement.split("|")
            symbol_from_tg = parts[0].strip()
            symbol_dict, sym = self.get_instrument_name(symbol_from_tg)
            ltps = re.findall(r"\d+\.\d+|\d+", parts[1].strip())
            ltp_max = max(
                [float(ltp) for ltp in ltps if ltp.replace(".", "", 1).isdigit()]
            )
            sl, targets = None, None
            if sym in index_options:
                key_to_chk = f"INCLUDE_INDEX_OPTION_{sym}"
                if int(StockPremium.channel_details.get(key_to_chk,'1')) == 0:
                    raise CustomError(f"{sym} is turned off as per config")
                sl, targets = get_sl_target_from_csv(sym, ltp_max)
            else:
                if int(StockPremium.channel_details.get('INCLUDE_STOCK_OPTIONS','1')) == 0:
                    raise CustomError(f"STOCK_OPTIONS is turned off as per config")
            if not sl and not targets:
                targets = re.findall(r"\d+\.\d+|\d+", parts[3].strip())
                if float(targets[0]) < float(ltps[0]):
                    targets = [
                        str(float(target) + ltp_max)
                        for target in targets
                        if target.replace(".", "", 1).isdigit()
                    ]
                sl = re.findall(r"\d+\.\d+|\d+", parts[2].strip())[0]
            __signal_details = {
                "channel_name": "StockPremium",
                "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                "ltp_range": "|".join(ltps),
                "target_range": "|".join(targets),
                "sl": sl,
                "quantity": get_multiplier(
                    symbol_dict["Trading Symbol"],
                    StockPremium.channel_details,
                ),
                "action": "Cancel" if is_reply_msg and is_close_msg else "Buy",
            }
            if __signal_details in signals:
                raise CustomError("Signal already exists")
            else:
                signals.append(__signal_details)
            signal_details = __signal_details.copy()
            signal_details["timestamp"] = (
                f"{StockPremium.channel_number}{self.msg_received_timestamp}"
            )
            write_signals_to_csv(signal_details)
        except:
            failure_details = {
                "channel_name": "StockPremium",
                "timestamp": self.msg_received_timestamp,
                "message": self.message,
                "exception": traceback.format_exc().strip(),
            }
            logger.error(failure_details)
            write_failure_to_csv(failure_details)


class StudentsGroup:
    split_words = [
        "ABOVE",
        "ABV",
        "@",
        " AT",
        "SL",
        "TARGETS",
        "TARGET",
        "TRT",
        "TARTE",
        "TARGE",
        "TG",
        "STOPLOSS",
        "TRG",
        "NEAR",
        "NOW",
    ]
    channel_details = CHANNEL_DETAILS["StudentsGroup"]
    channel_number = channel_details["channel_number"]

    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        try:
            self.message_as_is = telegram_msg
            self.message = (
                telegram_msg.upper().split("BUY ")[1].replace("-", " ").replace(",", " ").replace("/", " ")
            )
            self.message = self.message.replace("BELOW","")
        except:
            self.message = (
                telegram_msg.upper().replace("-", " ").replace(",", " ").replace("/", " ")
            )
        for misspelt_word, right_word in spell_checks.items():
            if misspelt_word in self.message:
                self.message = self.message.replace(misspelt_word, right_word)
                
    def get_float_values(self, string_val, start_val, split_values):
        float_values = []
        v = string_val.split(start_val)
        for word in re.split(split_values, v[1]):
            if not word.strip():
                continue
            if word.replace("+", "").replace(".", "", 1).isdigit():
                float_values.append(word.replace("+", ""))
            else:
                break
        return float_values

    def get_closest_match(self, symbol):
        if symbol in spell_checks:
            symbol = spell_checks[symbol]
        if symbol in all_symbols:
            return symbol
        raise CustomError("Closest match is not found")

    def get_instrument_name(self, symbol_from_tg):
        try:
            sym_split = symbol_from_tg.split()
            print(sym_split)
            if len(sym_split) == 3:
                sym, strike, option_type = sym_split
            # elif len(sym_split) > 3:
            #     sym, strike, option_type, *_ = sym_split
            elif len(sym_split) == 2:
                sym = sym_split[0]
                strike = sym_split[1].strip()[:-2]
                option_type = sym_split[1].strip()[-2:]
            elif sym_split[-1].upper().strip() in ('CE', 'PE'):
                sym = sym_split[0]
                if len(sym_split[-1].upper().strip()) == 2:
                    strike = sym_split[-2].strip()
                    option_type = sym_split[-1].strip()
                else:
                    strike = sym_split[-1].strip()[:-2]
                    option_type = sym_split[-1].strip()[-2:]
            sym = self.get_closest_match(sym)
            exch = "BFO" if sym in ["SENSEX", "BANKEX"] else "NFO"
            filtered_df = scrip_info_df[
                (scrip_info_df["Exch"] == exch)
                & (scrip_info_df["Symbol"] == sym)
                & (scrip_info_df["Strike Price"] == float(strike))
                & (scrip_info_df["Option Type"] == option_type)
            ]
            sorted_df = filtered_df.sort_values(by="Expiry Date")
            sorted_df['Expiry Date'] = pd.to_datetime(filtered_df['Expiry Date'], format='%Y-%m-%d')
            sorted_df = sorted_df[sorted_df['Expiry Date'] >= np.datetime64(datetime.now().date())]
            first_row = sorted_df.head(1)
            return first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0], sym
        except:
            raise CustomError(traceback.format_exc())

    def get_signal(self):
        try:
            statement = self.message.replace("@","")
            is_reply_msg = "$$$$" in statement
            new_msg = self.message.upper().split("$$$$")[-1]
            is_close_msg = any([word in new_msg.split() for word in close_words])
            if is_reply_msg and is_close_msg:
                # is a reply message and has close words in it:
                pass
            elif not is_reply_msg:
                # is not a reply message
                pass
            elif is_reply_msg and not is_close_msg:
                # is a reply message but not having close words
                # duplicate or junk
                failure_details = {
                    "channel_name": "StudentsGroup",
                    "timestamp": self.msg_received_timestamp,
                    "message": self.message,
                    "exception": "is a reply message but not having close words. Possible duplicate or junk",
                }
                logger.error(failure_details)
                write_failure_to_csv(failure_details)
                return
            
            if "BTST" in self.message:
                if int(StudentsGroup.channel_details.get('INCLUDE_BTST','1')) == 0:
                    raise CustomError(f"BTST is turned off as per config")
                
                statement = self.message.replace("BTST", "").strip().replace("CALL", "").replace("TRADE", "").strip().replace("BUY", "").strip().replace("ABOVE", "").replace("-", " ").replace("/", " ").replace("NEAR","")
                v = [s for s in statement.split() if s.strip()]
                sym = v[0]
                try:
                    strike = float(v[1])
                    strike = v[1] if strike >= 31 else v[3]
                    option_type = v[2]
                except:
                    strike = v[3]
                    option_type = v[4]
                sym = self.get_closest_match(sym)
                exch = "BFO" if sym in ["SENSEX", "BANKEX"] else "NFO"
                filtered_df = scrip_info_df[
                    (scrip_info_df["Exch"] == exch)
                    & (scrip_info_df["Symbol"] == sym)
                    # & (scrip_info_df["Strike Price"] == float(strike))
                    & (scrip_info_df["Strike Price"] == float(get_updated_strike_price(sym, strike, option_type)))
                    & (scrip_info_df["Option Type"] == option_type)
                ]
                sorted_df = filtered_df.sort_values(by="Expiry Date")
                sorted_df['Expiry Date'] = pd.to_datetime(filtered_df['Expiry Date'], format='%Y-%m-%d')
                sorted_df = sorted_df[sorted_df['Expiry Date'] >= np.datetime64(datetime.now().date())]
                first_row = sorted_df.head(1)
                symbol_dict = first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0]
                ltps = get_float_values(statement, "CE")
                if not ltps:
                    ltps = get_float_values(statement, "PE")
                if not ltps:
                    raise CustomError(f"ltps is not found in {statement}")
                __signal_details = {
                    "channel_name": "StudentsGroup",
                    "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                    "ltp_range": "|".join(ltps),
                    "target_range": "0",
                    "sl": "",
                    "quantity": get_multiplier(
                        symbol_dict["Trading Symbol"],
                        StudentsGroup.channel_details, special_case="BTST"
                    ),
                    "action": "BTST-Cancel" if is_reply_msg and is_close_msg else "BTST-Buy",
                }
                if __signal_details in signals:
                    raise CustomError("Signal already exists")
                else:
                    signals.append(__signal_details)
                signal_details = __signal_details.copy()
                signal_details["timestamp"] = (
                    f"{StudentsGroup.channel_number}{self.msg_received_timestamp}"
                )
                write_signals_to_csv(signal_details)
                    

            elif " FUTURE " in self.message:
                if int(StudentsGroup.channel_details.get('INCLUDE_FUT','1')) == 0:
                    raise CustomError(f"Future is turned off as per config")
                
                sym_fut = self.message.split('FUTURE')
                sym = sym_fut[0].split()[-1]
                symbol_dict = get_fut_instrument_name(sym)
                ltps = get_float_values(statement, "FUTURE")
                if not ltps:
                    raise CustomError(f"ltps is not found in {statement}")
                targets = get_float_values(statement, "TARGETS")
                if not targets:
                    raise CustomError(f"targets is not found in {statement}")
                sl = get_float_values(statement, "SL")
                if not sl:
                    raise CustomError(f"SL is not found in {statement}")
                sl = sl[0]
                if 'SELL' in self.message_as_is.split():
                    action = "SHORT"
                elif 'BUY' in self.message_as_is.split():
                    action = "BUY"
                else:
                    action = "BUY"
                if is_reply_msg and is_close_msg:
                    action = "CANCEL"
                
                if not action:
                    raise CustomError(f"could not decide action in {statement}")

                __signal_details = {
                    "channel_name": "StudentsGroup",
                    "symbol": symbol_dict["Exch"]+":"+symbol_dict["Trading Symbol"],
                    "ltp_range": "|".join(ltps),
                    "target_range": "|".join(targets),
                    "sl": sl,
                    "quantity": get_multiplier(symbol_dict["Trading Symbol"], StudentsGroup.channel_details),
                    "action": action,
                }
                if __signal_details in signals:
                    raise CustomError("Signal already exists")
                else:
                    signals.append(__signal_details)
                signal_details = __signal_details.copy()
                signal_details["timestamp"] = f"{StudentsGroup.channel_number}{self.msg_received_timestamp}"
                write_signals_to_csv(signal_details)
            else:
                for word in StudentsGroup.split_words:
                    statement = statement.replace(word, "|")
                parts = statement.split("|")
                symbol_from_tg = parts[0].strip().removeprefix("BUY").strip()
                symbol_dict, sym = self.get_instrument_name(symbol_from_tg)
                ltps = re.findall(r"\d+\.\d+|\d+", parts[1].strip())
                ltp_max = max(
                    [float(ltp) for ltp in ltps if ltp.replace(".", "", 1).isdigit()]
                )
                sl, targets = None, None
                targets = []
                for target_keyword in ["TARGETS", "TARGET", "TRT", "TRG", "TARTE", "TARGE", "TG"]:
                    try:
                        targets = self.get_float_values(self.message, target_keyword, " ")
                    except:
                        pass
                    if targets:
                        break
                # else:
                #     raise CustomError("TARGET not found")    
                if float(targets[0]) < float(ltps[0]):
                    targets = [
                        str(float(target) + ltp_max)
                        for target in targets
                        if target.replace(".", "", 1).isdigit()
                    ]
                for sl_keyword in ["SL", "STOPLOSS"]:
                    sl = self.get_float_values(self.message, sl_keyword, " ")
                    if sl:
                        sl = sl[0]
                        break
                # else:
                #     raise CustomError("SL not found")
                if sym in index_options:
                    key_to_chk = f"INCLUDE_INDEX_OPTION_{sym}"
                    if int(StudentsGroup.channel_details.get(key_to_chk,'1')) == 0:
                        raise CustomError(f"{sym} is turned off as per config")
                    if not sl or not targets:
                        sl, targets = get_sl_target_from_csv(sym, ltp_max)
                else:
                    if int(StudentsGroup.channel_details.get('INCLUDE_STOCK_OPTIONS','1')) == 0:
                        raise CustomError(f"STOCK_OPTIONS is turned off as per config")
                if not sl or not targets:
                    raise CustomError("Target and SL is not availble in msg")
                __signal_details = {
                    "channel_name": "StudentsGroup",
                    "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                    "ltp_range": "|".join(ltps),
                    "target_range": "|".join(targets),
                    "sl": sl,
                    "quantity": get_multiplier(
                        symbol_dict["Trading Symbol"],
                        StudentsGroup.channel_details,
                    ),
                    "action": "Cancel" if is_reply_msg and is_close_msg else "Buy",
                }
                if __signal_details in signals:
                    raise CustomError("Signal already exists")
                else:
                    signals.append(__signal_details)
                signal_details = __signal_details.copy()
                signal_details["timestamp"] = (
                    f"{StudentsGroup.channel_number}{self.msg_received_timestamp}"
                )
                write_signals_to_csv(signal_details)
        except:
            failure_details = {
                "channel_name": "StudentsGroup",
                "timestamp": self.msg_received_timestamp,
                "message": self.message,
                "exception": traceback.format_exc().strip(),
            }
            logger.error(failure_details)
            write_failure_to_csv(failure_details)


class PremiumMembershipGroup:
    split_words = ["ABOVE", "TARGET", "SL"]
    channel_details = CHANNEL_DETAILS["PremiumMembershipGroup"]
    channel_number = channel_details["channel_number"]

    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        self.message = re.sub(r"\.+", ".", telegram_msg.upper())
        self.message = self.message.strip().removeprefix("BUY ").replace(":-", " ").replace("(", " ").replace(")", " ").replace(":", " ").strip()
        for misspelt_word, right_word in spell_checks.items():
            if misspelt_word in self.message:
                self.message = self.message.replace(misspelt_word, right_word)
                
    def get_float_values(self, string_val, start_val, split_values):
        float_values = []
        v = string_val.split(start_val)
        for word in re.split(split_values, v[1]):
            if not word.strip():
                continue
            if word.replace("+", "").replace(".", "", 1).isdigit():
                float_values.append(word)
            else:
                break
        return float_values

    def get_closest_match(self, symbol):
        if symbol in spell_checks:
            symbol = spell_checks[symbol]
        if symbol in all_symbols:
            return symbol
        raise CustomError("Closest match is not found")

    def get_instrument_name(self, symbol_from_tg):
        try:
            sym_split = symbol_from_tg.split()
            if len(sym_split) == 3:
                sym, strike, option_type = sym_split
            elif len(sym_split) > 3:
                sym, strike, option_type, *_ = sym_split
            elif len(sym_split) == 2:
                sym = sym_split[0]
                strike = sym_split[1].strip()[:-2]
                option_type = sym_split[1].strip()[-2:]
            sym = self.get_closest_match(sym)
            exch = "BFO" if sym in ["SENSEX", "BANKEX"] else "NFO"
            filtered_df = scrip_info_df[
                (scrip_info_df["Exch"] == exch)
                & (scrip_info_df["Symbol"] == sym)
                & (scrip_info_df["Strike Price"] == float(strike))
                & (scrip_info_df["Option Type"] == option_type)
            ]
            sorted_df = filtered_df.sort_values(by="Expiry Date")
            sorted_df['Expiry Date'] = pd.to_datetime(filtered_df['Expiry Date'], format='%Y-%m-%d')
            sorted_df = sorted_df[sorted_df['Expiry Date'] >= np.datetime64(datetime.now().date())]
            first_row = sorted_df.head(1)
            return first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0], sym
        except:
            raise CustomError(traceback.format_exc())

    def get_signal(self):
        try:
            statement = self.message
            is_reply_msg = "$$$$" in statement
            new_msg = self.message.upper().split("$$$$")[-1]
            is_close_msg = any([word in new_msg.split() for word in close_words])
            if is_reply_msg and is_close_msg:
                # is a reply message and has close words in it:
                pass
            elif not is_reply_msg:
                # is not a reply message
                pass
            elif is_reply_msg and not is_close_msg:
                # is a reply message but not having close words
                # duplicate or junk
                failure_details = {
                    "channel_name": "PremiumMembershipGroup",
                    "timestamp": self.msg_received_timestamp,
                    "message": self.message,
                    "exception": "is a reply message but not having close words. Possible duplicate or junk",
                }
                logger.error(failure_details)
                write_failure_to_csv(failure_details)
                return

            for word in PremiumMembershipGroup.split_words:
                statement = statement.replace(word, "|")
            parts = statement.split("|")
            symbol_from_tg = parts[0].strip().removeprefix("#")
            symbol_dict, sym = self.get_instrument_name(symbol_from_tg)
            ltps = re.findall(r"\d+\.\d+|\d+", parts[1].strip())
            ltp_max = max(
                [float(ltp) for ltp in ltps if ltp.replace(".", "", 1).isdigit()]
            )
            sl, targets = None, None
            try:
                targets = self.get_float_values(self.message, "TARGET", "/| ")
                sl = self.get_float_values(self.message, "SL", " ")[0]
                # targets = re.findall(r"\d+\.\d+|\d+", parts[3].strip())
                if float(targets[0]) < float(ltps[0]):
                    targets = [
                        str(float(target) + ltp_max)
                        for target in targets
                        if target.replace(".", "", 1).isdigit()
                    ]
            except:
                sl, targets = None, None
            if sym in index_options:
                key_to_chk = f"INCLUDE_INDEX_OPTION_{sym}"
                if int(PremiumMembershipGroup.channel_details.get(key_to_chk,'1')) == 0:
                    raise CustomError(f"{sym} is turned off as per config")
                if not sl or not targets:
                    sl, targets = get_sl_target_from_csv(sym, ltp_max)
            else:
                if int(PremiumMembershipGroup.channel_details.get('INCLUDE_STOCK_OPTIONS','1')) == 0:
                    raise CustomError("STOCK_OPTIONS is turned off as per config")
            if not sl or not targets:
                raise CustomError("sl and target is not available in the msg")
                
            __signal_details = {
                "channel_name": "PremiumMembershipGroup",
                "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                "ltp_range": "|".join(ltps),
                "target_range": "|".join(targets),
                "sl": sl,
                "quantity": get_multiplier(
                    symbol_dict["Trading Symbol"],
                    PremiumMembershipGroup.channel_details,
                ),
                "action": "Cancel" if is_reply_msg and is_close_msg else "Buy",
            }
            if __signal_details in signals:
                raise CustomError("Signal already exists")
            else:
                signals.append(__signal_details)
            signal_details = __signal_details.copy()
            signal_details["timestamp"] = (
                f"{PremiumMembershipGroup.channel_number}{self.msg_received_timestamp}"
            )
            write_signals_to_csv(signal_details)
        except:
            failure_details = {
                "channel_name": "PremiumMembershipGroup",
                "timestamp": self.msg_received_timestamp,
                "message": self.message,
                "exception": traceback.format_exc().strip(),
            }
            logger.error(failure_details)
            write_failure_to_csv(failure_details)


class AllIn1Group:
    split_words = ["ABOVE", "TARGET", "SL"]
    channel_details = CHANNEL_DETAILS["AllIn1Group"]
    channel_number = channel_details["channel_number"]

    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        self.message = re.sub(r"\.+", ".", telegram_msg.upper())
        for misspelt_word, right_word in spell_checks.items():
            if misspelt_word in self.message:
                self.message = self.message.replace(misspelt_word, right_word)
                
    def get_closest_match(self, symbol):
        if symbol in spell_checks:
            symbol = spell_checks[symbol]
        if symbol in all_symbols:
            return symbol
        raise CustomError("Closest match is not found")

    def get_instrument_name(self, symbol_from_tg, next_strike=False):
        try:
            sym_split = symbol_from_tg.split()
            if len(sym_split) == 3:
                sym, strike, option_type = sym_split
            elif len(sym_split) > 3:
                sym, strike, option_type, *_ = sym_split
            elif len(sym_split) == 2:
                sym = sym_split[0]
                strike = sym_split[1].strip()[:-2]
                option_type = sym_split[1].strip()[-2:]
            sym = self.get_closest_match(sym)
            exch = "BFO" if sym in ["SENSEX", "BANKEX"] else "NFO"
            if next_strike:
                filtered_df = scrip_info_df[
                    (scrip_info_df["Exch"] == exch)
                    & (scrip_info_df["Symbol"] == sym)
                    & (scrip_info_df["Strike Price"] == float(get_updated_strike_price(sym, strike, option_type)))
                    & (scrip_info_df["Option Type"] == option_type)
                ]
            else:        
                filtered_df = scrip_info_df[
                    (scrip_info_df["Exch"] == exch)
                    & (scrip_info_df["Symbol"] == sym)
                    & (scrip_info_df["Strike Price"] == float(strike))
                    & (scrip_info_df["Option Type"] == option_type)
                ]
            sorted_df = filtered_df.sort_values(by="Expiry Date")
            sorted_df['Expiry Date'] = pd.to_datetime(filtered_df['Expiry Date'], format='%Y-%m-%d')
            sorted_df = sorted_df[sorted_df['Expiry Date'] >= np.datetime64(datetime.now().date())]
            first_row = sorted_df.head(1)
            return first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0], sym
        except:
            raise CustomError(traceback.format_exc())

    def get_signal(self):
        try:
            statement = self.message
            # if "SL" not in statement:
            #     raise CustomError("SL not found")
            is_reply_msg = "$$$$" in statement
            new_msg = self.message.upper().split("$$$$")[-1]
            is_close_msg = any([word in new_msg.split() for word in close_words])
            if is_reply_msg and is_close_msg:
                # is a reply message and has close words in it:
                pass
            elif not is_reply_msg:
                # is not a reply message
                pass
            elif is_reply_msg and not is_close_msg:
                # is a reply message but not having close words
                # duplicate or junk
                failure_details = {
                    "channel_name": "AllIn1Group",
                    "timestamp": self.msg_received_timestamp,
                    "message": self.message,
                    "exception": "is a reply message but not having close words. Possible duplicate or junk",
                }
                logger.error(failure_details)
                write_failure_to_csv(failure_details)
                return
            if "BTST" in self.message:
                if int(AllIn1Group.channel_details.get('INCLUDE_BTST','1')) == 0:
                    raise CustomError(f"BTST is turned off as per config")
                
                for word in AllIn1Group.split_words:
                    statement = statement.replace(word, "|")
                parts = statement.split("|")
                sym = parts[0].strip().removeprefix("#")
                symbol_dict, sym = self.get_instrument_name(sym, next_strike=True)
                # sym = self.get_closest_match(sym)
                # exch = "BFO" if sym in ["SENSEX", "BANKEX"] else "NFO"
                # filtered_df = scrip_info_df[
                #     (scrip_info_df["Exch"] == exch)
                #     & (scrip_info_df["Symbol"] == sym)
                #     & (scrip_info_df["Strike Price"] == float(strike))
                #     & (scrip_info_df["Option Type"] == option_type)
                #     # & (scrip_info_df["Expiry Date"] == f"2024-{month}-{date}")
                # ]
                # filtered_df = filtered_df.sort_values(by="Expiry Date")
                # filtered_df['Expiry Date'] = pd.to_datetime(filtered_df['Expiry Date'], format='%Y-%m-%d')
                # filtered_df = filtered_df[filtered_df['Expiry Date'] >= np.datetime64(datetime.now().date())]
                # first_row = filtered_df.head(1)
                ltps = re.findall(r"\d+\.\d+|\d+", parts[1].strip())
                
                __signal_details = {
                        "channel_name": "AllIn1Group",
                        "symbol": symbol_dict["Exch"]+":"+symbol_dict["Trading Symbol"],
                        "ltp_range": "|".join(ltps),
                        "target_range": "0",
                        "sl": "",
                        "quantity": get_multiplier(symbol_dict["Trading Symbol"], AllIn1Group.channel_details, special_case="BTST"),
                        "action": "BTST-Cancel"
                                if is_reply_msg and is_close_msg
                                else "BTST-Buy"
                    }
                if __signal_details in signals:
                    raise CustomError("Signal already exists")
                else:
                    signals.append(__signal_details)
                signal_details = __signal_details.copy()
                signal_details["timestamp"] = f"{AllIn1Group.channel_number}{self.msg_received_timestamp}"
                write_signals_to_csv(signal_details)
            else:
                for word in AllIn1Group.split_words:
                    statement = statement.replace(word, "|")
                parts = statement.split("|")
                symbol_from_tg = parts[0].strip().removeprefix("#")
                symbol_dict, sym = self.get_instrument_name(symbol_from_tg)
                ltps = re.findall(r"\d+\.\d+|\d+", parts[1].strip())
                ltp_max = max(
                    [float(ltp) for ltp in ltps if ltp.replace(".", "", 1).isdigit()]
                )
                sl, targets = None, None
                if sym in index_options:
                    key_to_chk = f"INCLUDE_INDEX_OPTION_{sym}"
                    if int(AllIn1Group.channel_details.get(key_to_chk,'1')) == 0:
                        raise CustomError(f"{sym} is turned off as per config")
                    sl, targets = get_sl_target_from_csv(sym, ltp_max)
                else:
                    if int(AllIn1Group.channel_details.get('INCLUDE_STOCK_OPTIONS','1')) == 0:
                        raise CustomError(f"STOCK_OPTIONS is turned off as per config")
                if not sl and not targets:
                    targets = re.findall(r"\d+\.\d+|\d+", parts[3].strip())
                    if float(targets[0]) < float(ltps[0]):
                        targets = [
                            str(float(target) + ltp_max)
                            for target in targets
                            if target.replace(".", "", 1).isdigit()
                        ]
                    sl = re.findall(r"\d+\.\d+|\d+", parts[2].strip())[0]
                __signal_details = {
                    "channel_name": "AllIn1Group",
                    "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                    "ltp_range": "|".join(ltps),
                    "target_range": "|".join(targets),
                    "sl": sl,
                    "quantity": get_multiplier(
                        symbol_dict["Trading Symbol"],
                        AllIn1Group.channel_details,
                    ),
                    "action": "Cancel" if is_reply_msg and is_close_msg else "Buy",
                }
                if __signal_details in signals:
                    raise CustomError("Signal already exists")
                else:
                    signals.append(__signal_details)
                signal_details = __signal_details.copy()
                signal_details["timestamp"] = (
                    f"{AllIn1Group.channel_number}{self.msg_received_timestamp}"
                )
                write_signals_to_csv(signal_details)
        except:
            failure_details = {
                "channel_name": "AllIn1Group",
                "timestamp": self.msg_received_timestamp,
                "message": self.message,
                "exception": traceback.format_exc().strip(),
            }
            logger.error(failure_details)
            write_failure_to_csv(failure_details)


class SChoudhry12:
    split_words = ["ABOVE", "TGT", "SL"]
    channel_details = CHANNEL_DETAILS["SChoudhry12"]
    channel_number = channel_details["channel_number"]

    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        self.message = re.sub(r"\.+", ".", telegram_msg.upper())
        if "BUY " in self.message:
            self.message = self.message.strip().split("BUY ")[1]
        self.message = self.message.strip().removeprefix("BUY ").strip()
        for misspelt_word, right_word in spell_checks.items():
            if misspelt_word in self.message:
                self.message = self.message.replace(misspelt_word, right_word)
                
    def get_closest_match(self, symbol):
        if symbol in spell_checks:
            symbol = spell_checks[symbol]
        if symbol in all_symbols:
            return symbol
        raise CustomError("Closest match is not found")

    def get_instrument_name(self, symbol_from_tg):
        try:
            sym_split = symbol_from_tg.split()
            if len(sym_split) == 3:
                sym, strike, option_type = sym_split
            elif len(sym_split) > 3:
                sym, strike, option_type, *_ = sym_split
            elif len(sym_split) == 2:
                sym = sym_split[0]
                strike = sym_split[1].strip()[:-2]
                option_type = sym_split[1].strip()[-2:]
            sym = self.get_closest_match(sym)
            exch = "BFO" if sym in ["SENSEX", "BANKEX"] else "NFO"
            filtered_df = scrip_info_df[
                (scrip_info_df["Exch"] == exch)
                & (scrip_info_df["Symbol"] == sym)
                & (scrip_info_df["Strike Price"] == float(strike))
                & (scrip_info_df["Option Type"] == option_type)
            ]
            sorted_df = filtered_df.sort_values(by="Expiry Date")
            sorted_df['Expiry Date'] = pd.to_datetime(filtered_df['Expiry Date'], format='%Y-%m-%d')
            sorted_df = sorted_df[sorted_df['Expiry Date'] >= np.datetime64(datetime.now().date())]
            first_row = sorted_df.head(1)
            return first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0], sym
        except:
            raise CustomError(traceback.format_exc())

    def get_signal(self):
        try:
            statement = self.message
            is_reply_msg = "$$$$" in statement
            new_msg = self.message.upper().split("$$$$")[-1]
            is_close_msg = any([word in new_msg.split() for word in close_words])
            if not is_close_msg:
                is_close_msg = any([word for word in close_words if word in new_msg])
            if is_reply_msg and is_close_msg:
                # is a reply message and has close words in it:
                pass
            elif not is_reply_msg:
                # is not a reply message
                pass
            elif is_reply_msg and not is_close_msg:
                # is a reply message but not having close words
                # duplicate or junk
                failure_details = {
                    "channel_name": "SChoudhry12",
                    "timestamp": self.msg_received_timestamp,
                    "message": self.message,
                    "exception": "is a reply message but not having close words. Possible duplicate or junk",
                }
                logger.error(failure_details)
                write_failure_to_csv(failure_details)
                return

            for word in SChoudhry12.split_words:
                statement = statement.replace(word, "|")
            parts = statement.split("|")
            symbol_from_tg = parts[0].strip().removeprefix("#")
            symbol_dict, sym = self.get_instrument_name(symbol_from_tg)
            ltps = re.findall(r"\d+\.\d+|\d+", parts[1].strip())
            ltp_max = max(
                [float(ltp) for ltp in ltps if ltp.replace(".", "", 1).isdigit()]
            )
            sl, targets = None, None
            if sym in index_options:
                key_to_chk = f"INCLUDE_INDEX_OPTION_{sym}"
                if int(SChoudhry12.channel_details.get(key_to_chk,'1')) == 0:
                    raise CustomError(f"{sym} is turned off as per config")
                sl, targets = get_sl_target_from_csv(sym, ltp_max)
            else:
                if int(SChoudhry12.channel_details.get('INCLUDE_STOCK_OPTIONS','1')) == 0:
                    raise CustomError(f"STOCK_OPTIONS is turned off as per config")
            if not sl and not targets:
                targets = re.findall(r"\d+\.\d+|\d+", parts[2].strip())
                if float(targets[0]) < float(ltps[0]):
                    targets = [
                        str(float(target) + ltp_max)
                        for target in targets
                        if target.replace(".", "", 1).isdigit()
                    ]
                sl = re.findall(r"\d+\.\d+|\d+", parts[3].strip())[0]
            __signal_details = {
                "channel_name": "SChoudhry12",
                "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                "ltp_range": "|".join(ltps),
                "target_range": "|".join(targets),
                "sl": sl,
                "quantity": get_multiplier(
                    symbol_dict["Trading Symbol"],
                    SChoudhry12.channel_details,
                ),
                "action": "Cancel" if is_reply_msg and is_close_msg else "Buy",
            }
            if __signal_details in signals:
                raise CustomError("Signal already exists")
            else:
                signals.append(__signal_details)
            signal_details = __signal_details.copy()
            signal_details["timestamp"] = (
                f"{SChoudhry12.channel_number}{self.msg_received_timestamp}"
            )
            write_signals_to_csv(signal_details)
        except:
            failure_details = {
                "channel_name": "SChoudhry12",
                "timestamp": self.msg_received_timestamp,
                "message": self.message,
                "exception": traceback.format_exc().strip(),
            }
            logger.error(failure_details)
            write_failure_to_csv(failure_details)


class VipPremiumPaidCalls:
    split_words = ["ABOVE", "BUY PRICE", "TGT", "TARGET", "TG", "SL"]
    channel_details = CHANNEL_DETAILS["VipPremiumPaidCalls"]
    channel_number = channel_details["channel_number"]

    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        self.message = re.sub(r"-+", "", telegram_msg.upper())
        self.message = self.message.removeprefix("BUY").strip()
        self.message = self.message.removeprefix("#").strip()
        for misspelt_word, right_word in spell_checks.items():
            if misspelt_word in self.message:
                self.message = self.message.replace(misspelt_word, right_word)
                
    def get_closest_match(self, symbol):
        if symbol in spell_checks:
            symbol = spell_checks[symbol]
        if symbol in all_symbols:
            return symbol
        raise CustomError("Closest match is not found")

    def get_instrument_name(self, symbol_from_tg):
        try:
            sym_split_org = [s.strip() for s in symbol_from_tg.split() if s.strip()]
            sym_split = []
            sym_char = ''
            original_sym_split = sym_split_org.pop(0)
            for char in original_sym_split:
                if char.isdigit():
                    break
                sym_char += char
            sym_split = original_sym_split.replace(sym_char, f"{sym_char} ").split()
            sym_split.extend(sym_split_org)

            if len(sym_split) == 3:
                sym, strike, option_type = sym_split
            # elif len(sym_split) > 3:
            #     sym, strike, option_type, *_ = sym_split
            elif len(sym_split) == 2:
                sym = sym_split[0]
                strike = sym_split[1].strip()[:-2]
                option_type = sym_split[1].strip()[-2:]
            elif sym_split[-1].upper().strip() in ('CE', 'PE'):
                sym = sym_split[0]
                if len(sym_split[-1].upper().strip()) == 2:
                    strike = sym_split[-2].strip()
                    option_type = sym_split[-1].strip()
                else:
                    strike = sym_split[-1].strip()[:-2]
                    option_type = sym_split[-1].strip()[-2:]
            sym = self.get_closest_match(sym)
            exch = "BFO" if sym in ["SENSEX", "BANKEX"] else "NFO"
            filtered_df = scrip_info_df[
                (scrip_info_df["Exch"] == exch)
                & (scrip_info_df["Symbol"] == sym)
                & (scrip_info_df["Strike Price"] == float(strike))
                & (scrip_info_df["Option Type"] == option_type)
            ]
            sorted_df = filtered_df.sort_values(by="Expiry Date")
            sorted_df['Expiry Date'] = pd.to_datetime(filtered_df['Expiry Date'], format='%Y-%m-%d')
            sorted_df = sorted_df[sorted_df['Expiry Date'] >= np.datetime64(datetime.now().date())]
            first_row = sorted_df.head(1)
            return first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0], sym
        except:
            raise CustomError(traceback.format_exc())

    def get_signal(self):
        try:
            statement = self.message
            is_reply_msg = "$$$$" in statement
            new_msg = self.message.upper().split("$$$$")[-1]
            is_close_msg = any([word in new_msg.split() for word in close_words])
            if is_reply_msg and is_close_msg:
                # is a reply message and has close words in it:
                pass
            elif not is_reply_msg:
                # is not a reply message
                pass
            elif is_reply_msg and not is_close_msg:
                # is a reply message but not having close words
                # duplicate or junk
                failure_details = {
                    "channel_name": "VipPremiumPaidCalls",
                    "timestamp": self.msg_received_timestamp,
                    "message": self.message,
                    "exception": "is a reply message but not having close words. Possible duplicate or junk",
                }
                logger.error(failure_details)
                write_failure_to_csv(failure_details)
                return

            for word in VipPremiumPaidCalls.split_words:
                statement = statement.replace(word, "|")
            parts = statement.split("|")
            symbol_from_tg = parts[0].strip().removeprefix("#")
            # sym, *_ = symbol_from_tg.upper().split()
            symbol_dict, sym = self.get_instrument_name(symbol_from_tg)
            ltps = re.findall(r"\d+\.\d+|\d+", parts[1].strip())
            ltp_max = max(
                [float(ltp) for ltp in ltps if ltp.replace(".", "", 1).isdigit()]
            )
            sl, targets = None, None
            if sym in index_options:
                key_to_chk = f"INCLUDE_INDEX_OPTION_{sym}"
                if int(VipPremiumPaidCalls.channel_details.get(key_to_chk,'1')) == 0:
                    raise CustomError(f"{sym} is turned off as per config")
                sl, targets = get_sl_target_from_csv(sym, ltp_max)
            else:
                if int(VipPremiumPaidCalls.channel_details.get('INCLUDE_STOCK_OPTIONS','1')) == 0:
                    raise CustomError(f"STOCK_OPTIONS is turned off as per config")
            if not sl and not targets:
                targets = re.findall(r"\d+\.\d+|\d+", parts[3].strip())
                if float(targets[0]) < float(ltps[0]):
                    targets = [
                        str(float(target) + ltp_max)
                        for target in targets
                        if target.replace(".", "", 1).isdigit()
                    ]
            __signal_details = {
                "channel_name": "VipPremiumPaidCalls",
                "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                "ltp_range": "|".join(ltps),
                "target_range": "|".join(targets),
                "sl": re.findall(r"\d+\.\d+|\d+", parts[2].strip())[0],
                "quantity": get_multiplier(
                    symbol_dict["Trading Symbol"],
                    VipPremiumPaidCalls.channel_details
                ),
                "action": "Cancel" if is_reply_msg and is_close_msg else "Buy",
            }
            if __signal_details in signals:
                raise CustomError("Signal already exists")
            else:
                signals.append(__signal_details)
            signal_details = __signal_details.copy()
            signal_details["timestamp"] = (
                f"{VipPremiumPaidCalls.channel_number}{self.msg_received_timestamp}"
            )
            write_signals_to_csv(signal_details)
        except:
            failure_details = {
                "channel_name": "VipPremiumPaidCalls",
                "timestamp": self.msg_received_timestamp,
                "message": self.message,
                "exception": traceback.format_exc().strip(),
            }
            logger.error(failure_details)
            write_failure_to_csv(failure_details)


class PlatinumMembers:
    split_words = ["ABOVE", "TGT", "TARGET", "SL"]
    channel_details = CHANNEL_DETAILS["PlatinumMembers"]
    channel_number = channel_details["channel_number"]

    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        self.message = re.sub(r"=", "", telegram_msg.upper())
        if self.message.strip().startswith("BUY"):
            self.message = self.message.strip().removeprefix("BUY").strip()
        elif self.message.split().count("BUY") >= 2 and "BUY" in self.message:
            self.message = self.message.split("BUY", 1)[1].strip()
        print(self.message)
        for misspelt_word, right_word in spell_checks.items():
            if misspelt_word in self.message:
                self.message = self.message.replace(misspelt_word, right_word)
                
    def get_closest_match(self, symbol):
        if symbol in spell_checks:
            symbol = spell_checks[symbol]
        if symbol in all_symbols:
            return symbol
        raise CustomError("Closest match is not found")

    def get_instrument_name(self, symbol_from_tg):
        try:
            sym_split = [s.strip() for s in symbol_from_tg.split() if s.strip()]
            print(sym_split)
            if len(sym_split) == 3:
                sym, strike, option_type = sym_split
            elif len(sym_split) > 3:
                sym = sym_split[0]
                strike = None
                option_type = None
                for i in sym_split[1:]:
                    if str(i).isdigit():
                        strike = i.strip()
                for i in sym_split[1:]:
                    if i.upper() in ('PE', 'CE'):
                        option_type = i.upper()
                if not (strike or option_type):
                    raise CustomError(f'Strike or option type is not found in {symbol_from_tg}')
            elif len(sym_split) == 2:
                sym = sym_split[0]
                strike = sym_split[1].strip()[:-2]
                option_type = sym_split[1].strip()[-2:]
            sym = self.get_closest_match(sym)
            exch = "BFO" if sym in ["SENSEX", "BANKEX"] else "NFO"
            filtered_df = scrip_info_df[
                (scrip_info_df["Exch"] == exch)
                & (scrip_info_df["Symbol"] == sym)
                & (scrip_info_df["Strike Price"] == float(strike))
                & (scrip_info_df["Option Type"] == option_type)
            ]
            sorted_df = filtered_df.sort_values(by="Expiry Date")
            sorted_df['Expiry Date'] = pd.to_datetime(filtered_df['Expiry Date'], format='%Y-%m-%d')
            sorted_df = sorted_df[sorted_df['Expiry Date'] >= np.datetime64(datetime.now().date())]
            first_row = sorted_df.head(1)
            return first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0], sym
        except:
            raise CustomError(traceback.format_exc())

    def get_signal(self):
        try:
            statement = self.message
            is_reply_msg = "$$$$" in statement
            new_msg = self.message.upper().split("$$$$")[-1]
            is_close_msg = any([word in new_msg.split() for word in close_words])
            if is_reply_msg and is_close_msg:
                # is a reply message and has close words in it:
                pass
            elif not is_reply_msg:
                # is not a reply message
                pass
            elif is_reply_msg and not is_close_msg:
                # is a reply message but not having close words
                # duplicate or junk
                failure_details = {
                    "channel_name": "PlatinumMembers",
                    "timestamp": self.msg_received_timestamp,
                    "message": self.message,
                    "exception": "is a reply message but not having close words. Possible duplicate or junk",
                }
                logger.error(failure_details)
                write_failure_to_csv(failure_details)
                return

            for word in PlatinumMembers.split_words:
                statement = statement.replace(word, "|")
            parts = statement.split("|")
            symbol_from_tg = parts[0].strip().removeprefix("#")
            # sym, *_ = symbol_from_tg.upper().split()
            symbol_dict, sym = self.get_instrument_name(symbol_from_tg)
            ltps = re.findall(r"\d+\.\d+|\d+", parts[1].strip())
            ltp_max = max(
                [float(ltp) for ltp in ltps if ltp.replace(".", "", 1).isdigit()]
            )
            sl, targets = None, None
            try:
                targets = re.findall(r"\d+\.\d+|\d+", parts[3].strip())
                if float(targets[0]) < float(ltps[0]):
                    targets = [
                        str(float(target) + ltp_max)
                        for target in targets
                        if target.replace(".", "", 1).isdigit()
                    ]
                sl = re.findall(r"\d+\.\d+|\d+", parts[2].strip())[0]
            except:
                sl, targets = None, None
            print(sym)
            print(index_options)
            print(sl, targets)
            if sym in index_options :
                if (not sl and not targets):
                    key_to_chk = f"INCLUDE_INDEX_OPTION_{sym}"
                    if int(PlatinumMembers.channel_details.get(key_to_chk,'1')) == 0:
                        raise CustomError(f"{sym} is turned off as per config")
                    if not sl or not targets:
                        sl, targets = get_sl_target_from_csv(sym, ltp_max)
            else:
                print(PlatinumMembers.channel_details)
                if int(PlatinumMembers.channel_details.get('INCLUDE_STOCK_OPTIONS','1')) == 0:
                    raise CustomError("STOCK_OPTIONS is turned off as per config")
            if not sl and not targets:
                raise CustomError("Target and SL is not available in the msg")
            __signal_details = {
                "channel_name": "PlatinumMembers",
                "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                "ltp_range": "|".join(ltps),
                "target_range": "|".join(targets),
                "sl": sl,
                "quantity": get_multiplier(
                    symbol_dict["Trading Symbol"],
                    PlatinumMembers.channel_details
                ),
                "action": "Cancel" if is_reply_msg and is_close_msg else "Buy",
            }
            if __signal_details in signals:
                raise CustomError("Signal already exists")
            else:
                signals.append(__signal_details)
            signal_details = __signal_details.copy()
            signal_details["timestamp"] = (
                f"{PlatinumMembers.channel_number}{self.msg_received_timestamp}"
            )
            write_signals_to_csv(signal_details)
        except:
            failure_details = {
                "channel_name": "PlatinumMembers",
                "timestamp": self.msg_received_timestamp,
                "message": self.message,
                "exception": traceback.format_exc().strip(),
            }
            logger.error(failure_details)
            write_failure_to_csv(failure_details)



class BankNiftyRani:
    channel_details = CHANNEL_DETAILS["BankNiftyRani"]
    channel_number = channel_details["channel_number"]

    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        self.message = re.sub(r"=", "", telegram_msg.upper()).strip()
        self.message = self.message.replace("*", "").strip()
        self.message = self.message.replace("🦁", "").strip()
        print(self.message)
        for misspelt_word, right_word in spell_checks.items():
            if misspelt_word in self.message:
                self.message = self.message.replace(misspelt_word, right_word)

    def get_closest_match(self, symbol):
        if symbol in spell_checks:
            symbol = spell_checks[symbol]
        if symbol in all_symbols:
            return symbol
        return symbol
    
    def adjust_list_by_hundreds(self, data_list):
        if not data_list:
            return [] 

        max_value = max(data_list)
        max_hundreds = max_value // 100 

        adjusted_list = []
        for num in data_list:
            if abs(num - max_value) > 100:
                target_hundreds = max_hundreds if num <= max_value else max_hundreds
                adjusted_num = target_hundreds * 100 + (num % 100)
                if adjusted_num == target_hundreds * 100:
                    adjusted_num = adjusted_num + 100
                adjusted_list.append(adjusted_num)
            else:
                adjusted_list.append(num)
        return sorted(adjusted_list)
    def get_instrument_name(self, symbol_from_tg):
        try:
            sym_strike_opt_list = symbol_from_tg.split(" ")
            option_type = sym_strike_opt_list[-1]
            strike = sym_strike_opt_list[-2]
            sym = "".join(sym_strike_opt_list[:-2])
            # print(f"{sym=}")
            sym = self.get_closest_match(sym)
            exch = "BFO" if sym in ["SENSEX", "BANKEX"] else "NFO"
            logger.info(f"Exch: {exch}, Symbol: {sym}, Strike: {strike}, Option Type: {option_type}")
            filtered_df = scrip_info_df[
                (scrip_info_df["Exch"] == exch)
                & (scrip_info_df["Symbol"] == sym)
                & (scrip_info_df["Strike Price"] == float(strike))
                & (scrip_info_df["Option Type"] == option_type)
            ]
            # print(f"{filtered_df=}")
            sorted_df = filtered_df.sort_values(by="Expiry Date")
            sorted_df['Expiry Date'] = pd.to_datetime(filtered_df['Expiry Date'], format='%Y-%m-%d')
            sorted_df = sorted_df[sorted_df['Expiry Date'] >= np.datetime64(datetime.now().date())]
            first_row = sorted_df.head(1)
            return first_row[["Exch", "Trading Symbol", "Symbol"]].to_dict(orient="records")[0], sym
        except:
            raise CustomError(traceback.format_exc())

    def get_signal(self):
        try:
            statement = self.message
            is_reply_msg = "$$$$" in statement
            is_valid_msg =  "BUY" in statement
            if not is_valid_msg:
                failure_details = {
                    "channel_name": "BankNiftyRani",
                    "timestamp": self.msg_received_timestamp,
                    "message": self.message,
                    "exception": "is not a valid message. Does not have ANGRY LION in it",
                }
                logger.error(failure_details)
                write_failure_to_csv(failure_details)
                return
            if is_reply_msg:
                # is a reply message 
                # duplicate or junk
                failure_details = {
                    "channel_name": "BankNiftyRani",
                    "timestamp": self.msg_received_timestamp,
                    "message": self.message,
                    "exception": "is a reply message. Possible duplicate or junk",
                }
                logger.error(failure_details)
                write_failure_to_csv(failure_details)
                return
            # self.message = self.message.repace("BUY ", "").strip()
            self.message = self.message.replace("STRATEGY ", "")
            if "ANGRY LION " in self.message:
                self.message = self.message.split("ANGRY LION ")[1].strip()
            sym_strike_opt = self.message.split("BUY ")[0].strip()
            
            
            symbol_dict, sym = self.get_instrument_name(sym_strike_opt)
            ltps = get_float_values(statement.lower(), start_val='above')
            ltps = [float(ltp) for ltp in ltps]
            # ltps.sort()
            ltps = self.adjust_list_by_hundreds(ltps)
            if not ltps:
                failure_details = {
                    "channel_name": "BankNiftyRani",
                    "timestamp": self.msg_received_timestamp,
                    "message": self.message,
                    "exception": "LTPs not found",
                }
                logger.error(failure_details)
                write_failure_to_csv(failure_details)

            targets = get_float_values(statement.lower(), start_val='targets')
            if not targets:
                targets = get_float_values(statement.lower(), start_val='target')

            if not targets:
                target_value = 0.0
                if symbol_dict['Symbol'] == 'NIFTY':
                    target_value = BankNiftyRani.channel_details['NIFTY_TARGET_VALUE']
                elif symbol_dict['Symbol'] == 'BANKNIFTY':
                    target_value = BankNiftyRani.channel_details['BANKNIFTY_TARGET_VALUE']
                elif symbol_dict['Symbol'] == 'FINNIFTY':
                    target_value = BankNiftyRani.channel_details['FINNIFTY_TARGET_VALUE']
                elif symbol_dict['Symbol'] == 'MIDCPNIFTY':
                    target_value = BankNiftyRani.channel_details['MIDCPNIFTY_TARGET_VALUE']
                elif symbol_dict['Symbol'] == 'SENSEX':
                    target_value = BankNiftyRani.channel_details['SENSEX_TARGET_VALUE']
                elif symbol_dict['Symbol'] == 'BANKEX':
                    target_value = BankNiftyRani.channel_details['BANKEX_TARGET_VALUE']
                else:
                    failure_details = {
                        "channel_name": "BankNiftyRani",
                        "timestamp": self.msg_received_timestamp,
                        "message": self.message,
                        "exception": f"target value not found for symbol - {symbol_dict['Symbol']}",
                    }
                    logger.error(failure_details)
                    write_failure_to_csv(failure_details)
                    return
                target_val = str(target_value).split(",")
                targets = [max(ltps) + float(val) for val in target_val]
            
            sl = get_float_values(statement.lower(), start_val='sl')
            if not sl:
                sl_value = 0.0
                if symbol_dict['Symbol'] == 'NIFTY':
                    sl_value = BankNiftyRani.channel_details['NIFTY_SL_VALUE']
                elif symbol_dict['Symbol'] == 'BANKNIFTY':
                    sl_value = BankNiftyRani.channel_details['BANKNIFTY_SL_VALUE']
                elif symbol_dict['Symbol'] == 'FINNIFTY':
                    sl_value = BankNiftyRani.channel_details['FINNIFTY_SL_VALUE']
                elif symbol_dict['Symbol'] == 'MIDCPNIFTY':
                    sl_value = BankNiftyRani.channel_details['MIDCPNIFTY_SL_VALUE']
                elif symbol_dict['Symbol'] == 'SENSEX':
                    sl_value = BankNiftyRani.channel_details['SENSEX_SL_VALUE']
                elif symbol_dict['Symbol'] == 'BANKEX':
                    sl_value = BankNiftyRani.channel_details['BANKEX_SL_VALUE']
                else:
                    failure_details = {
                        "channel_name": "BankNiftyRani",
                        "timestamp": self.msg_received_timestamp,
                        "message": self.message,
                        "exception": f"sl value not found for symbol - {symbol_dict['Symbol']}",
                    }
                    logger.error(failure_details)
                    write_failure_to_csv(failure_details)
                    return
                sl_val = str(sl_value).split(",")
                sl = [min(ltps) - float(val) for val in sl_val]
                
                # targets = [max(ltps) + float(val) for val in target_val]
            
            __signal_details = {
                "channel_name": "BankNiftyRani",
                "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                "ltp_range": "|".join([str(int(ltp)) if ltp==int(ltp) else str(ltp) for ltp in ltps]),
                "target_range": "|".join([str(int(target)) if target==int(target) else str(target) for target in targets]),
                "sl": "|".join([str(int(sl_)) if sl_==int(sl_) else str(sl_) for sl_ in sl]),
                "quantity": get_multiplier(
                    symbol_dict["Trading Symbol"],
                    BankNiftyRani.channel_details
                ),
                "action": "Buy",
            }
            if __signal_details in signals:
                raise CustomError("Signal already exists")
            else:
                signals.append(__signal_details)
            signal_details = __signal_details.copy()
            signal_details["timestamp"] = (
                f"{BankNiftyRani.channel_number}{self.msg_received_timestamp}"
            )
            write_signals_to_csv(signal_details)
        except:
            failure_details = {
                "channel_name": "BankNiftyRani",
                "timestamp": self.msg_received_timestamp,
                "message": self.message,
                "exception": traceback.format_exc().strip(),
            }
            logger.error(failure_details)
            write_failure_to_csv(failure_details)
