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
from constants import BRKR, FUTL, CHANNEL_DETAILS, DATA
from logzero import logger
import random

zero_sl = "0.50"
signals_csv_filename = DATA + "signals_v2.csv"
if os.path.isfile(signals_csv_filename):
    shutil.move(signals_csv_filename, signals_csv_filename.removesuffix(
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
failure_csv_filename = DATA + "failures_v2.csv"
failure_csv_file_headers = ["channel_name", "timestamp",
                            "message", "exception", "normal_timestamp",]
signals = []
spell_checks = {
    "F1NIFTY": "FINNIFTY",
    "N1FTY": "NIFTY",
    "MIDCAPNIFTY": "MIDCPNIFTY",
}
close_words = ("CANCEL", "EXIT", "BREAK", "AVOID", "LOSS", "IGNORE", "CLOSE", "SAFE")


class CustomError(Exception):
    pass


def download_masters(broker):
    exchanges = ["NFO", "BFO"]
    for exchange in exchanges:
        if FUTL.is_file_not_2day(f"./{exchange}.csv"):
            broker.get_contract_master(exchange)


def get_multiplier(symbol, channel_config, num_of_targets=1):
    nfo_df = pd.read_csv("NFO.csv")
    bfo_df = pd.read_csv("BFO.csv")
    df = pd.concat([nfo_df, bfo_df])
    lot_size = df.loc[df['Trading Symbol'] == symbol, 'Lot Size'].iloc[0]
    if "BANKNIFTY" in symbol:
        return "|".join([str(15 * channel_config.get("BANKNIFTY", 1))] * num_of_targets) if num_of_targets > 1 else str(15*channel_config.get("BANKNIFTY", 1))
    elif "FINNIFTY" in symbol:
        return "|".join([str(40 * channel_config.get("FINNIFTY", 1))]* num_of_targets) if num_of_targets > 1 else str(40*channel_config.get("FINNIFTY", 1))
    elif "MIDCPNIFTY" in symbol:
        return "|".join([str(75 * channel_config.get("MIDCPNIFTY", 1))]* num_of_targets) if num_of_targets > 1 else str(75*channel_config.get("MIDCPNIFTY", 1))
    elif "NIFTY" in symbol:
        return (
            "|".join([str(25 * channel_config.get("NIFTY", 1))] * num_of_targets)
            if num_of_targets > 1
            else str(25 * channel_config.get("NIFTY", 1))
        )
    elif "SENSEX" in symbol:
        return "|".join([str(10 * channel_config.get("SENSEX", 1))]* num_of_targets) if num_of_targets > 1 else str(10*channel_config.get("SENSEX", 1))
    elif "BANKEX" in symbol:
        return "|".join([str(15 * channel_config.get("BANKEX", 1))]* num_of_targets) if num_of_targets > 1 else str(15*channel_config.get("BANKEX", 1))
    return lot_size


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
            int(_signal_details["timestamp"][1:])).strftime('%Y-%m-%d %H:%M:%S')
        _signal_details["timestamp"] = _signal_details["timestamp"] + ''.join(random.choices('0123456789', k=5))
        __sl = _signal_details["sl"]
        if (isinstance(__sl, int) or isinstance(__sl, float)) and __sl == 0:
            _signal_details["sl"] = zero_sl
        elif isinstance(__sl, str) and __sl.isdigit() and  __sl in ("0", "00"):
            _signal_details["sl"] = zero_sl
        elif isinstance(__sl, str) and not __sl.strip():
            _signal_details["sl"] = zero_sl
        writer.writerow(
            {k: str(_signal_details.get(k, ""))
             for k in signals_csv_file_headers}
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


api = get_broker(BRKR)
download_masters(api.broker)
scrip_info_df = get_all_contract_details()
all_symbols = set(scrip_info_df["Symbol"].to_list())


class PremiumJackpot:
    split_words = ["BUY", "ABOVE", "NEAR", "TARGET", "TARGE"]
    channel_details = CHANNEL_DETAILS["PremiumJackpot"]
    channel_number = channel_details["channel_number"]

    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        self.message = telegram_msg

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
            sym = self.get_closest_match(sym)
            exch = "BFO" if sym in ["SENSEX", "BANKEX"] else "NFO"
            filtered_df = scrip_info_df[
                (scrip_info_df["Exch"] == exch)
                & (scrip_info_df["Symbol"] == sym)
                & (scrip_info_df["Strike Price"] == float(strike))
                & (scrip_info_df["Option Type"] == option_type)
            ]
            sorted_df = filtered_df.sort_values(by="Expiry Date")
            first_row = sorted_df.head(1)
            return first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0]
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
            elif is_reply_msg and not is_close_msg:
                # is a reply message but not having close words
                # duplicate or junk
                failure_details = {
                    "channel_name": "Premium jackpot",
                    "timestamp": self.msg_received_timestamp,
                    "message": self.message,
                    "exception": "is a reply message but not having close words. Possible duplicate or junk",
                }
                write_failure_to_csv(failure_details)
                return

            for word in PremiumJackpot.split_words:
                statement = statement.replace(word, "|")
            parts = statement.split("|")
            symbol_from_tg = parts[1].strip().removeprefix("#")
            sym, *_ = symbol_from_tg.upper().split()
            symbol_dict = self.get_instrument_name(symbol_from_tg)
            ltps = re.findall(r"\d+\.\d+|\d+", parts[2])
            targets = re.findall(r"\d+\.\d+|\d+", parts[3].split("SL")[0])
            ltp_max = max([float(ltp) for ltp in ltps
                          if ltp.replace('.', '', 1).isdigit()])
            if float(targets[0]) < float(ltps[0]):
                targets = [str(float(target) + ltp_max)
                           for target in targets if target.replace('.', '', 1).isdigit()]
            __signal_details = {
                "channel_name": "Premium jackpot",
                "symbol": symbol_dict["Exch"]+":"+symbol_dict["Trading Symbol"],
                "ltp_range": "|".join(ltps),
                "target_range": "|".join(targets),
                "sl": re.findall(r"SL-(\d+)?", parts[3])[0],
                "quantity": get_multiplier(symbol_dict["Trading Symbol"], PremiumJackpot.channel_details, len(targets)),
                "action": "Cancel"
                if is_close_msg
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
    split_words = ["BUY", "ONLY IN RANGE @", "TARGET", "SL FOR TRADE @ "]
    channel_details = CHANNEL_DETAILS["SmsOptionsPremium"]
    spot_sl = channel_details["spot_sl"]
    channel_number = channel_details["channel_number"]

    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        self.message = telegram_msg

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
            sym, *_, strike, option_type = symbol_from_tg.split()
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
            first_row = filtered_df.head(1)
            return first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0]
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
                symbol_dict = self.get_instrument_name(symbol_d)
                ltp_range = self.get_float_values(statement, "ABOVE ")
                sl = math.floor(
                    float(ltp_range[0]) * (1 - SmsOptionsPremium.spot_sl))
                ltps = self.get_float_values(statement, "ABOVE ")
                targets = self.get_float_values(statement, "TARGETS @ ")
                ltp_max = max(
                    [float(ltp) for ltp in ltps if ltp.replace('.', '', 1).isdigit()])
                if float(targets[0]) < float(ltps[0]):
                    targets = [str(float(target) + ltp_max)
                               for target in targets if target.replace('.', '', 1).isdigit()]
                _signal_details = {
                    "channel_name": "SmsOptionsPremium",
                    "symbol": symbol_dict["Exch"]+":"+symbol_dict["Trading Symbol"],
                    "ltp_range": "|".join(ltps),
                    "target_range": "|".join(targets),
                    "sl": sl if sl > 0 else 0.05,
                    "quantity": get_multiplier(symbol_dict["Trading Symbol"], SmsOptionsPremium.channel_details, len(targets)),
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
        elif is_reply_msg and (is_close_msg or is_sl_message):
            # is a reply message and has close words in it:
            pass
        elif not is_reply_msg:
            # is not a reply message
            pass
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
            if is_close_msg:
                sl = zero_sl
            else:
                sl = re.findall(r"(\d+)?", parts[4])[0]
                if not sl:
                    sl = re.findall(r"(\d+)?", statement.split("$$$$")[-1])[0]
                    if not sl:
                        raise CustomError(f"SL is not found in {parts[4]}")
            symbol_dict = self.get_instrument_name(parts[1].upper().strip())
            ltps = re.findall(r"\d+\.\d+|\d+", parts[2])
            targets = self.get_float_values(
                self.message.strip().upper(), "TARGET")
            ltp_max = max([float(ltp) for ltp in ltps
                          if ltp.replace('.', '', 1).isdigit()])
            if float(targets[0]) < float(ltps[0]):
                targets = [str(float(target) + ltp_max)
                           for target in targets if target.replace('.', '', 1).isdigit()]
            _signal_details = {
                "channel_name": "SmsOptionsPremium",
                "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                "ltp_range": "|".join(ltps),
                "target_range": "|".join(targets),
                "sl": sl,
                "quantity": get_multiplier(symbol_dict["Trading Symbol"], SmsOptionsPremium.channel_details, len(targets)),
                "action": "Cancel" if is_close_msg else "Buy",
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
            write_failure_to_csv(failure_details)


class PaidCallPut:
    channel_details = CHANNEL_DETAILS["PaidCallPut"]
    channel_number = channel_details["channel_number"]

    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        self.message = telegram_msg

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
                write_failure_to_csv(failure_details)
                return

            symbol = self.get_symbol_from_message(self.message)
            print(symbol)
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
                    strike = req_content[i + 1].strip()
                    option = req_content[i + 2].strip()
            sl_list = self.get_float_values(
                self.message.strip().upper().replace("-", ""), "SL")
            if sl_list:
                sl = sl_list[0]
            else:
                raise CustomError("SL is not available")
                # if word.upper().strip().startswith("SL-"):
                #     sl = re.findall(r"SL-(\d+)?", word.upper().strip())[0]
            if strike == None or option == None:
                raise CustomError("Strike or Option is None")
            targets = self.get_target_values(
                self.message.replace("TARGTE", "TARGET"), "TARGET")
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
            targets = targets
            ltp_max = max([float(ltp) for ltp in ltps
                          if ltp.replace('.', '', 1).isdigit()])
            if float(targets[0]) < float(ltps[0]):
                targets = [str(float(target) + ltp_max)
                           for target in targets if target.replace('.', '', 1).isdigit()]
            _signal_details = {
                "channel_name": "PaidCallPut",
                "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                "ltp_range": "|".join(ltps),
                "target_range": "|".join(targets),
                "sl": sl,
                "quantity": get_multiplier(symbol_dict["Trading Symbol"], PaidCallPut.channel_details, len(targets)),
                "action": "Cancel" if is_close_msg else "Buy",
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
                write_failure_to_csv(failure_details)
                return
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

            ltp_words = ["RANGE", "ABOVE", "NEAR LEVEL"]
            target_words = ["TARGET", "TRG"]
            sl_words = ["SL", "STOPLOSS"]

            ltp_range = None
            for word in ltp_words:
                ltp_range = self.get_target_values(self.message_upper, word)
                if ltp_range:
                    break
            else:
                raise CustomError("ltp_range values is not found")

            target_range = None
            for word in target_words:
                target_range = self.get_target_values(self.message_upper, word)
                if target_range:
                    break
            else:
                raise CustomError("target_range values is not found")

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
            # else:
            #     if sym in ('SENSEX', 'BANKEX', 'NIFTY', 'BANKNIFTY', 'MIDCPNIFTY', 'FINNIFTY'):
            #         sl_range = [str(float(sl)/2) for sl in sl_range]
            if not sl_range:
                raise CustomError("sl_range values is not found")
            ltps = ltp_range
            targets = target_range
            ltp_max = max([float(ltp) for ltp in ltps
                          if ltp.replace('.', '', 1).isdigit()])
            if float(targets[0]) < float(ltps[0]):
                targets = [str(float(target) + ltp_max)
                           for target in targets if target.replace('.', '', 1).isdigit()]
            _signal_details = {
                "channel_name": "PaidStockIndexOption",
                "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                "ltp_range": "|".join(ltps),
                "target_range": "|".join(targets),
                "sl": sl_range[0],
                "quantity": get_multiplier(symbol_dict["Trading Symbol"], PaidStockIndexOption.channel_details, len(targets)),
                "action": "Cancel" if is_close_msg else "Buy",
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
            first_row = sorted_df.head(1)
            return first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0]
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
                write_failure_to_csv(failure_details)
                return

            for word in BnoPremium.split_words:
                statement = statement.replace(word, "|")
            parts = statement.split("|")
            print(parts)
            symbol_from_tg = parts[1].strip().removeprefix("#")
            sym, *_ = symbol_from_tg.upper().split()
            symbol_dict = self.get_instrument_name(symbol_from_tg)
            ltps = re.findall(r"\d+\.\d+|\d+", " ".join(parts[1].strip().split()[3:]))
            targets = re.findall(r"\d+\.\d+|\d+", parts[3])
            ltp_max = max(
                [float(ltp) for ltp in ltps if ltp.replace(".", "", 1).isdigit()]
            )
            if float(targets[0]) < float(ltps[0]):
                targets = [
                    str(float(target) + ltp_max)
                    for target in targets
                    if target.replace(".", "", 1).isdigit()
                ]
            __signal_details = {
                "channel_name": "BnoPremium",
                "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                "ltp_range": "|".join(ltps),
                "target_range": "|".join(targets),
                "sl": re.findall(r"\d+\.\d+|\d+", parts[2])[0],
                "quantity": get_multiplier(
                    symbol_dict["Trading Symbol"],
                    BnoPremium.channel_details,
                    len(targets),
                ),
                "action": "Cancel" if is_close_msg else "Buy",
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
    split_words = ["BUY", "ABOVE", "SL", "TGT"]
    channel_details = CHANNEL_DETAILS["StockPremium"]
    channel_number = channel_details["channel_number"]

    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        self.message = telegram_msg

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
            first_row = sorted_df.head(1)
            return first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0]
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
                write_failure_to_csv(failure_details)
                return

            for word in StockPremium.split_words:
                statement = statement.replace(word, "|")
            parts = statement.split("|")
            print(parts)
            symbol_from_tg = parts[1].strip().removeprefix("#")
            sym, *_ = symbol_from_tg.upper().split()
            symbol_dict = self.get_instrument_name(symbol_from_tg)
            ltps = re.findall(r"\d+\.\d+|\d+", parts[2].strip())
            targets = re.findall(r"\d+\.\d+|\d+", parts[4].strip())
            ltp_max = max(
                [float(ltp) for ltp in ltps if ltp.replace(".", "", 1).isdigit()]
            )
            if float(targets[0]) < float(ltps[0]):
                targets = [
                    str(float(target) + ltp_max)
                    for target in targets
                    if target.replace(".", "", 1).isdigit()
                ]
            __signal_details = {
                "channel_name": "StockPremium",
                "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                "ltp_range": "|".join(ltps),
                "target_range": "|".join(targets),
                "sl": re.findall(r"\d+\.\d+|\d+", parts[3].strip())[0],
                "quantity": get_multiplier(
                    symbol_dict["Trading Symbol"],
                    StockPremium.channel_details,
                    len(targets),
                ),
                "action": "Cancel" if is_close_msg else "Buy",
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


class PremiumGroup:
    split_words = [
        "ABOVE",
        "ABV",
        " AT",
        "SL",
        "TARGETS",
        "TARGET",
        "TRT",
        "STOPLOSS",
        "TRG",
        "NEAR",
    ]
    channel_details = CHANNEL_DETAILS["PremiumGroup"]
    channel_number = channel_details["channel_number"]

    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        try:
            self.message = (
                telegram_msg.upper().split("BUY ")[1].replace("-", " ").replace(",", " ").replace("/", " ")
            )
        except:
            self.message = (
                telegram_msg.upper().replace("-", " ").replace(",", " ").replace("/", " ")
            )

    def get_float_values(self, string_val, start_val, split_values):
        float_values = []
        v = string_val.split(start_val)
        for word in re.split(split_values, v[1]):
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
            first_row = sorted_df.head(1)
            return first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0]
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
                    "channel_name": "PremiumGroup",
                    "timestamp": self.msg_received_timestamp,
                    "message": self.message,
                    "exception": "is a reply message but not having close words. Possible duplicate or junk",
                }
                write_failure_to_csv(failure_details)
                return

            for word in PremiumGroup.split_words:
                statement = statement.replace(word, "|")
            parts = statement.split("|")
            print(parts)
            symbol_from_tg = parts[1].strip().removeprefix("BUY").strip()
            sym, *_ = symbol_from_tg.upper().split()
            symbol_dict = self.get_instrument_name(symbol_from_tg)
            ltps = re.findall(r"\d+\.\d+|\d+", parts[1].strip())
            for target_keyword in ["TARGETS", "TARGET", "TRT", "TRG"]:
                targets = self.get_float_values(self.message, target_keyword, " ")
                if targets:
                    break
            else:
                raise CustomError("TARGET not found")
            ltp_max = max(
                [float(ltp) for ltp in ltps if ltp.replace(".", "", 1).isdigit()]
            )
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
            else:
                raise CustomError("SL not found")
            __signal_details = {
                "channel_name": "PremiumGroup",
                "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                "ltp_range": "|".join(ltps),
                "target_range": "|".join(targets),
                "sl": sl,
                "quantity": get_multiplier(
                    symbol_dict["Trading Symbol"],
                    PremiumGroup.channel_details,
                    len(targets),
                ),
                "action": "Cancel" if is_close_msg else "Buy",
            }
            if __signal_details in signals:
                raise CustomError("Signal already exists")
            else:
                signals.append(__signal_details)
            signal_details = __signal_details.copy()
            signal_details["timestamp"] = (
                f"{PremiumGroup.channel_number}{self.msg_received_timestamp}"
            )
            write_signals_to_csv(signal_details)
        except:
            failure_details = {
                "channel_name": "PremiumGroup",
                "timestamp": self.msg_received_timestamp,
                "message": self.message,
                "exception": traceback.format_exc().strip(),
            }
            logger.error(failure_details)
            write_failure_to_csv(failure_details)


class PremiumMembershipGroup:
    split_words = ["BUY", "ABOVE", "TARGET", "SL"]
    channel_details = CHANNEL_DETAILS["PremiumMembershipGroup"]
    channel_number = channel_details["channel_number"]

    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        self.message = re.sub(r"\.+", ".", telegram_msg.upper())
        self.message = self.message.replace(":-", " ").replace(":", " ")

    def get_float_values(self, string_val, start_val, split_values):
        float_values = []
        v = string_val.split(start_val)
        for word in re.split(split_values, v[1]):
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
            first_row = sorted_df.head(1)
            return first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0]
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
                write_failure_to_csv(failure_details)
                return

            for word in PremiumMembershipGroup.split_words:
                statement = statement.replace(word, "|")
            parts = statement.split("|")
            print(parts)
            symbol_from_tg = parts[0].strip().removeprefix("#")
            sym, *_ = symbol_from_tg.upper().split()
            symbol_dict = self.get_instrument_name(symbol_from_tg)
            ltps = re.findall(r"\d+\.\d+|\d+", parts[2].strip())
            targets = self.get_float_values(self.message, "TARGET", "/")
            sl = self.get_float_values(self.message, "SL", "/")[0]
            # targets = re.findall(r"\d+\.\d+|\d+", parts[3].strip())
            ltp_max = max(
                [float(ltp) for ltp in ltps if ltp.replace(".", "", 1).isdigit()]
            )
            if float(targets[0]) < float(ltps[0]):
                targets = [
                    str(float(target) + ltp_max)
                    for target in targets
                    if target.replace(".", "", 1).isdigit()
                ]
            __signal_details = {
                "channel_name": "PremiumMembershipGroup",
                "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                "ltp_range": "|".join(ltps),
                "target_range": "|".join(targets),
                "sl": sl,
                "quantity": get_multiplier(
                    symbol_dict["Trading Symbol"],
                    PremiumMembershipGroup.channel_details,
                    len(targets),
                ),
                "action": "Cancel" if is_close_msg else "Buy",
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


class LiveTradingGroup:
    split_words = ["ABOVE", "TARGET", "SL"]
    channel_details = CHANNEL_DETAILS["LiveTradingGroup"]
    channel_number = channel_details["channel_number"]

    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        self.message = re.sub(r"\.+", ".", telegram_msg.upper())

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
            first_row = sorted_df.head(1)
            return first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0]
        except:
            raise CustomError(traceback.format_exc())

    def get_signal(self):
        try:
            statement = self.message
            if "SL" not in statement:
                raise CustomError("SL not found")
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
                    "channel_name": "LiveTradingGroup",
                    "timestamp": self.msg_received_timestamp,
                    "message": self.message,
                    "exception": "is a reply message but not having close words. Possible duplicate or junk",
                }
                write_failure_to_csv(failure_details)
                return

            for word in LiveTradingGroup.split_words:
                statement = statement.replace(word, "|")
            parts = statement.split("|")
            print(parts)
            symbol_from_tg = parts[0].strip().removeprefix("#")
            sym, *_ = symbol_from_tg.upper().split()
            symbol_dict = self.get_instrument_name(symbol_from_tg)
            ltps = re.findall(r"\d+\.\d+|\d+", parts[1].strip())
            targets = re.findall(r"\d+\.\d+|\d+", parts[3].strip())
            ltp_max = max(
                [float(ltp) for ltp in ltps if ltp.replace(".", "", 1).isdigit()]
            )
            if float(targets[0]) < float(ltps[0]):
                targets = [
                    str(float(target) + ltp_max)
                    for target in targets
                    if target.replace(".", "", 1).isdigit()
                ]
            __signal_details = {
                "channel_name": "LiveTradingGroup",
                "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                "ltp_range": "|".join(ltps),
                "target_range": "|".join(targets),
                "sl": re.findall(r"\d+\.\d+|\d+", parts[2].strip())[0],
                "quantity": get_multiplier(
                    symbol_dict["Trading Symbol"],
                    LiveTradingGroup.channel_details,
                    len(targets),
                ),
                "action": "Cancel" if is_close_msg else "Buy",
            }
            if __signal_details in signals:
                raise CustomError("Signal already exists")
            else:
                signals.append(__signal_details)
            signal_details = __signal_details.copy()
            signal_details["timestamp"] = (
                f"{LiveTradingGroup.channel_number}{self.msg_received_timestamp}"
            )
            write_signals_to_csv(signal_details)
        except:
            failure_details = {
                "channel_name": "LiveTradingGroup",
                "timestamp": self.msg_received_timestamp,
                "message": self.message,
                "exception": traceback.format_exc().strip(),
            }
            logger.error(failure_details)
            write_failure_to_csv(failure_details)


class SChoudhry12:
    split_words = ["BUY", "ABOVE", "TGT", "SL"]
    channel_details = CHANNEL_DETAILS["SChoudhry12"]
    channel_number = channel_details["channel_number"]

    def __init__(self, msg_received_timestamp, telegram_msg):
        self.msg_received_timestamp = msg_received_timestamp
        self.message = re.sub(r"\.+", ".", telegram_msg.upper())

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
            first_row = sorted_df.head(1)
            return first_row[["Exch", "Trading Symbol"]].to_dict(orient="records")[0]
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
                    "channel_name": "SChoudhry12",
                    "timestamp": self.msg_received_timestamp,
                    "message": self.message,
                    "exception": "is a reply message but not having close words. Possible duplicate or junk",
                }
                write_failure_to_csv(failure_details)
                return

            for word in SChoudhry12.split_words:
                statement = statement.replace(word, "|")
            parts = statement.split("|")
            print(parts)
            symbol_from_tg = parts[1].strip().removeprefix("#")
            sym, *_ = symbol_from_tg.upper().split()
            symbol_dict = self.get_instrument_name(symbol_from_tg)
            ltps = re.findall(r"\d+\.\d+|\d+", parts[2].strip())
            targets = re.findall(r"\d+\.\d+|\d+", parts[3].strip())
            ltp_max = max(
                [float(ltp) for ltp in ltps if ltp.replace(".", "", 1).isdigit()]
            )
            if float(targets[0]) < float(ltps[0]):
                targets = [
                    str(float(target) + ltp_max)
                    for target in targets
                    if target.replace(".", "", 1).isdigit()
                ]
            __signal_details = {
                "channel_name": "SChoudhry12",
                "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                "ltp_range": "|".join(ltps),
                "target_range": "|".join(targets),
                "sl": re.findall(r"\d+\.\d+|\d+", parts[4].strip())[0],
                "quantity": get_multiplier(
                    symbol_dict["Trading Symbol"],
                    SChoudhry12.channel_details,
                    len(targets),
                ),
                "action": "Cancel" if is_close_msg else "Buy",
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
