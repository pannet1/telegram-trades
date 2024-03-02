import re
import glob
import pandas as pd
import difflib
import csv
import traceback
from datetime import datetime
from login import get_broker
from constants import BRKR, FUTL
from logzero import logger

signals_csv_filename = "data/signals.csv"
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
failure_csv_filename = "data/failures.csv"
failure_csv_file_headers = ["channel_name", "timestamp", "message", "exception", "normal_timestamp",]
signals = []
spell_checks = {
    "F1NIFTY": "FINNIFTY",
    "N1FTY": "NIFTY",
}
close_words = ("CANCEL", "EXIT", "BOOK", "HIT", "BREAK", "AVOID", "PROFIT", "LOSS", "TRIAL", "IGNORE")

class CustomError(Exception):
    pass

def download_masters(broker):
    exchanges = ["NFO", "BFO"]
    for exchange in exchanges:
        if FUTL.is_file_not_2day(f"./{exchange}.csv"):
            broker.get_contract_master(exchange)


def get_multiplier(symbol):
    nfo_df = pd.read_csv("NFO.csv")
    bfo_df = pd.read_csv("BFO.csv")
    df = pd.concat([nfo_df, bfo_df])
    lot_size = df.loc[df['Trading Symbol'] == symbol, 'Lot Size'].iloc[0]
    if "BANKNIFTY" in symbol:
        return f"{lot_size|lot_size}"
    elif "FINNIFTY" in symbol:
        return f"{lot_size|lot_size}"
    elif "MIDCPNIFTY" in symbol:
        return f"{lot_size|lot_size}"
    elif "NIFTY" in symbol:
        return f"{lot_size|lot_size}"
    elif "SENSEX" in symbol:
        return f"{lot_size|lot_size}"
    elif "BANKEX" in symbol:
        return f"{lot_size|lot_size}"    
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
        _signal_details["normal_timestamp"] = datetime.fromtimestamp(int(_signal_details["timestamp"][1:])).strftime('%Y-%m-%d %H:%M:%S')
        writer.writerow(
            {k: _signal_details.get(k, "") for k in signals_csv_file_headers}
        )


def write_failure_to_csv(failure_details):
    with open(failure_csv_filename, "a", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=failure_csv_file_headers)
        failure_details["normal_timestamp"] = datetime.fromtimestamp(failure_details["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        writer.writerow(
            {k: failure_details.get(k, "") for k in failure_csv_file_headers}
        )


api = get_broker(BRKR)
download_masters(api.broker)
scrip_info_df = get_all_contract_details()
all_symbols = set(scrip_info_df["Symbol"].to_list())


class PremiumJackpot:
    split_words = ["BUY", "ABOVE", "NEAR", "TARGET", "TARGE"]
    channel_number = 1

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
            sym, strike, option_type = symbol_from_tg.split()
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
            is_close_msg = any([word in new_msg for word in close_words])
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
            __signal_details = {
                "channel_name": "Premium jackpot",
                "symbol": symbol_dict["Exch"]+":"+symbol_dict["Trading Symbol"],
                "ltp_range": "|".join(re.findall(r"\d+\.\d+|\d+", parts[2])),
                "target_range": "|".join(
                    re.findall(r"\d+\.\d+|\d+", parts[3].split("SL")[0])
                ),
                "sl": re.findall(r"SL-(\d+)?", parts[3])[0],
                "quantity": get_multiplier(symbol_dict["Trading Symbol"]),
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
            logger.info(signal_details)
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
    split_words = ["BUY", "ONLY IN RANGE @", "TARGET" ,"SL FOR TRADE @ "]
    spot_sl = .15
    channel_number = 2

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
            if word.replace(".", "", 1).isdigit():
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
                sl = float(ltp_range[0]) * (1 - SmsOptionsPremium.spot_sl)
                _signal_details = {
                    "channel_name": "SmsOptionsPremium",
                    "symbol": symbol_dict["Exch"]+":"+symbol_dict["Trading Symbol"],
                    "ltp_range": "|".join(self.get_float_values(statement, "ABOVE ")),
                    "target_range": "|".join(
                        self.get_float_values(statement, "TARGETS @ ")
                    ),
                    "sl": sl,
                    "quantity": get_multiplier(symbol_dict["Trading Symbol"]),
                    "action": "Buy"
                }
                if _signal_details in signals:
                    raise CustomError("Signal already exists")
                else:
                    signals.append(_signal_details)
                signal_details = _signal_details.copy()
                signal_details["timestamp"] = f"{SmsOptionsPremium.channel_number}{self.msg_received_timestamp}"
                logger.info(signal_details)
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
        is_close_msg = any([word in new_msg for word in close_words])
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
            sl = re.findall(r"(\d+)?", parts[4])[0]
            if not sl:
                sl = re.findall(r"(\d+)?", statement.split("$$$$")[-1])[0]
                if not sl:
                    raise CustomError(f"SL is not found in {parts[4]}")
            symbol_dict = self.get_instrument_name(parts[1].upper().strip())          
            _signal_details = {
                "channel_name": "SmsOptionsPremium",
                "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                "ltp_range": "|".join(re.findall(r"\d+\.\d+|\d+", parts[2])),
                "target_range": "|".join(
                    self.get_float_values(self.message.strip().upper(), "TARGET")
                ),
                "sl": sl,
                "quantity": get_multiplier(symbol_dict["Trading Symbol"]),
                "action": "Cancel" if is_close_msg else "Buy",
            }
            if _signal_details in signals:
                raise CustomError("Signal already exists")
            signals.append(_signal_details)
            signal_details = _signal_details.copy()
            signal_details["timestamp"] = f"{SmsOptionsPremium.channel_number}{self.msg_received_timestamp}"
            logger.info(signal_details)
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
    
    channel_number = 3
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
        filtered_df = df[
            (df["Exch"] == "NFO")
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
            v = string_val.upper().replace("-", " ").replace("/", " ").split(start_val)
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
            is_close_msg = any([word in new_msg for word in close_words])
            is_reply_msg = '$$$$' in self.message
            if is_reply_msg and is_close_msg :
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
            sl_list = self.get_float_values(self.message.strip().upper().replace("-",""), "SL")
            if sl_list:
                sl = sl_list[0]
            else:
                raise CustomError("SL is not available")
                # if word.upper().strip().startswith("SL-"):
                #     sl = re.findall(r"SL-(\d+)?", word.upper().strip())[0]
            if strike == None or option == None:
                raise CustomError("Strike or Option is None")
            targets = self.get_target_values(self.message.replace("TARGTE", "TARGET"), "TARGET")
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
                raise CustomError("target values is not found")
            _signal_details = {
                "channel_name": "PaidCallPut",
                "symbol": symbol_dict["Exch"] + ":" + symbol_dict["Trading Symbol"],
                "ltp_range": "|".join(ltp_range),
                "target_range": "|".join(targets),
                "sl": sl,
                "quantity": get_multiplier(symbol_dict["Trading Symbol"]),
                "action": "Cancel" if is_close_msg else "Buy",
            }
            if _signal_details in signals:
                raise CustomError("Signal already exists")
            else:
                signals.append(_signal_details)
            signal_details = _signal_details.copy()
            signal_details["timestamp"] = f"{PaidCallPut.channel_number}{self.msg_received_timestamp}"
            logger.info(signal_details)
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
