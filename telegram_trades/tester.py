
input_file = "data/output.csv"
import pandas as pd

from telegram_message_parser import PremiumJackpot, SmsOptionsPremium, PaidCallPut



df = pd.read_csv(input_file, header=None)
for i, row in df.iterrows():
    print(i, row)
    if row[2] == "PREMIUM JACKPOT":
        i = PremiumJackpot(row[1], row[3])
        i.get_signal()
    elif row[2] == "SMS Options Premium":
        i = SmsOptionsPremium(row[1], row[3])
        i.get_signal()
    elif row[2] == "Paid - CALL & PUT":
        i = PaidCallPut(row[1], row[3])
        i.get_signal()