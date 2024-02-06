
input_file = "data/output.csv"
import pandas as pd

from telegram_message_parser import PremiumJackpot, SmsOptionsPremium, PaidCallPut



df = pd.read_csv(input_file, header=None)
for i, row in df.iterrows():
    # print(i, row)
    if row[2] == "PREMIUM JACKPOT":
        i = PremiumJackpot(row[0], row[3])
        i.get_signal()
    elif row[2] == "SMS Options Premium":
        i = SmsOptionsPremium(row[0], row[3])
        i.get_signal()
    elif row[2] == "Paid - CALL & PUT":
        i = PaidCallPut(row[0], row[3])
        i.get_signal()


# msg = "BANKNIFTY 7 FEB 45800 CE IF CROSSES & SPOT SUSTAIN ONLY ABOVE 253.85 WILL TRY TO HIT TARGETS @ 275 300 330 360 400 & ABOVE$$$$"
# i = SmsOptionsPremium(1707192066, msg)
# i.get_signal()