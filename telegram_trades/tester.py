
input_file = "data/output.csv"
import pandas as pd

from telegram_message_parser import PremiumJackpot, SmsOptionsPremium, PaidCallPut



df = pd.read_csv(input_file, header=None)
for i, row in df.iterrows():
    # print(i, row)
    if row[1] == "PREMIUM JACKPOT":
        i = PremiumJackpot(row[0], row[2])
        i.get_signal()
    elif row[1] == "SMS Options Premium":
        i = SmsOptionsPremium(row[0], row[2])
        i.get_signal()
    elif row[1] == "Paid - CALL & PUT":
        i = PaidCallPut(row[0], row[2])
        i.get_signal()


# msg = "BANKNIFTY 7 FEB 45800 CE IF CROSSES & SPOT SUSTAIN ONLY ABOVE 253.85 WILL TRY TO HIT TARGETS @ 275 300 330 360 400 & ABOVE$$$$"
# i = SmsOptionsPremium(1707192066, msg)
# i.get_signal()

# msg = "14th feb expiry  BUY 45200 CE only abv 220 SL-180 TARGET -400-500-750  cmp - 190"
# msg = "21st feb expiry  BUY 45600 PE only abv 330 SL-290 TARGET -400-500++  cmp - 310"
# i = PaidCallPut(1707192066, msg)
# i.get_signal()


# for msg in ["14th feb expiry  BUY 45200 CE only abv 220 SL-180 TARGET -400-500-750  cmp - 190", "14th feb expiry  BUY 45200 CE only abv 220 SL-180 TARGET -400-500-750  cmp - 190"]:
#     i = PaidCallPut(1707192066, msg)
#     i.get_signal()