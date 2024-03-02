
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
# msg = "Buy BankNifty Feb 46900 CE Only In Range @ 195 - 215 Target 240 270 300 350 & Above SL For Trade @ 124"
# i = SmsOptionsPremium(1707192066, msg)
# i.get_signal()

# msg = "BUY #SENSEX 73900 CE ABOVE -20-25 TARGET- 35,80 SL-0  EXPIRY 1ST MARCH  HERO ZERO CALL BUY ONLY 1-3 LOT"
# i = PremiumJackpot(1707192066, msg)
# i.get_signal()

# msg = "14th feb expiry  BUY 45200 CE only abv 220 SL-180 TARGET -400-500-750  cmp - 190"
# msg = "21st feb expiry  BUY 45600 PE only abv 330 SL-290 TARGET -400-500++  cmp - 310"
# msg = "28th feb expiry  BUY 47500 PE only abv 250 SL - 210 TARGET -450-550+  cmp - 240"
# i = PaidCallPut(1707192066, msg)
# i.get_signal()


# for msg in ["6th march expiry  BUY 46800 CE ONLY ABV 350 SL- 310 TARGET -460  CMP - 335", "7th march expiry nifty  buy 22150 PE CMP 135/130 SL-120 TARGTE-200-250+"]:
#     i = PaidCallPut(1707192066, msg)
#     i.get_signal()