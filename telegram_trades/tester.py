input_file = "../data/output.csv"
import pandas as pd

from telegram_message_parser_v2 import PremiumJackpot, SmsOptionsPremium, PaidCallPut, PaidStockIndexOption, BnoPremium, StockPremium, PremiumGroup, PremiumMembershipGroup, LiveTradingGroup, SChoudhry12


# df = pd.read_csv(input_file, header=None)
# for i, row in df.iterrows():
#     #     # print(i, row)
#     if row[1] == "PREMIUM JACKPOT":
#         i = PremiumJackpot(row[0], row[2])
#         i.get_signal()
#     elif row[1] == "SMS OPTIONS PREMIUM":
#         i = SmsOptionsPremium(row[0], row[2])
#         i.get_signal()
#     elif row[1] == "PAID - CALL & PUT":
#         i = PaidCallPut(row[0], row[2])
#         i.get_signal()
#     elif row[1] == "PAID STOCK & INDEX OPTION":
#         i = PaidStockIndexOption(row[0], row[2])
#         i.get_signal()
#     elif row[1] == "PREMIUM MEMBERSHIP GROUP":
#         i = PremiumMembershipGroup(row[0], row[2])
#         i.get_signal()
#     elif row[1] == "LIVE TRADING+ LOSS RECOVERY GROUP":
#         i = LiveTradingGroup(row[0], row[2])
#         i.get_signal()
#     elif row[1] == "PREMIUM GROUP":
#         i = PremiumGroup(row[0], row[2])
#         i.get_signal()
#     elif row[1] == "STOCK PREMIUM":
#         i = StockPremium(row[0], row[2])
#         i.get_signal()
#     elif row[1] == "USER-SCHOUDHRY12":
#         i = SChoudhry12(row[0], row[2])
#         i.get_signal()
#     elif row[1] == "BNO PREMIUM":
#         i = BnoPremium(row[0], row[2])
#         i.get_signal()


# msg = "BANKNIFTY 7 FEB 45800 CE IF CROSSES & SPOT SUSTAIN ONLY ABOVE 253.85 WILL TRY TO HIT TARGETS @ 275 300 330 360 400 & ABOVE$$$$"
# msg = "Buy BankNifty Feb 46900 CE Only In Range @ 195 - 215 Target 240 270 300 350 & Above SL For Trade @ 124"
# i = SmsOptionsPremium(1707192066, msg)
# i.get_signal()

# msg = "BUY #BANKEX 52300 PE ABOVE -10-15 TARGET- 22,30,50 SL-0  EXPIRY 18TH MAR  BUY ONLY 1-3 LOT"
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
# close_words = ("CANCEL", "EXIT", "BOOK", "HIT", "BREAK", "AVOID", "PROFIT", "LOSS", "TRIAL", "TRAIL", "IGNORE")

# 01-04-2024
# msg = "INTRADAY + BTST STOCK OPTION TRADE     BUY MARUTI 11700 PE RANGE 190-200 TRG 270-350-430 SL 140 MARUTI 11700 PE 200 TO 252   26%+ PROFIT 52++ POINTS RUNNING     SAFE TRADERS CAN BOOK PROFIT  OR TRAIL SL   "
# i = PaidStockIndexOption(1707192066, msg)
# i.get_signal()

# msg = "	BUY#ABB 6100 CE  ABOVE -160 TARGE-170,190+ SL-145  MAR SERIES  WAIT FOR LEVEL CROSS NOT ACTIVE AVOID   "
# i = PremiumJackpot(1707192066, msg)
# i.get_signal()

# msg = "   BUY#RELIANCE 2900 CE  ABOVE -85 TARGE-89,95 SL-73  MAR SERIES  WAIT FOR LEVEL CROSS 96  HIT TARGET     BOOK OR TRAIL SL"
# i = PremiumJackpot(1707192066, msg)
# i.get_signal()

# msg = " 	BUY#LUPIN 1620 ABOVE - 85 TARGE-87,90 SL-82  APR  SERIES  WAIT FOR LEVEL CROSS TRAIL SL    "
# i = PremiumJackpot(1707192066, msg)
# i.get_signal()

# msg = "BUY#IRCTC 1000 ABOVE -45.5-46 TARGE-48,55 SL-40  APR  SERIES  WAIT FOR LEVEL CROSS"
# i = PremiumJackpot(1707192066, msg)
# i.get_signal()

# msg = "INTRADAY STOCK OPTION TRADE     BUY MARUTI 12400 PE RANGE 180-185 TRG 270-350 SL  170"
# i = PaidStockIndexOption(1707192066, msg)
# i.get_signal()
msg = "INTRADAY STOCK OPTION TRADE     BUY MARUTI 12400 PE RANGE 180-185 TRG 270-350 SL  170"
i = PaidStockIndexOption(1707192066, msg)
i.get_signal()
