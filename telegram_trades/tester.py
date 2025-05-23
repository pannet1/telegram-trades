input_file = "../data/output.csv"
import pandas as pd

from telegram_message_parser_v2 import PremiumJackpot, SmsOptionsPremium, PaidCallPut, PaidStockIndexOption, BnoPremium, StockPremium, StudentsGroup, PremiumMembershipGroup, SChoudhry12, VipPremiumPaidCalls, PlatinumMembers

msg = "BUY NIFTY 20TH FEB 22900 CE   BUY ABOVE = 165  SL = 145  TGT = 180/200/230+"
i = PlatinumMembers(1707192066, msg)
i.get_signal()
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
#     elif row[1] == "STUDENTS GROUP":
#         i = StudentsGroup(row[0], row[2])
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
#     elif row[1] == "VIP PREMIUM PAID CALLS":
#         i = VipPremiumPaidCalls(row[0], row[2])
#         i.get_signal()
#     elif row[1] == "PLATINUM MEMBERS":
#         i = PlatinumMembers(row[0], row[2])
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
# msg = "BUY BANKNIFTY 50500 CE  ABOVE 490     SL 450 TGT 530/600"
# i = BnoPremium(1707192066, msg)
# i.get_signal()
# msg = "SELL#PFC FUT  NEAR -416 TARGET- 414,410 SL-420 416 -414      HIT TARGET     7,750 PROFIT IN JUST ONE LOT  #PFC FUT   "
# i = PremiumJackpot(1707192066, msg)
# i.get_signal()


# msg = "BUY#BHEL FUT  ABOVE -224 TARGET- 227,228 SL-222 224 -226      SAFE BOOK SMALL PROFIT   10,500 PROFIT IN JUST ONE LOT  #BHEL FUT     # "
# i = PremiumJackpot(1707192066, msg)
# i.get_signal()


# msg = "BUY#BAJAJ-AUTO  FUT  ABOVE -8330 TARGET-8350,8400,8450 SL-8300 "
# i = PremiumJackpot(1707192066, msg)
# i.get_signal()

# msg = "BUY #ADANIENT FUT  ABOVE -2960-2965 TARGET- 2975,3200 SL-2949  EXPIRY JUN$$$$2975      HIT TARGET   CANCEL   BOOK OR TRAIL SL BIG JUMP POSSIBLE ANYTIME  "
# i = PremiumJackpot(1707192066, msg)
# i.get_signal()

# msg = "MONTHLY CALL   BUY NTPC FUTURE 390  TARGETS 420-450   SL380 "
# i = PremiumGroup(1707192066, msg)
# i.get_signal()

# msg = "MONTHLY CALL   BUY NTPC FUTURE 390  TARGETS 420-450   SL380$$$$62  "
# i = PremiumGroup(1707192066, msg)
# i.get_signal()

# msg = "FUTURE TRADE     BUY  NIFTY RANGE 22640-22650 TRG 22750/22950/23100SL 22500"
# i = PaidStockIndexOption(1707192066, msg)
# i.get_signal()

# msg = "FUTURE TRADE     BUY  NIFTY RANGE 22640-22650 TRG 22750/22950/23100SL 22500$$$$FUTURE NIFTY 22640 TO 23844  1204++ POINTS PROFIT RUNNING     BOOK PROFIT ALL TARGET ACHIEVE     "
# i = PaidStockIndexOption(1707192066, msg)
# i.get_signal()

# msg = "BANNIFTY 10 JUL 52400 PE    ABOVE  170   TARGET @250/300//350    SL: 150"
# i = VipPremiumPaidCalls(1707192066, msg)
# i.get_signal()

# msg = "BANKNIFTY 10 JUL 52500 PE   ABOVE :  310   STOPLOSS : 280"
# i = AllIn1Group(1707192066, msg)
# i.get_signal()
# msg = "            Buy 1 Lot Nifty 21450 PE Only In Range @ 25 - 33 Target 50 75 100 125 150 & Above             Buy 1 Lot BankNifty 45300 CE Only In Range @ 90 - 110 Target 140 170 200 230 275 & Above  Ratio Of Today's Cross Index Hedge Trade  BankNifty - 1 Lot N Nifty - 1 Lot Nifty 15 Feb 21450 PE  @ 32 se 32 = 0 Point = 0/- Loss   "
# i = SmsOptionsPremium(1707192066, msg)
# i.get_signal()
# msg = "                        BUY BANKNIFTY 24 JUL 51000 CE ONLY IN RANGE @ 75 - 100 TARGET 130 160 190 220 250 & ABOVE             BUY BANKNIFTY 24 JUL 50900 PE ONLY IN RANGE @ 75 - 100 TARGET 130 160 190 220 250 & ABOVE"
# i = SmsOptionsPremium(1707192066, msg)
# i.get_signal()
# msg = "            Buy FinNifty Mar 20750 CE Only In Range @ 5 - 11 Target 25 40 60 80 100 & Above             Buy FinNifty Mar 20700 PE Only In Range @ 4 - 8 Target 25 40 60 80 100 & Above FinNifty Mar 20750 CE  @ 8 se 2 = 6 Point = 240/- Loss    FinNifty Mar 20700 PE  Four Times  @ 6 se 26 = 20 Points = 800/- Profit    Total Profits @ 560/- Per Hedge Pair     Book 70% To 90% Position Of Hedge Trade In Profits   "
# i = SmsOptionsPremium(1707192066, msg)
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


# msgs = [
# "BUY 2 LOT NIFTY 23500 CE ONLY IN RANGE @ 32 - 40 TARGET 60 80 100 125 150 & ABOVE             BUY 1 LOT BANKNIFTY 50100 PE ONLY IN RANGE @ 125 - 145  TARGET 170 200 230 260 300 & ABOVE  RATIO OF TODAY'S CROSS INDEX HEDGE TRADE  BANKNIFTY - 1 LOT N NIFTY - 2 LOTS"
# ]
# for msg in msgs:
#   print(msg)
#   i = SmsOptionsPremium(1707192066, msg)
#   i.get_signal()
#   print("====")

# msg = "INTRADAY STOCK OPTION TRADE     BUY MARUTI 12400 PE RANGE 180-185 TRG 270-350 SL  170"
# i = PaidStockIndexOption(1707192066, msg)
# i.get_signal()
# msg = "BANKNIFTY49300 CE  ABOVE 255  SL 220  TG 275/300/350++++"
# i = VipPremiumPaidCalls(1707192066, msg)
# i.get_signal()


# msg = "BUY BANKNIFTY 49400 CE (  29 MAY EX)   ABOVE 200  TARGET 250/300  SL  170"
# i = PremiumMembershipGroup(1707192066, msg)
# i.get_signal()

# msg = "BUY MIDCPNIFTY 11650 CE ( 27 MAY EX)  ABOVE  50  TARGET  75/100  SL 40"
# i = PremiumMembershipGroup(1707192066, msg)
# i.get_signal()

# msg = "BUY NIFTY 22550 PE ONLY ABV 65 SL-55 TARGTE-120-150+  CMP- 62"
# i = PaidCallPut(1707192066, msg)
# i.get_signal()

# msg = "BANKNIFTY EXPIRY SPECIAL ZERO TO HERO JACKPOT TRADE     BUY BANKNIFTY 50500 CE    BUY ABOVE  73   SL  46/00   TGT  100/130/160+"
# i = PlatinumMembers(1707192066, msg)
# i.get_signal()


# msg = "BUY IRCTC 1040 CE  ABOVE 18.50 SL "
# i = BnoPremium(1707192066, msg)
# i.get_signal()