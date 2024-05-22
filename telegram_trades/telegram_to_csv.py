from telethon.sync import TelegramClient, events
import csv
from datetime import datetime
from constants import TGRM, DATA
from telegram_message_parser_v2 import (
    PremiumJackpot, SmsOptionsPremium, PaidCallPut, PaidStockIndexOption,
    BnoPremium, StockPremium, PremiumGroup, PremiumMembershipGroup, 
    LiveTradingGroup, SChoudhry12, VipPremiumPaidCalls, PlatinumMembers)
from logzero import setup_logger
import traceback

logger = setup_logger(logfile="telegram_to_csv.log")
api_id = TGRM['api_id']
api_hash = TGRM['api_hash']
channel_ids = TGRM['channel_ids']

client = TelegramClient('anon', api_id, api_hash)

replace_non_ascii = lambda s: str(''.join(' ' if ord(i) >= 128 or i == '\n' or i == "₹" else i.upper() for i in s))
# replace_non_ascii = lambda s: str(''.join(' ' if str(i).isalnum() or str(i).strip().isspace() or i == '\n' or i == "₹" else i.upper() for i in s))

@client.on(events.NewMessage(chats=channel_ids))
async def my_event_handler(event):
    chat = await event.get_chat()
    with open(DATA + TGRM["output_file"], 'a', encoding='utf-8',  newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        now = int(datetime.now().timestamp())
        try:
            chat_title = replace_non_ascii(chat.title).strip().upper()
        except:
            if chat.id == 493143987:
                chat_title = "USER-SCHOUDHRY12"
            else:
                chat_title = chat.id
        if event.reply_to_msg_id is not None:
            original_message = await client.get_messages(chat.id, ids=event.reply_to_msg_id)
            msg = replace_non_ascii(original_message.raw_text) +"$$$$"+ replace_non_ascii(event.raw_text)
            csv_writer.writerow([now, chat_title, msg,])
        else:
            msg = replace_non_ascii(event.raw_text)
            csv_writer.writerow([now, chat_title, msg])
        
        try:
            if chat_title == "PREMIUM JACKPOT":
                i = PremiumJackpot(now, msg)
                i.get_signal()
            elif chat_title == "SMS OPTIONS PREMIUM":
                i = SmsOptionsPremium(now, msg)
                i.get_signal()
            elif chat_title == "PAID - CALL & PUT":
                i = PaidCallPut(now, msg)
                i.get_signal()
            elif chat_title == "PAID STOCK & INDEX OPTION":
                i = PaidStockIndexOption(now, msg)
                i.get_signal()
            elif chat_title == "PREMIUM MEMBERSHIP GROUP":
                logger.info(f"Premium Memebership group - {msg} - vs - {event.raw_text}")
                i = PremiumMembershipGroup(now, msg)
                i.get_signal()
            elif chat_title == "LIVE TRADING+ LOSS RECOVERY GROUP":
                i = LiveTradingGroup(now, msg)
                i.get_signal()
            elif chat_title == "PREMIUM GROUP":
                i = PremiumGroup(now, msg)
                i.get_signal()
            elif chat_title == "STOCK PREMIUM":
                i = StockPremium(now, msg)
                i.get_signal()
            elif chat_title == "USER-SCHOUDHRY12":
                i = SChoudhry12(now, msg)
                i.get_signal()
            elif chat_title == "BNO PREMIUM":
                i = BnoPremium(now, msg)
                i.get_signal()
            elif chat_title == "VIP PREMIUM PAID CALLS":
                i = VipPremiumPaidCalls(now, msg)
                i.get_signal()
            elif chat_title == "PLATINUM MEMBERS":
                i = PlatinumMembers(now, msg)
                i.get_signal()
            else:
                logger.info(f"{chat_title} ===> {msg}")
        except:
            logger.error(traceback.format_exc())

client.start(phone=TGRM["phone_number"])
client.run_until_disconnected()
