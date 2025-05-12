from telethon.sync import TelegramClient, events
import csv
import unicodedata
from datetime import datetime
from constants import TGRM, DATA
from telegram_message_parser_v2 import (
    PremiumJackpot, SmsOptionsPremium, PaidCallPut, PaidStockIndexOption,
    BnoPremium, StockPremium, StudentsGroup, PremiumMembershipGroup, 
    AllIn1Group, SChoudhry12, VipPremiumPaidCalls, PlatinumMembers,
    PremiumFXG, SmsStockOptionsPremium, BankNiftyRani)
from logzero import setup_logger
import traceback

logger = setup_logger(logfile="telegram_to_csv.log")
api_id = TGRM['api_id']
api_hash = TGRM['api_hash']
channel_ids = TGRM['channel_ids']

client = TelegramClient('anon', api_id, api_hash)

replace_non_ascii = lambda s: str(''.join(' ' if ord(i) >= 128 or i == '\n' or i == "₹" else i.upper() for i in s))

replace_unicode = lambda s: unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('utf-8')
# replace_non_ascii = lambda s: str(''.join(' ' if str(i).isalnum() or str(i).strip().isspace() or i == '\n' or i == "₹" else i.upper() for i in s))

@client.on(events.NewMessage(chats=channel_ids))
async def my_event_handler(event):
    chat = await event.get_chat()
    with open(DATA + TGRM["output_file"], 'a', encoding='utf-8',  newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        now = int(datetime.now().timestamp())
        try:
            logger.info(f"{chat.id=} and {chat.title=}")
        except:
            logger.error(traceback.format_exc())
        if chat.id == 493143987:
            chat_title = "USER-SCHOUDHRY12"
        elif chat.id == -1001612965918 or chat.id == 1612965918 :
            chat_title = "PREMIUM MEMBERSHIP GROUP"
        elif chat.id == -1002264094175 or chat.id == 2264094175:
            chat_title = "PREMIUM GROUP"
        elif chat.id == -1001953542337 or chat.id == 1953542337:
            chat_title = "STUDENTS GROUP"
        elif chat.id == -1001392872175 or chat.id == 1392872175:
            chat_title = "PLATINUM MEMBERS"
        elif chat.id == -1001519126374 or chat.id == 1519126374:
            chat_title = "SMS OPTIONS PREMIUM"
        elif chat.id == -1001521119016 or chat.id == 1521119016:
            chat_title = "PAID - CALL & PUT"
        elif chat.id == -1001706634236 or chat.id == 1706634236:
            chat_title = "BNO PREMIUM"
        elif chat.id == -1001697722741 or chat.id == 1697722741:
            chat_title = "STOCK PREMIUM"
        elif chat.id == -1002358213778 or chat.id == 2358213778:
            chat_title = "SMS Stock Options Premium"
        elif chat.id == -1001972442018 or chat.id == 1972442018:
            chat_title = "VIP PREMIUM PAID CALLS"
        elif chat.id == -1001644402199 or chat.id == 1644402199:
            chat_title = "PAID STOCK & INDEX OPTION"     
        elif chat.id == -1002689628842 or chat.id == 2689628842: 
            chat_title = "PREMIUMFXG"   
        elif chat.id == 1116480951 or chat.id == -1116480951:
            chat_title = "BANKNIFTYRANI"
        else:
            chat_title = chat.id
        logger.info(f"After processing ->{chat.id=} and {chat_title=}")
        # try:
        #     chat_title = replace_non_ascii(chat.title).strip().upper()
        #     if not chat_title:
        #         chat_title = replace_non_ascii(replace_unicode(chat.title)).strip().upper()
        # except:
        #     if chat.id == 493143987:
        #         chat_title = "USER-SCHOUDHRY12"
        #     else:
        #         chat_title = chat.id
        
        if event.reply_to_msg_id is not None:
            original_message = await client.get_messages(chat.id, ids=event.reply_to_msg_id)
            if chat_title == "PREMIUM MEMBERSHIP GROUP":
                msg = replace_non_ascii(replace_unicode(original_message.raw_text)) +"$$$$"+ replace_non_ascii(replace_unicode(event.raw_text))
            else:
                msg = replace_non_ascii(original_message.raw_text) +"$$$$"+ replace_non_ascii(event.raw_text)
            if msg.strip():
                csv_writer.writerow([now, chat_title, msg,])
        else:
            if chat_title == "PREMIUM MEMBERSHIP GROUP":
                msg = replace_non_ascii(replace_unicode(event.raw_text))
            else:
                msg = replace_non_ascii(event.raw_text)
            if msg.strip():
                csv_writer.writerow([now, chat_title, msg])
        
        msg = msg.strip().removeprefix('#').strip()
        try:
            if msg.strip():
                if chat_title == "PREMIUM GROUP":
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
                    # logger.info(f"Premium Memebership group - {msg} - vs - {event.raw_text}")
                    i = PremiumMembershipGroup(now, msg)
                    i.get_signal()
                # elif chat_title == "LOSS RECOVERY GROUP":
                #     i = AllIn1Group(now, msg)
                #     i.get_signal()
                elif chat_title == "STUDENTS GROUP":
                    i = StudentsGroup(now, msg)
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
                elif chat_title == "SMS Stock Options Premium":
                    # logger.info(f"{chat_title} ===> {msg}")
                    i = SmsStockOptionsPremium(now, msg)
                    i.get_signal()
                elif chat_title == "PREMIUMFXG":
                    i = PremiumFXG(now, msg)
                    i.get_signal()
                elif chat_title == "BANKNIFTYRANI":
                    i = BankNiftyRani(now, msg)
                    i.get_signal()
                else:
                    logger.info(f"{chat_title} ===> {msg}")
        except:
            logger.error(traceback.format_exc())

client.start(phone=TGRM["phone_number"])
client.run_until_disconnected()
