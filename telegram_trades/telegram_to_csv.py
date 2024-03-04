from telethon.sync import TelegramClient, events
import csv
from datetime import datetime
from constants import TGRM
from telegram_message_parser import PremiumJackpot, SmsOptionsPremium, PaidCallPut, PaidStockIndexOption
from logzero import logger
import traceback

api_id = TGRM['api_id']
api_hash = TGRM['api_hash']
channel_ids = TGRM['channel_ids']

client = TelegramClient('anon', api_id, api_hash)

replace_non_ascii = lambda s: ''.join(' ' if ord(i) >= 128 or i == '\n' else i for i in s)

@client.on(events.NewMessage(chats=channel_ids))
async def my_event_handler(event):
    chat = await event.get_chat()
    with open(TGRM["output_file"], 'a', encoding='utf-8',  newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        now = int(datetime.now().timestamp())
        if event.reply_to_msg_id is not None:
            original_message = await client.get_messages(chat.id, ids=event.reply_to_msg_id)
            msg = replace_non_ascii(original_message.raw_text) +" "+ replace_non_ascii(event.raw_text)
            csv_writer.writerow([now, chat.title, msg,])
        else:
            msg = replace_non_ascii(event.raw_text)
            csv_writer.writerow([now, chat.title, msg])
        logger.info(f"{chat.title} ===> {msg}")
        try:
            if chat.title == "PREMIUM JACKPOT":
                i = PremiumJackpot(now, msg)
                i.get_signal()
            elif chat.title == "SMS Options Premium":
                i = SmsOptionsPremium(now, msg)
                i.get_signal()
            elif chat.title == "Paid - CALL & PUT":
                i = PaidCallPut(now, msg)
                i.get_signal()
            elif chat.title == "Paid Stock & Index Option":
                i = PaidStockIndexOption(now, msg)
                i.get_signal()
        except:
            logger.error(traceback.format_exc())

client.start(phone=TGRM["phone_number"])
client.run_until_disconnected()
