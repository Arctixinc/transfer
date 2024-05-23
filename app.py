import logging
import asyncio
import os
from pyrogram import Client, errors
from pymongo import MongoClient
from pyrogram.errors import BadRequest

# Configure logging
logging.basicConfig(level=logging.INFO)

# Environment variables for sensitive data
message_bot_token = "7077404988:AAHWWM5WuJuoNd9SfrXROtMZJdlekbLh5Tg"
API_ID = int(os.getenv('API_ID', '4796990'))             
API_HASH = os.getenv('API_HASH', '32b6f41a4bf740efed2d4ce911f145c7')
SESSION_STRING = os.getenv('SESSION_STRING', "BABJMj4AGCeKymng4c-AKeDUi-fimjX1D0QPAlh98yjnbtCU5y_VPM7-tjMSKoKWsQ9PZhAYDtnSgld9XHoVyq0w_eMuL80JcEzpJCXLTRe3yrRf-ibsD_Pb4Mbs6D7ubWVlx1Zw5z0q2SmLUrMRz9BtqzCaL8pXsoySRtL87k1NbK8u9UWpQG45ECIu6qd49dx8Q_uIdnJIUkFQrqnDRtioVmPZDSGH-gF7US85Rqk9wDeRkYXwqKzjfmLScDmiSDh2eUmrvvQDRLYI1r5dchKhRroc4hg5YRtRQjfMBn0DWaWwhwXYZnZ31L-kWl59miQG4jNPfB2CbQAV-3WkPtqj71nEpAAAAAGg0cHLAQ")

# MongoDB configuration
MONGO_URI = os.getenv('MONGO_URI', 'mongodb+srv://abcd:abcd@personalproject.mxx6dgi.mongodb.net/?retryWrites=true&w=majority')
DB_NAME = 'forward_bot_db'
COLLECTION_NAME = 'message_status'
PROGRESS_COLLECTION_NAME = 'progress_messages'

# Channel IDs
SOURCE_CHANNEL_ID = -1002079489506 
DESTINATION_CHANNEL_ID = -1002084341815

# Start and End Message IDs to forward
START_MESSAGE_ID = 1504
END_MESSAGE_ID = 500000
STATUS_ID = 1881720028
PROGRESS_ID = [1881720028, 5301275567, -1002084341815]  # List of chat IDs where progress updates will be sent

# Initialize the MongoDB client
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
collection = db[COLLECTION_NAME]
progress_collection = db[PROGRESS_COLLECTION_NAME]

# Initialize the Pyrogram Client
app = Client("forward_bot", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING)
bot = Client("my_account", bot_token="6285135839:AAE5savazJeNxwkAnGW3mW9l-4hUPLLoUds", api_id="25033101", api_hash="d983e07db3fe330a1fd134e61604e11d")

async def forward_specific_message(message_id, total_files):
    try:
        # Fetch the message from the source channel
        message = await app.get_messages(SOURCE_CHANNEL_ID, message_id)
        # Forward the message to the destination channel
        await app.copy_message(chat_id=DESTINATION_CHANNEL_ID, from_chat_id=SOURCE_CHANNEL_ID, message_id=message_id)
        logging.info(f"Successfully forwarded message {message_id} to {DESTINATION_CHANNEL_ID}")

        # Calculate progress and send update every 10 messages
        if message_id % 10 == 0:  # Adjust this value as needed
            await send_progress_update(message_id, total_files)

        return True
    except errors.FloodWait as e:
        msg = await bot.send_message(chat_id=STATUS_ID, text=f"<b>😥 Please wait {e.value} seconds due to flood wait.</b>")
        logging.warning(f"Flood wait error: waiting for {e.value} seconds")
        await asyncio.sleep(e.value)
        await msg.edit("<b>Everything is okay now.</b>")
        return await forward_specific_message(message_id, total_files)  # Retry after the wait
    except Exception as e:
        logging.error(f"Failed to forward message {message_id}: {e}")
        return False

async def send_progress_update(current_file, total_files):
    progress = (current_file / total_files) * 100
    remaining_files = total_files - current_file
    time_per_file = 2  # Adjust this value based on actual performance
    eta_seconds = remaining_files * time_per_file

    eta_days = eta_seconds // 86400
    eta_seconds %= 86400
    eta_hours = eta_seconds // 3600
    eta_seconds %= 3600
    eta_minutes = eta_seconds // 60
    eta_seconds %= 60

    progress_message = (
        f"[{'⬢' * int(progress * 20 // 100)}{'⬡' * (20 - int(progress * 20 // 100))}]\n"
        f"╭━━━━❰Progress Bar❱━➣\n"
        f"┣⪼ 🗃️ Files uploaded: {current_file} | {total_files}\n"
        f"┣⪼ 📁 Remaining files: {remaining_files}\n"
        f"┣⪼ ⏳️ Done : {progress:.2f}%\n"
        f"┣⪼ ⏰️ ETA: {int(eta_days)} days, {int(eta_hours)} hours, {int(eta_minutes)} minutes, {int(eta_seconds)} seconds\n"
        f"╰━━━━━━━━━━━━━━━➣"
    )

    try:
        for progress_id in PROGRESS_ID:
            try:
                progress_doc = progress_collection.find_one({'progress_id': progress_id})
                if progress_doc:
                    progress_message_id = progress_doc['message_id']
                    await bot.edit_message_text(chat_id=progress_id, message_id=progress_message_id, text=progress_message)
                else:
                    sent_message = await bot.send_message(chat_id=progress_id, text=progress_message)
                    progress_collection.update_one({'progress_id': progress_id}, {'$set': {'message_id': sent_message.id}}, upsert=True)
            except errors.MessageNotModified:
                pass
            except errors.MessageIdInvalid:
                sent_message = await bot.send_message(chat_id=progress_id, text=progress_message)
                progress_collection.update_one({'progress_id': progress_id}, {'$set': {'message_id': sent_message.id}}, upsert=True)
    except Exception as e:
        logging.error(f"Error updating progress message: {e}")

async def get_latest_message_id(bot_token, source_channel_id):
    try:
        response = requests.get(f"https://api.telegram.org/bot{bot_token}/getUpdates").json()
        for result in response.get('result', []):
            if 'channel_post' in result:
                channel_post = result['channel_post']
                if channel_post['chat']['id'] == int(source_channel_id):
                    return channel_post['message_id']
        return END_MESSAGE_ID
    except requests.RequestException as e:
        logging.error(f"Failed to fetch latest message ID: {e}")
        return END_MESSAGE_ID

async def update_end_message_id():
    global END_MESSAGE_ID
    while True:
        end_message_id = await get_latest_message_id(message_bot_token, SOURCE_CHANNEL_ID)
        END_MESSAGE_ID = end_message_id
        collection.update_one({'_id': 1}, {'$set': {'end_message_id': END_MESSAGE_ID}}, upsert=True)
        await asyncio.sleep(60)

async def main():
    logging.info("Starting the user client...")
    await app.start()
    logging.info("User client started successfully.")
    logging.info("Starting the bot client...")
    await bot.start()
    logging.info("Bot client started successfully.")

    try:
        asyncio.create_task(update_end_message_id())
        
        status = collection.find_one({'_id': 1})
        last_processed_id = status['last_processed_id'] if status else START_MESSAGE_ID - 1

        end_message_id = get_latest_message_id(message_bot_token, SOURCE_CHANNEL_ID)
        global END_MESSAGE_ID
        END_MESSAGE_ID = end_message_id
        collection.update_one({'_id': 1}, {'$set': {'end_message_id': END_MESSAGE_ID}}, upsert=True)

        for message_id in range(last_processed_id + 1, END_MESSAGE_ID + 1):
            success = await forward_specific_message(message_id, total_files=END_MESSAGE_ID)
            if success:
                collection.update_one({'_id': 1}, {'$set': {'last_processed_id': message_id}}, upsert=True)
                await asyncio.sleep(2)
            else:
                for progress_id in PROGRESS_ID:
                    await bot.send_message(chat_id=progress_id, text=f"Skipping message {message_id} due to failure")
                continue
    finally:
        await app.stop()
        await bot.stop()

if __name__ == '__main__':
    asyncio.run(main())
