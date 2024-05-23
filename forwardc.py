import logging
import requests
import asyncio
import os
from dotenv import load_dotenv
from pyrogram import Client, errors
from pymongo import MongoClient
from pyrogram.errors import BadRequest

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)

# Environment variables for sensitive data
MESSAGE_BOT_TOKEN = os.getenv('MESSAGE_BOT_TOKEN')
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
SESSION_STRING = os.getenv('SESSION_STRING')
BOT_API_ID = os.getenv('BOT_API_ID')
BOT_API_HASH = os.getenv('BOT_API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')

# MongoDB configuration
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = 'forward_bot_db'
COLLECTION_NAME = 'message_status'
PROGRESS_COLLECTION_NAME = 'progress_messages'

# Channel IDs
SOURCE_CHANNEL_ID = int(os.getenv('SOURCE_CHANNEL_ID'))
DESTINATION_CHANNEL_ID = int(os.getenv('DESTINATION_CHANNEL_ID'))

# Start and End Message IDs to forward
START_MESSAGE_ID = int(os.getenv('START_MESSAGE_ID', 1))
END_MESSAGE_ID = int(os.getenv('END_MESSAGE_ID', 500000))
STATUS_ID = int(os.getenv('STATUS_ID'))
PROGRESS_IDS = list(map(int, os.getenv('PROGRESS_IDS').split(',')))  # List of chat IDs where progress updates will be sent

# Initialize the MongoDB client
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
collection = db[COLLECTION_NAME]
progress_collection = db[PROGRESS_COLLECTION_NAME]

# Initialize the Pyrogram Client
app = Client("forward_bot", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING)
bot = Client("my_account", bot_token=BOT_TOKEN, api_id=BOT_API_ID, api_hash=BOT_API_HASH)

async def forward_specific_message(message_id, total_files):
    try:
        message = await app.get_messages(SOURCE_CHANNEL_ID, message_id)
        await app.copy_message(chat_id=DESTINATION_CHANNEL_ID, from_chat_id=SOURCE_CHANNEL_ID, message_id=message_id)
        logging.info(f"Successfully forwarded message {message_id} to {DESTINATION_CHANNEL_ID}")

        if message_id % 10 == 0:
            await send_progress_update(message_id, total_files)
        return True
    except errors.FloodWait as e:
        logging.warning(f"Flood wait error: waiting for {e.value} seconds")
        await asyncio.sleep(e.value)
        return await forward_specific_message(message_id, total_files)
    except Exception as e:
        logging.error(f"Failed to forward message {message_id}: {e}")
        return False

async def send_progress_update(current_file, total_files):
    progress = (current_file / total_files) * 100
    remaining_files = total_files - current_file
    time_per_file = 2  # Adjust based on actual performance
    eta_seconds = remaining_files * time_per_file

    eta_days, eta_hours, eta_minutes, eta_seconds = calculate_eta(eta_seconds)

    progress_message = (
        f"[{'⬢' * int(progress * 20 // 100)}{'⬡' * (20 - int(progress * 20 // 100))}]\n"
        f"╭━━━━❰Progress Bar❱━➣\n"
        f"┣⪼ 🗃️ Files uploaded: {current_file} | {total_files}\n"
        f"┣⪼ 📁 Remaining files: {remaining_files}\n"
        f"┣⪼ ⏳️ Done : {progress:.2f}%\n"
        f"┣⪼ ⏰️ ETA: {int(eta_days)} days, {int(eta_hours)} hours, {int(eta_minutes)} minutes, {int(eta_seconds)} seconds\n"
        f"╰━━━━━━━━━━━━━━━➣"
    )

    for progress_id in PROGRESS_IDS:
        try:
            await update_progress_message(progress_id, progress_message)
        except Exception as e:
            logging.error(f"Error updating progress message for {progress_id}: {e}")

def calculate_eta(eta_seconds):
    eta_days = eta_seconds // 86400
    eta_seconds %= 86400
    eta_hours = eta_seconds // 3600
    eta_seconds %= 3600
    eta_minutes = eta_seconds // 60
    eta_seconds %= 60
    return eta_days, eta_hours, eta_minutes, eta_seconds

async def update_progress_message(progress_id, progress_message):
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

async def get_latest_message_id(bot_token, source_channel_id):
    try:
        response = requests.get(f"https://api.telegram.org/bot{bot_token}/getUpdates").json()
        for result in response.get('result', []):
            if 'channel_post' in result and result['channel_post']['chat']['id'] == int(source_channel_id):
                return result['channel_post']['message_id']
        return END_MESSAGE_ID
    except requests.RequestException as e:
        logging.error(f"Failed to fetch latest message ID: {e}")
        return END_MESSAGE_ID

async def update_end_message_id():
    global END_MESSAGE_ID
    while True:
        END_MESSAGE_ID = await get_latest_message_id(MESSAGE_BOT_TOKEN, SOURCE_CHANNEL_ID)
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

        global END_MESSAGE_ID
        END_MESSAGE_ID = await get_latest_message_id(MESSAGE_BOT_TOKEN, SOURCE_CHANNEL_ID)
        collection.update_one({'_id': 1}, {'$set': {'end_message_id': END_MESSAGE_ID}}, upsert=True)

        for message_id in range(last_processed_id + 1, END_MESSAGE_ID + 1):
            success = await forward_specific_message(message_id, total_files=END_MESSAGE_ID)
            if success:
                collection.update_one({'_id': 1}, {'$set': {'last_processed_id': message_id}}, upsert=True)
                await asyncio.sleep(2)
            else:
                for progress_id in PROGRESS_IDS:
                    await bot.send_message(chat_id=progress_id, text=f"Skipping message {message_id} due to failure")
                continue
    finally:
        await app.stop()
        await bot.stop()

if __name__ == '__main__':
    asyncio.run(main())
