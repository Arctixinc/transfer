import logging
import asyncio
import os
from datetime import datetime
from pytz import timezone
from pyrogram import Client, errors
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Timezone configuration
TIMEZONE = 'Asia/Kolkata'

# Function to set timezone for logging
def changetz(*args):
    return datetime.now(timezone(TIMEZONE)).timetuple()

# Set timezone for logging
logging.Formatter.converter = changetz

# Configure logging
user_logger = logging.getLogger("user_client")
bot_logger = logging.getLogger("bot_client")

# Set logging level
user_logger.setLevel(logging.INFO)
bot_logger.setLevel(logging.INFO)

# Create console handlers
user_handler = logging.StreamHandler()
bot_handler = logging.StreamHandler()

# Create formatters with 12-hour time format and add them to the handlers
user_formatter = logging.Formatter('%(asctime)s - USER - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
bot_formatter = logging.Formatter('%(asctime)s - BOT - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
user_handler.setFormatter(user_formatter)
bot_handler.setFormatter(bot_formatter)

# Add handlers to the loggers
user_logger.addHandler(user_handler)
bot_logger.addHandler(bot_handler)

# Environment variables for sensitive data
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
SESSION_STRING = os.getenv('SESSION_STRING')
BOT_API_ID = int(os.getenv('BOT_API_ID'))
BOT_API_HASH = os.getenv('BOT_API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = 'forward_bot_db'
COLLECTION_NAME = 'message_forwarding_status'
SOURCE_CHANNEL_ID = int(os.getenv('SOURCE_CHANNEL_ID'))
DESTINATION_CHANNEL_ID = int(os.getenv('DESTINATION_CHANNEL_ID'))
START_MESSAGE_ID = int(os.getenv('START_MESSAGE_ID', 1))
END_MESSAGE_ID = int(os.getenv('END_MESSAGE_ID', 500000))
STATUS_ID = int(os.getenv('STATUS_ID'))
PROGRESS_IDS = list(map(int, os.getenv('PROGRESS_IDS').split(',')))

# Initialize the MongoDB client
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
collection = db[COLLECTION_NAME]

# Define the new _id value
GLOBAL_DATA_ID = int(os.getenv('GLOBAL_DATA_ID'))

# Initialize the Pyrogram Client
app = Client("forward_bot", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING)
bot = Client("my_account", bot_token=BOT_TOKEN, api_id=BOT_API_ID, api_hash=BOT_API_HASH)

async def forward_specific_message(message_id, total_files):
    try:
        message = await app.get_messages(SOURCE_CHANNEL_ID, message_id)
        await app.copy_message(chat_id=DESTINATION_CHANNEL_ID, from_chat_id=SOURCE_CHANNEL_ID, message_id=message_id)
        user_logger.info(f"Successfully forwarded message {message_id} to {DESTINATION_CHANNEL_ID}")

        if message_id % 10 == 0:
            await send_progress_update(message_id, total_files)
        return True
    except errors.FloodWait as e:
        user_logger.warning(f"Flood wait error: waiting for {e.value} seconds")
        await asyncio.sleep(e.value)
        return await forward_specific_message(message_id, total_files)
    except errors.RPCError as e:
        if e.CODE == 500 and "INTERDC_X_CALL_RICH_ERROR" in str(e):
            user_logger.warning(f"INTERDC_X_CALL_RICH_ERROR: Retrying message {message_id} after 5 seconds")
            await asyncio.sleep(5)
            return await forward_specific_message(message_id, total_files)
        user_logger.error(f"Failed to forward message {message_id}: {e}")
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
            bot_logger.error(f"Error updating progress message for {progress_id}: {e}")

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
        progress_message_id = get_progress_message_id(progress_id)
        if progress_message_id:
            await bot.edit_message_text(chat_id=progress_id, message_id=progress_message_id, text=progress_message)
        else:
            sent_message = await bot.send_message(chat_id=progress_id, text=progress_message)
            update_progress_messages(progress_id, sent_message.id)
    except errors.MessageNotModified:
        pass
    except errors.MessageIdInvalid:
        sent_message = await bot.send_message(chat_id=progress_id, text=progress_message)
        update_progress_messages(progress_id, sent_message.id)

def update_progress_messages(progress_id, message_id):
    if collection.find_one({'_id': GLOBAL_DATA_ID, 'progress_messages.progress_id': progress_id}):
        collection.update_one(
            {'_id': GLOBAL_DATA_ID, 'progress_messages.progress_id': progress_id},
            {'$set': {'progress_messages.$.message_id': message_id}},
            upsert=True
        )
    else:
        collection.update_one(
            {'_id': GLOBAL_DATA_ID},
            {'$push': {'progress_messages': {'progress_id': progress_id, 'message_id': message_id}}},
            upsert=True
        )

def get_progress_message_id(progress_id):
    status = collection.find_one({'_id': GLOBAL_DATA_ID})
    if status and 'progress_messages' in status:
        for progress_message in status['progress_messages']:
            if progress_message['progress_id'] == progress_id:
                return progress_message['message_id']
    return None

async def main():
    user_logger.info("Starting the user client...")
    await app.start()
    user_logger.info("User client started successfully.")
    bot_logger.info("Starting the bot client...")
    await bot.start()
    bot_logger.info("Bot client started successfully.")

    try:
        status = collection.find_one({'_id': GLOBAL_DATA_ID})
        last_processed_id = status['last_processed_id'] if status else START_MESSAGE_ID - 1
        end_message_id = status['end_message_id'] if status and 'end_message_id' in status else END_MESSAGE_ID

        collection.update_one({'_id': GLOBAL_DATA_ID}, {'$set': {'end_message_id': end_message_id}}, upsert=True)

        for message_id in range(last_processed_id + 1, end_message_id + 1):
            success = await forward_specific_message(message_id, total_files=end_message_id)
            if success:
                collection.update_one({'_id': GLOBAL_DATA_ID}, {'$set': {'last_processed_id': message_id}}, upsert=True)
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
