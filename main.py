from pyrogram import Client, errors
import asyncio
import os
from pymongo import MongoClient

# Environment variables for sensitive data
API_ID = int(os.getenv('API_ID', 4796990))              
API_HASH = os.getenv('API_HASH', '32b6f41a4bf740efed2d4ce911f145c7')
SESSION_STRING = os.getenv('SESSION_STRING', "BAAtp4AAQTkp622SwxukmACtcaPzZ_3TG8DyDojVIFuaI98uDI1KAWF2ul8mSqWVwW8Y5y96p1IMpx3yUmWnLesJMQ3-6kxIvBrq85CsYQqkB0oddt1A0HgNRK82KQOeczTcSfOmEtpuLCzZnTgqztvHWSkU7H3yHGXsZkELLLiCbma3YMCAMywvHilr0Wl05JZxxG8LsS7eJsA7qW6UP9oCDDPowA0NP4HKiSzAqLxqg61yDCQiOaRCX0VboM4_5l_ASUPImicn8fH45J4HQC94BqQV7pd9e8QxmPTorgHYFjuSn1uRsCBGVCqckaI7KPwDkkMvLjUfOrw01X0ejIhTQ-u_iQAAAAF01KpnAA")

# MongoDB configuration
MONGO_URI = os.getenv('MONGO_URI', 'mongodb+srv://abcd:abcd@personalproject.mxx6dgi.mongodb.net/?retryWrites=true&w=majority')
DB_NAME = 'forward_bot_db'
COLLECTION_NAME = 'message_status'

# Channel IDs
SOURCE_CHANNEL_ID = -1002079489506  # Use negative sign for channel IDs
DESTINATION_CHANNEL_ID = -1002084341815

# Start and End Message IDs to forward
START_MESSAGE_ID = 1504
END_MESSAGE_ID = 500000

# Initialize the MongoDB client
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
collection = db[COLLECTION_NAME]

# Initialize the Pyrogram Client
app = Client("forward_bot", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING)

bot = Client("my_account", bot_token="6285135839:AAE5savazJeNxwkAnGW3mW9l-4hUPLLoUds", api_id="25033101", api_hash="d983e07db3fe330a1fd134e61604e11d")

async def forward_specific_message(message_id):
    try:
        # Fetch the message from the source channel
        message = await app.get_messages(SOURCE_CHANNEL_ID, message_id)
        # Forward the message to the destination channel
        await app.copy_message(chat_id=DESTINATION_CHANNEL_ID, from_chat_id=SOURCE_CHANNEL_ID, message_id=message_id)
        print(f"Successfully forwarded message {message_id} to {DESTINATION_CHANNEL_ID}")
        return True
    except errors.FloodWait as e:
        await bot.send_message(chat_id=1881720028, text=f"<b>😥 Pʟᴇᴀsᴇ Wᴀɪᴛ ᴅᴏɴ'ᴛ ᴅᴏ ғʟᴏᴏᴅɪɴɢ ᴡᴀɪᴛ ғᴏʀ {e.value} Sᴇᴄᴄᴏɴᴅs</b>")
        print(f"Flood wait error: waiting for {e.value} seconds")
        await asyncio.sleep(e.value)
        await bot.send_message(chat_id=1881720028, text=f"<b>Now Every Thing Ok</b>")
        return await forward_specific_message(message_id)  # Retry after the wait
    except Exception as e:
        print(f"Failed to forward message {message_id}: {e}")
        return False

async def main():
    await app.start()
    await bot.start()
    try:
        # Fetch the last processed message ID from MongoDB
        status = collection.find_one({'_id': 1})
        last_processed_id = status['last_processed_id'] if status else START_MESSAGE_ID - 1

        for message_id in range(last_processed_id + 1, END_MESSAGE_ID + 1):
            success = await forward_specific_message(message_id)
            if success:
                # Update the last processed message ID in MongoDB
                collection.update_one({'_id': 1}, {'$set': {'last_processed_id': message_id}}, upsert=True)
            else:
                print(f"Stopping the forwarding process due to failure at message {message_id}")
                break
    finally:
        await app.stop()
        await bot.stop()

if __name__ == '__main__':
    asyncio.run(main())
