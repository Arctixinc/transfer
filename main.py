from pyrogram import Client, errors
import asyncio
import os

# Environment variables for sensitive data
API_ID = int(os.getenv('API_ID', 4796990))              
API_HASH = os.getenv('API_HASH', '32b6f41a4bf740efed2d4ce911f145c7')            
SESSION_STRING = os.getenv('SESSION_STRING', "BAAtp4AAQTkp622SwxukmACtcaPzZ_3TG8DyDojVIFuaI98uDI1KAWF2ul8mSqWVwW8Y5y96p1IMpx3yUmWnLesJMQ3-6kxIvBrq85CsYQqkB0oddt1A0HgNRK82KQOeczTcSfOmEtpuLCzZnTgqztvHWSkU7H3yHGXsZkELLLiCbma3YMCAMywvHilr0Wl05JZxxG8LsS7eJsA7qW6UP9oCDDPowA0NP4HKiSzAqLxqg61yDCQiOaRCX0VboM4_5l_ASUPImicn8fH45J4HQC94BqQV7pd9e8QxmPTorgHYFjuSn1uRsCBGVCqckaI7KPwDkkMvLjUfOrw01X0ejIhTQ-u_iQAAAAF01KpnAA")

# Channel IDs
SOURCE_CHANNEL_ID = -1002079489506  # Use negative sign for channel IDs
DESTINATION_CHANNEL_ID = -1002084341815

# Start and End Message IDs to forward
START_MESSAGE_ID = 313
END_MESSAGE_ID = 498858  # Example end ID, adjust as needed

# Initialize the Client
app = Client("forward_bot", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING)

async def forward_specific_message(message_id):
    try:
        # Fetch the message from the source channel
        message = await app.get_messages(SOURCE_CHANNEL_ID, message_id)
        # Forward the message to the destination channel
        await app.copy_message(chat_id=DESTINATION_CHANNEL_ID, from_chat_id=SOURCE_CHANNEL_ID, message_id=message_id)
        print(f"Successfully forwarded message {message_id} to {DESTINATION_CHANNEL_ID}")
        return True
    except errors.FloodWait as e:
        print(f"Flood wait error: waiting for {e.value} seconds")
        await asyncio.sleep(e.value)
        return await forward_specific_message(message_id)  # Retry after the wait
    except Exception as e:
        print(f"Failed to forward message {message_id}: {e}")
        return False

async def main():
    await app.start()
    try:
        for message_id in range(START_MESSAGE_ID, END_MESSAGE_ID + 1):
            success = await forward_specific_message(message_id)
            if not success:
                print(f"Stopping the forwarding process due to failure at message {message_id}")
                break
    finally:
        await app.stop()

if __name__ == '__main__':
    asyncio.run(main())
