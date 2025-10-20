import asyncio
import math
import secrets
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError

from dotenv import load_dotenv
import os

# Load environment variables from config.env
load_dotenv("config.env")
# Access variables
multi_clients = int(os.getenv("MULTI_CLIENTS", 1))
work_loads = int(os.getenv("WORK_LOADS", 1))
print(f"Multi Clients: {multi_clients}, Work Loads: {work_loads}")

CHUNK_SIZE_SMALL = 512 * 1024      # 512 KB for large files
CHUNK_SIZE_LARGE = 1024 * 1024     # 1 MB for small files
TIMEOUT = 25                        # per chunk timeout in seconds
MAX_RETRIES = 5                      # retries per chunk
SAVE_DIR = "/tmp"                    # RAM-backed directory for Koyeb
MAX_CONCURRENT_DOWNLOADS = 2         # limit concurrent downloads globally
# =========================================

DOWNLOAD_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

# ======= Workload tracking for multi-client =======
from contextlib import asynccontextmanager

@asynccontextmanager
async def track_workload(client_index):
    work_loads[client_index] += 1
    try:
        yield
    finally:
        work_loads[client_index] -= 1

def get_optimal_client():
    """Select the client with least active downloads."""
    idx = min(work_loads, key=work_loads.get)
    return idx, multi_clients[idx]

# ======= Safe Cross-DC Chunked Download Function =======
async def safe_download(client: Client, message):
    """
    Download Telegram file safely with adaptive chunk size.
    Works for multi-DC files and optimized for Koyeb.
    """
    # Determine file name
    file_name = "file"
    file_size = 0

    if hasattr(message, "document") and message.document.file_name:
        file_name = message.document.file_name
        file_size = message.document.file_size
    elif hasattr(message, "video") and message.video.file_name:
        file_name = message.video.file_name
        file_size = message.video.file_size
    elif hasattr(message, "audio") and message.audio.file_name:
        file_name = message.audio.file_name
        file_size = message.audio.file_size

    # Sanitize filename for safety
    file_name = "".join(c for c in file_name if c.isalnum() or c in (" ", ".", "_", "-"))
    file_path = os.path.join(SAVE_DIR, file_name)
    os.makedirs(SAVE_DIR, exist_ok=True)

    # Adaptive chunk size
    chunk_size = CHUNK_SIZE_SMALL if file_size > 200 * 1024 * 1024 else CHUNK_SIZE_LARGE
    offset = 0
    retries = 0

    print(f"🚀 Starting download to {file_path} with chunk size {chunk_size // 1024} KB")

    async with DOWNLOAD_SEMAPHORE:
        with open(file_path, "wb") as f:
            while True:
                try:
                    data = await asyncio.wait_for(
                        client.download_media(
                            message,
                            in_memory=True,
                            offset=offset,
                            limit=chunk_size
                        ),
                        timeout=TIMEOUT
                    )

                    if not data:
                        print(f"✅ Download completed successfully: {file_path}")
                        break

                    f.write(data)
                    offset += len(data)
                    retries = 0  # reset retries after success

                except asyncio.TimeoutError:
                    retries += 1
                    print(f"⚠️ Timeout at offset {offset}, retry {retries}/{MAX_RETRIES}")
                    if retries >= MAX_RETRIES:
                        print("❌ Aborting download after too many timeouts")
                        break
                    await asyncio.sleep(2)

                except FloodWait as e:
                    print(f"⏳ FloodWait: sleeping for {e.value}s")
                    await asyncio.sleep(e.value + 1)

                except RPCError as e:
                    retries += 1
                    print(f"❌ Telegram RPCError ({type(e).__name__}): {e}. Retry {retries}/{MAX_RETRIES}")
                    if retries >= MAX_RETRIES:
                        print("❌ Aborting after repeated RPC errors")
                        break
                    await asyncio.sleep(3)

                except Exception as e:
                    retries += 1
                    print(f"⚠️ Unknown error: {e}. Retry {retries}/{MAX_RETRIES}")
                    if retries >= MAX_RETRIES:
                        print("❌ Stopping after unknown repeated errors")
                        break
                    await asyncio.sleep(3)

    print("📦 Download task ended\n")
    return file_path

# ======= Multi-client download wrapper =======
async def download_file(message):
    """
    Automatically selects optimal client and downloads file.
    """
    client_index, client = get_optimal_client()
    async with track_workload(client_index):
        return await safe_download(client, message)

# ======= Connection Watchdog for all clients =======
async def connection_watchdog(client, name):
    """Keep a client session alive on Koyeb."""
    while True:
        await asyncio.sleep(300)
        try:
            await client.get_me()
        except Exception:
            print(f"🔁 Connection lost for {name} — restarting client")
            await client.stop()
            await asyncio.sleep(5)
            await client.start()

# ======= Example Usage =======
async def main():
    # Start all clients
    for i, client in enumerate(multi_clients):
        await client.start()
        print(f"✅ Client {i} started: {client.session_name}")

        # Start watchdog per client
        asyncio.create_task(connection_watchdog(client, client.session_name))

    print("⚡ ThunderBot Multi-Client Safe Downloader started")
    print("✅ Ready to download files from any DC safely!")

    # Example usage:
    # Replace 'message' with your Pyrogram message object
    # downloaded_file = await download_file(message)
    # print(f"Downloaded file path: {downloaded_file}")

    await asyncio.get_event_loop().create_future()  # keep bot running

if __name__ == "__main__":
    asyncio.run(main())
