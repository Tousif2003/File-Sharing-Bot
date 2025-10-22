import asyncio
import os
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError
from dotenv import load_dotenv
from contextlib import asynccontextmanager

# ==================== Load Environment ====================
load_dotenv("config.env")

CHUNK_SIZE_SMALL = 512 * 1024      # 512 KB
CHUNK_SIZE_LARGE = 1024 * 1024     # 1 MB
TIMEOUT = 30                        # per chunk timeout (seconds)
MAX_RETRIES = 6                      # retries per chunk
SAVE_DIR = "/tmp"                    # storage path
MAX_CONCURRENT_DOWNLOADS = 6         # global concurrency limit

DOWNLOAD_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

# ==================== Multi-client Initialization ====================
multi_clients = []

# ----- API_ID/API_HASH clients -----
i = 1
while True:
    api_id = os.getenv(f"API_ID_{i}")
    api_hash = os.getenv(f"API_HASH_{i}")
    if not api_id or not api_hash:
        break
    client = Client(f"session_api_{i}", api_id=int(api_id), api_hash=api_hash)
    multi_clients.append(client)
    i += 1

# ----- Bot token clients -----
i = 1
while True:
    token = os.getenv(f"MULTI_TOKEN{i}")
    if not token:
        break
    client = Client(f"session_bot_{i}", bot_token=token)
    multi_clients.append(client)
    i += 1

if not multi_clients:
    raise Exception("❌ No clients found! Add API_ID/API_HASH or MULTI_TOKENs in config.env")

work_loads = {idx: 0 for idx in range(len(multi_clients))}
print(f"✅ Loaded {len(multi_clients)} clients")

# ==================== Workload Tracking ====================
@asynccontextmanager
async def track_workload(client_index):
    work_loads[client_index] += 1
    try:
        yield
    finally:
        work_loads[client_index] -= 1

def get_optimal_client():
    """Select client with least active downloads."""
    idx = min(work_loads, key=work_loads.get)
    return idx, multi_clients[idx]

# ==================== Safe Chunked Download ====================
async def safe_download(client: Client, message):
    """Download Telegram file safely with adaptive chunking."""
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

    # Sanitize filename
    file_name = "".join(c for c in file_name if c.isalnum() or c in (" ", ".", "_", "-")).strip()
    file_path = os.path.join(SAVE_DIR, file_name)
    os.makedirs(SAVE_DIR, exist_ok=True)

    # Adaptive chunk size
    chunk_size = CHUNK_SIZE_LARGE if file_size > 200 * 1024 * 1024 else CHUNK_SIZE_SMALL
    offset = 0
    retries = 0

    print(f"📥 Starting download: {file_path} | Chunk size: {chunk_size // 1024} KB")

    async with DOWNLOAD_SEMAPHORE:
        with open(file_path, "wb") as f:
            while True:
                try:
                    data = await asyncio.wait_for(
                        client.download_media(message, in_memory=True, offset=offset, limit=chunk_size),
                        timeout=TIMEOUT
                    )
                    if not data:
                        print(f"✅ Download completed: {file_path}")
                        break
                    f.write(data)
                    offset += len(data)
                    retries = 0

                except asyncio.TimeoutError:
                    retries += 1
                    print(f"⚠️ Timeout at offset {offset}, retry {retries}/{MAX_RETRIES}")
                    if retries >= MAX_RETRIES:
                        print("❌ Aborting download due to repeated timeouts")
                        break
                    await asyncio.sleep(2)

                except FloodWait as e:
                    print(f"⏳ FloodWait: sleeping for {e.value}s")
                    await asyncio.sleep(e.value + 1)

                except RPCError as e:
                    retries += 1
                    print(f"❌ RPCError ({type(e).__name__}): {e}, retry {retries}/{MAX_RETRIES}")
                    if retries >= MAX_RETRIES:
                        print("❌ Aborting after repeated RPC errors")
                        break
                    await asyncio.sleep(3)

                except Exception as e:
                    retries += 1
                    print(f"⚠️ Unknown error: {e}, retry {retries}/{MAX_RETRIES}")
                    if retries >= MAX_RETRIES:
                        print("❌ Stopping after repeated unknown errors")
                        break
                    await asyncio.sleep(3)

    print("📦 Download task ended\n")
    return file_path

# ==================== Multi-client Download Wrapper ====================
async def download_file(message):
    """Select optimal client and download file."""
    client_index, client = get_optimal_client()
    async with track_workload(client_index):
        return await safe_download(client, message)

# ==================== Connection Watchdog ====================
async def connection_watchdog(client, name):
    """Keep a client alive and auto-restart if disconnected."""
    while True:
        await asyncio.sleep(300)
        try:
            await client.get_me()
        except Exception:
            print(f"🔁 Connection lost for {name}, restarting...")
            try:
                await client.stop()
            except Exception:
                pass
            await asyncio.sleep(5)
            try:
                await client.start()
            except Exception as e:
                print(f"❌ Failed to restart client {name}: {e}")

# ==================== Main ====================
async def main():
    # Start clients and watchdogs
    for i, client in enumerate(multi_clients):
        await client.start()
        print(f"✅ Client {i} started: {client.session_name}")
        asyncio.create_task(connection_watchdog(client, client.session_name))

    print("⚡ ThunderBot Multi-Client Downloader started")
    print("✅ Ready to download files safely!")

    # Keep running
    await asyncio.get_event_loop().create_future()

if __name__ == "__main__":
    asyncio.run(main())
