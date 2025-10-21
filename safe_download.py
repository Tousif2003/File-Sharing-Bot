import asyncio
import os
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError
from dotenv import load_dotenv
from contextlib import asynccontextmanager

# ================= Load Environment Variables =================
load_dotenv("config.env")

# ================= Multi-client Initialization =================
multi_clients = []

# ----- API_ID/API_HASH clients -----
i = 1
while True:
    api_id = os.getenv(f"API_ID_{i}")
    api_hash = os.getenv(f"API_HASH_{i}")
    if not api_id or not api_hash:
        break
    multi_clients.append(Client(f"session_api_{i}", api_id=int(api_id), api_hash=api_hash))
    i += 1

# ----- Bot token clients -----
i = 1
while True:
    token = os.getenv(f"MULTI_TOKEN{i}")
    if not token:
        break
    multi_clients.append(Client(f"session_bot_{i}", bot_token=token))
    i += 1

if not multi_clients:
    raise Exception("❌ No clients found! Add API_ID/API_HASH or MULTI_TOKENs in config.env")

# ================= Download Configuration =================
CHUNK_SIZE_SMALL = 600 * 1024      # 600 KB for large files
CHUNK_SIZE_LARGE = 1024 * 1024     # 1 MB for small files
TIMEOUT = 25                        # per chunk timeout in seconds
MAX_RETRIES = 6                      # maximum retries per chunk
SAVE_DIR = "/tmp"                    # RAM-backed storage
MAX_CONCURRENT_DOWNLOADS = 6         # safe for free tiers

DOWNLOAD_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

# ======= Workload tracking for multi-client =======
work_loads = {idx: 0 for idx in range(len(multi_clients))}

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

# ======= Cleanup File Function =======
def cleanup_file(file_path):
    """Delete temporary file after usage."""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"🗑️  Cleanup done: {file_path}")
    except Exception as e:
        print(f"⚠️ Error deleting {file_path}: {e}")

# ======= Safe Download with Progress Bar =======
async def safe_download(client: Client, message):
    """Download Telegram file safely with progress and cleanup."""
    file_name = getattr(message.document or message.video or message.audio, "file_name", "file")
    file_size = getattr(message.document or message.video or message.audio, "file_size", 0)

    # Sanitize filename
    file_name = "".join(c for c in file_name if c.isalnum() or c in (" ", ".", "_", "-"))
    file_path = os.path.join(SAVE_DIR, file_name)
    os.makedirs(SAVE_DIR, exist_ok=True)

    chunk_size = CHUNK_SIZE_SMALL if file_size > 200 * 1024 * 1024 else CHUNK_SIZE_LARGE
    offset = 0
    retries = 0

    print(f"🚀 Downloading '{file_name}' ({file_size / 1024 / 1024:.2f} MB) with chunk {chunk_size // 1024} KB")

    async with DOWNLOAD_SEMAPHORE:
        try:
            with open(file_path, "wb") as f:
                while True:
                    try:
                        data = await asyncio.wait_for(
                            client.download_media(message, in_memory=True, offset=offset, limit=chunk_size),
                            timeout=TIMEOUT
                        )
                        if not data:
                            break

                        f.write(data)
                        offset += len(data)
                        retries = 0

                        # Progress bar
                        progress = (offset / file_size) * 100 if file_size else 0
                        print(f"⬇️  Progress: {progress:.2f}% ({offset / 1024 / 1024:.2f}/{file_size / 1024 / 1024:.2f} MB)", end="\r")

                    except asyncio.TimeoutError:
                        retries += 1
                        wait_time = 2 * retries
                        print(f"⚠️ Timeout at {offset}, retry {retries}/{MAX_RETRIES}, waiting {wait_time}s")
                        if retries >= MAX_RETRIES:
                            print("❌ Aborting download due to repeated timeouts")
                            break
                        await asyncio.sleep(wait_time)

                    except FloodWait as e:
                        print(f"⏳ FloodWait {e.value}s, sleeping...")
                        await asyncio.sleep(e.value + 1)

                    except RPCError as e:
                        retries += 1
                        wait_time = 3 * retries
                        print(f"❌ RPCError {type(e).__name__}, retry {retries}/{MAX_RETRIES}, waiting {wait_time}s")
                        if retries >= MAX_RETRIES:
                            print("❌ Aborting after repeated RPC errors")
                            break
                        await asyncio.sleep(wait_time)

                    except Exception as e:
                        retries += 1
                        wait_time = 3 * retries
                        print(f"⚠️ Unknown error: {e}, retry {retries}/{MAX_RETRIES}, waiting {wait_time}s")
                        if retries >= MAX_RETRIES:
                            print("❌ Stopping after repeated unknown errors")
                            break
                        await asyncio.sleep(wait_time)

        finally:
            # Cleanup temp file after download attempt
            cleanup_file(file_path)

    print(f"\n✅ Download task ended: {file_name}")
    return file_path

# ======= Multi-client download wrapper =======
async def download_file(message):
    client_index, client = get_optimal_client()
    print(f"🔹 Using client {client_index}")
    async with track_workload(client_index):
        return await safe_download(client, message)

# ======= Connection Watchdog =======
async def connection_watchdog(client, name):
    while True:
        await asyncio.sleep(300)
        try:
            await client.get_me()
        except Exception:
            print(f"🔁 Connection lost for {name}, restarting...")
            await client.stop()
            await asyncio.sleep(5)
            await client.start()

# ======= Main Bot Starter =======
async def main():
    for i, client in enumerate(multi_clients):
        await client.start()
        print(f"✅ Client {i} started: {client.session_name}")
        asyncio.create_task(connection_watchdog(client, client.session_name))

    print("⚡ ThunderBot Multi-Client Safe Downloader started")
    await asyncio.get_event_loop().create_future()  # keep bot alive

if __name__ == "__main__":
    asyncio.run(main())
