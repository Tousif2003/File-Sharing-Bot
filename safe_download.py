#!/usr/bin/env python3
"""
Thunder/safe_download.py
Thunder Smart Downloader — DC-aware, adaptive in_memory, resume-safe

Place this file at: Thunder/safe_download.py

Exports:
    async def download_file(message: pyrogram.types.Message) -> str
    async def download_multiple(messages: List[Message]) -> List[Any]

Features:
- Uses existing `multi_clients` and `work_loads` (from Thunder.bot).
- Chunked raw GetFile reads when possible (resume via .part).
- Fallback to client.download_media(in_memory=...) when necessary.
- Adaptive in_memory toggle (auto switch on low speed / repeated errors).
- Stall detection, exponential backoff + jitter.
- Best-effort DC migration handling (recreate client with same creds).
- Watchdogs + CLI helper.
- Avoids passing unsupported kwargs to Client() constructor.
"""

from __future__ import annotations

import asyncio
import os
import time
import random
import logging
import traceback
import secrets
from contextlib import asynccontextmanager
from typing import Optional, Any, List, Tuple

from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError
from pyrogram.raw.functions.upload import GetFile
from pyrogram.raw.types import InputDocumentFileLocation, InputPhotoFileLocation
from pyrogram.types import Message

# Load env (safe to call; your repo already does this elsewhere)
load_dotenv("config.env")

# Import the existing multi_clients and work_loads from your bot loader
# (Your Thunder.bot module should already have created multi_clients & work_loads)
try:
    from Thunder.bot import multi_clients, work_loads
except Exception:
    # Fallback: if Thunder.bot isn't loaded yet, create placeholders (will error later)
    multi_clients = []
    work_loads = {}

# Import Var for defaults
from Thunder.vars import Var

# ----------------------------
# Logging
# ----------------------------
logger = logging.getLogger("Thunder.safe_download")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# ----------------------------
# Configurable defaults (from Var or environment)
# ----------------------------
CHUNK_SIZE = getattr(Var, "CHUNK_SIZE", 4 * 1024 * 1024)  # bytes (default 4MB)
TIMEOUT = getattr(Var, "TIMEOUT", 90)                    # seconds per chunk
MAX_RETRIES = getattr(Var, "MAX_RETRIES", 8)
# Save directory preference: prefer /mnt/data/files if present (persistent), else downloads/
DEFAULT_SAVE_DIR = "/mnt/data/files" if os.path.exists("/mnt/data/files") else os.path.join(os.getcwd(), "downloads")
SAVE_DIR = getattr(Var, "DOWNLOAD_DIR", DEFAULT_SAVE_DIR) if hasattr(Var, "DOWNLOAD_DIR") else DEFAULT_SAVE_DIR
MAX_CONCURRENT_DOWNLOADS = getattr(Var, "MAX_CONCURRENT_DOWNLOADS", 3)
STALL_TIMEOUT = getattr(Var, "STALL_TIMEOUT", 60)
USE_IN_MEMORY_DEFAULT = getattr(Var, "DEFAULT_IN_MEMORY", False)

# Ensure save dir exists
os.makedirs(SAVE_DIR, exist_ok=True)

# Semaphore for concurrent downloads
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

# ----------------------------
# Utilities
# ----------------------------
def sanitize_filename(name: str) -> str:
    if not name:
        return f"file_{secrets.token_hex(6)}"
    safe = "".join(c for c in name if c.isalnum() or c in (" ", ".", "_", "-")).strip()
    return safe or f"file_{secrets.token_hex(6)}"

def jitter_sleep(base: float = 1.0, jitter: float = 1.0) -> float:
    return base + random.random() * jitter

async def ensure_client_started(client: Client) -> None:
    """Start client if not started. Safe no-op if already running."""
    try:
        # Pyrogram internals differ; attempt robust start
        if not getattr(client, "_is_connected", False):
            await client.start()
    except Exception as e:
        logger.debug(f"ensure_client_started: start attempt raised: {e}")

# ----------------------------
# Raw GetFile helper and location extraction
# ----------------------------
async def raw_getfile_chunk(client: Client, location_obj: Any, offset: int, limit: int) -> bytes:
    """Invoke raw.upload.GetFile and return bytes (best effort for different response shapes)."""
    req = GetFile(location=location_obj, offset=offset, limit=limit)
    res = await client.invoke(req)
    data = getattr(res, "bytes", None) or getattr(res, "file_bytes", None) or None
    if data is None:
        file_attr = getattr(res, "file", None)
        if file_attr is not None and hasattr(file_attr, "bytes"):
            data = getattr(file_attr, "bytes")
    return data or b""

def extract_location_from_message(message: Message) -> Tuple[Optional[Any], int, Optional[str], Optional[int]]:
    """
    Try to build InputDocumentFileLocation or InputPhotoFileLocation from message.
    Returns (location_obj or None, file_size, suggested_name, detected_dc)
    """
    file_obj = getattr(message, "document", None) or getattr(message, "video", None) or getattr(message, "audio", None) or getattr(message, "photo", None)
    if not file_obj:
        return None, 0, None, None

    suggested_name = getattr(file_obj, "file_name", None) or getattr(file_obj, "file_unique_id", None) or None
    file_size = getattr(file_obj, "file_size", None) or getattr(file_obj, "size", None) or 0
    detected_dc = None

    raw = getattr(file_obj, "_raw", None) or getattr(file_obj, "__dict__", None)
    if raw:
        doc_id = getattr(raw, "id", None) or getattr(raw, "document_id", None)
        access_hash = getattr(raw, "access_hash", None) or getattr(raw, "accessHash", None)
        file_ref = getattr(raw, "file_reference", None)
        dc_val = getattr(raw, "dc_id", None) or getattr(raw, "dc", None)
        if dc_val:
            try:
                detected_dc = int(dc_val)
            except Exception:
                detected_dc = None
        try:
            if doc_id and access_hash and file_ref is not None:
                loc = InputDocumentFileLocation(id=int(doc_id), access_hash=int(access_hash), file_reference=file_ref, thumb_size="")
                return loc, file_size, suggested_name, detected_dc
        except Exception:
            pass
        # photo fallback
        photo_id = getattr(raw, "id", None) or getattr(raw, "photo_id", None)
        photo_access = getattr(raw, "access_hash", None)
        photo_ref = getattr(raw, "file_reference", None)
        try:
            if photo_id and photo_access and photo_ref is not None:
                loc = InputPhotoFileLocation(id=int(photo_id), access_hash=int(photo_access), file_reference=photo_ref, thumb_size="")
                return loc, file_size, suggested_name, detected_dc
        except Exception:
            pass

    return None, file_size, suggested_name, detected_dc

# ----------------------------
# Core download implementation (adaptive)
# ----------------------------
async def _adaptive_chunked_download(
    client: Client,
    message: Message,
    *,
    chunk_size: int = CHUNK_SIZE,
    timeout: int = TIMEOUT,
    max_retries: int = MAX_RETRIES,
    save_dir: str = SAVE_DIR,
    stall_timeout: int = STALL_TIMEOUT,
    use_in_memory_override: Optional[bool] = None
) -> str:
    """
    Robust downloader:
    - prefer raw GetFile chunks when available
    - fallback to download_media (in_memory True/False)
    - .part resume + atomic finalize
    - adaptive in_memory toggling
    """
    file_obj = getattr(message, "document", None) or getattr(message, "video", None) or getattr(message, "audio", None) or getattr(message, "photo", None)
    if not file_obj:
        raise ValueError("Message contains no media")

    location_obj, file_size, suggested_name, detected_dc = extract_location_from_message(message)
    raw_name = suggested_name or getattr(file_obj, "file_name", None) or getattr(file_obj, "file_unique_id", None) or f"file_{secrets.token_hex(4)}"
    filename = sanitize_filename(raw_name)

    os.makedirs(save_dir, exist_ok=True)
    final_path = os.path.join(save_dir, filename)
    part_path = final_path + ".part"

    # resume offset
    offset = 0
    if os.path.exists(part_path):
        offset = os.path.getsize(part_path)
    # if final exists and matches expected size, return it
    if os.path.exists(final_path) and file_size and os.path.getsize(final_path) >= file_size:
        logger.info(f"✅ Already exists: {final_path}")
        return final_path

    # adaptive vars
    auto_in_memory = USE_IN_MEMORY_DEFAULT if use_in_memory_override is None else use_in_memory_override
    speed_history: List[float] = []
    retries = 0
    consecutive_empty = 0
    last_progress_time = time.time()
    backoff = 1.0

    logger.info(f"📥 Start download: {final_path} | size={file_size/1024/1024:.2f}MB | dc={detected_dc} | offset={offset} bytes | in_memory={auto_in_memory}")

    # fallback if no raw location (use disk-mode direct write)
    if location_obj is None:
        logger.info("ℹ️ Raw location not found — using download_media fallback (disk write).")
        tmp = part_path
        fallback_retries = 0
        while True:
            try:
                await asyncio.wait_for(client.download_media(message, file_name=tmp, in_memory=False), timeout=timeout)
                try:
                    os.replace(tmp, final_path)
                except Exception:
                    os.rename(tmp, final_path)
                logger.info(f"✅ Fallback download complete: {final_path}")
                return final_path
            except FloodWait as e:
                logger.warning(f"⏳ FloodWait during fallback: sleep {e.value}s")
                await asyncio.sleep(e.value + 1)
            except asyncio.TimeoutError:
                fallback_retries += 1
                logger.warning(f"Timeout during fallback download_media retry {fallback_retries}/{max_retries}")
                if fallback_retries >= max_retries:
                    raise Exception("Fallback download_media timed out repeatedly")
                await asyncio.sleep(jitter_sleep(2, 2))
            except Exception as e:
                fallback_retries += 1
                logger.warning(f"Error during fallback download_media: {e} (retry {fallback_retries}/{max_retries})")
                if fallback_retries >= max_retries:
                    raise
                await asyncio.sleep(jitter_sleep(2, 2))

    # main chunked loop (raw or adaptive)
    mode = "r+b" if os.path.exists(part_path) else "wb"
    with open(part_path, mode) as f:
        if os.path.exists(part_path):
            f.seek(offset)
            logger.info(f"🔁 Resuming part at {offset} bytes")
        while True:
            if file_size and offset >= file_size:
                break

            # Stall detection
            if time.time() - last_progress_time > stall_timeout:
                retries += 1
                logger.warning(f"⚠️ Stall detected (no progress in {stall_timeout}s). retry {retries}/{max_retries}")
                backoff = min(backoff * 2, 30)
                await asyncio.sleep(jitter_sleep(backoff, 3))
                last_progress_time = time.time()
                if retries >= max_retries:
                    raise Exception("Download stalled too many times")
                continue

            try:
                start_time = time.time()
                # prefer raw_getfile_chunk if in-disk mode
                if not auto_in_memory:
                    chunk = await asyncio.wait_for(raw_getfile_chunk(client, location_obj, offset, chunk_size), timeout=timeout)
                else:
                    memobj = await asyncio.wait_for(client.download_media(message, in_memory=True), timeout=timeout)
                    if hasattr(memobj, "getbuffer"):
                        chunk = memobj.getbuffer().tobytes()
                    else:
                        chunk = memobj or b""

                if not chunk or len(chunk) == 0:
                    consecutive_empty += 1
                    retries += 1
                    logger.warning(f"⚠️ Empty chunk at offset {offset} (count={consecutive_empty}). retry {retries}/{max_retries}")
                    if consecutive_empty >= 3:
                        await asyncio.sleep(jitter_sleep(2, 2))
                        consecutive_empty = 0
                    else:
                        await asyncio.sleep(1)
                    if retries >= max_retries:
                        raise Exception("Repeated empty chunks, aborting")
                    continue

                # write safely
                f.seek(offset)
                f.write(chunk)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception:
                    pass

                written = len(chunk)
                offset += written
                last_progress_time = time.time()
                retries = 0
                consecutive_empty = 0
                backoff = 1.0

                elapsed = max(1e-6, time.time() - start_time)
                speed = (written / 1024 / 1024) / elapsed
                speed_history.append(speed)
                if len(speed_history) > 10:
                    speed_history.pop(0)
                avg_speed = sum(speed_history) / len(speed_history) if speed_history else speed

                progress_pct = (offset / file_size) * 100 if file_size else 0.0
                logger.info(f"⬇️ {progress_pct:.2f}% | chunk={written/1024:.1f}KB | {speed:.2f} MB/s | avg={avg_speed:.2f} MB/s | offset={offset/1024/1024:.2f}MB")

                # Adaptive in_memory switching logic
                if avg_speed < 0.5 or retries >= 2:
                    if not auto_in_memory:
                        auto_in_memory = True
                        logger.warning("⚠️ Low speed or repeated retries — enabling in_memory mode")
                elif avg_speed > 2.5 and retries == 0:
                    if auto_in_memory:
                        auto_in_memory = False
                        logger.info("✅ Speed stable — disabling in_memory mode")

            except asyncio.TimeoutError:
                retries += 1
                backoff = min(backoff * 2, 30)
                logger.warning(f"⏳ Chunk timeout at offset {offset}. retry {retries}/{max_retries}. backoff {backoff}s")
                await asyncio.sleep(jitter_sleep(backoff, 2))
                if retries >= max_retries:
                    raise Exception("Chunk timeout: too many retries")
                if retries >= 2 and not auto_in_memory:
                    auto_in_memory = True
                    logger.warning("⚠️ Enabling in_memory due to repeated timeouts")
                continue

            except FloodWait as e:
                logger.warning(f"⏳ FloodWait {e.value}s — sleeping...")
                await asyncio.sleep(e.value + 1)
                continue

            except RPCError as e:
                err_text = str(e).upper()
                if "MIGRATE" in err_text or "PHONE_MIGRATE" in err_text or "NETWORK_MIGRATE" in err_text:
                    logger.warning(f"🔄 RPCError indicates DC migration: {e}")
                    # bubble up to outer wrapper to handle recreation/rotation
                    raise
                if "TIMEOUT" in err_text:
                    retries += 1
                    backoff = min(backoff * 2, 30)
                    logger.warning(f"⚠️ RPC TIMEOUT from server at offset {offset}. retry {retries}/{max_retries}. backoff {backoff}s")
                    await asyncio.sleep(jitter_sleep(backoff, 2))
                    if retries >= max_retries:
                        raise Exception("Repeated RPC TIMEOUTs, aborting")
                    if not auto_in_memory:
                        auto_in_memory = True
                        logger.warning("⚠️ Enabling in_memory due to RPC TIMEOUTs")
                    continue
                # generic RPCError -> retry
                retries += 1
                backoff = min(backoff * 2, 30)
                logger.warning(f"❌ RPCError {type(e).__name__}: {e} — retry {retries}/{max_retries}")
                if retries >= max_retries:
                    raise
                await asyncio.sleep(jitter_sleep(backoff, 2))
                continue

            except Exception as e:
                retries += 1
                backoff = min(backoff * 2, 30)
                logger.error(f"⚠️ Unknown error during download chunk: {e}. retry {retries}/{max_retries}")
                traceback.print_exc()
                if retries >= max_retries:
                    raise
                await asyncio.sleep(jitter_sleep(backoff, 2))
                continue

    # finalize .part -> final
    try:
        os.replace(part_path, final_path)
    except Exception:
        try:
            os.rename(part_path, final_path)
        except Exception as e:
            logger.error(f"❌ Finalize failed: {e}")
            raise
    logger.info(f"✅ Download finished: {final_path}")
    return final_path

# ----------------------------
# DC-aware wrapper (best-effort recreate client)
# ----------------------------
async def _download_with_dc_handling(client_index: int, client: Client, message: Message, **kwargs) -> str:
    attempts = 0
    while True:
        try:
            return await _adaptive_chunked_download(client, message, **kwargs)
        except RPCError as e:
            s = str(e).upper()
            if "MIGRATE" in s or "PHONE_MIGRATE" in s or "NETWORK_MIGRATE" in s:
                # parse target dc id
                dc_id = getattr(e, "value", None) or getattr(e, "new_dc", None) or getattr(e, "dc_id", None)
                try:
                    import re
                    m = re.search(r"MIGRATE_(\d+)", str(e), re.IGNORECASE)
                    if m:
                        dc_id = int(m.group(1))
                except Exception:
                    pass
                logger.warning(f"🔄 DC migration requested -> target DC {dc_id}. Attempting to recreate client (best-effort).")
                # stop old client (best-effort)
                try:
                    await client.stop()
                except Exception:
                    pass
                session_name = getattr(client, "session_name", f"session_recreate_{client_index}")
                api_id = getattr(client, "api_id", None)
                api_hash = getattr(client, "api_hash", None)
                bot_token = getattr(client, "bot_token", None) if hasattr(client, "bot_token") else None
                try:
                    if api_id and api_hash:
                        new_client = Client(f"{session_name}_dc{dc_id}", api_id=api_id, api_hash=api_hash, workdir=getattr(Var, "WORKDIR", "/tmp"))
                    elif bot_token:
                        new_client = Client(f"{session_name}_dc{dc_id}", bot_token=bot_token, workdir=getattr(Var, "WORKDIR", "/tmp"))
                    else:
                        raise Exception("Missing credentials to recreate client for DC migration")
                    await new_client.start()
                    multi_clients[client_index] = new_client
                    client = new_client
                    attempts += 1
                    if attempts >= MAX_RETRIES:
                        raise Exception("Too many DC migration attempts")
                    logger.info(f"✅ Recreated client for DC {dc_id} at index {client_index}; retrying download.")
                    continue
                except Exception as ex:
                    logger.error(f"❌ Failed to recreate client for DC {dc_id}: {ex}")
                    raise
            else:
                raise

# ----------------------------
# Workload tracking helpers (module-local)
# ----------------------------
@asynccontextmanager
async def _track_workload_idx(idx: int):
    work_loads[idx] += 1
    try:
        yield
    finally:
        work_loads[idx] = max(0, work_loads[idx] - 1)

def _get_optimal_client() -> Tuple[int, Client]:
    if not work_loads:
        raise RuntimeError("No clients loaded (work_loads empty)")
    # pick the client with the smallest workload
    idx = min(work_loads, key=lambda k: work_loads[k])
    return idx, multi_clients[idx]

# ----------------------------
# Public function expected by stream_routes.py
# ----------------------------
async def download_file(message: Message, *,
                        chunk_size: Optional[int] = None,
                        timeout: Optional[int] = None,
                        max_retries: Optional[int] = None,
                        save_dir: Optional[str] = None,
                        stall_timeout: Optional[int] = None,
                        use_in_memory_override: Optional[bool] = None) -> str:
    """
    Public API used by stream_routes.py. Returns final file path on success.
    """
    chunk_size = chunk_size or CHUNK_SIZE
    timeout = timeout or TIMEOUT
    max_retries = max_retries or MAX_RETRIES
    save_dir = save_dir or SAVE_DIR
    stall_timeout = stall_timeout or STALL_TIMEOUT

    client_index, client = _get_optimal_client()
    async with _track_workload_idx(client_index):
        async with DOWNLOAD_SEMAPHORE:
            try:
                await ensure_client_started(client)
            except Exception as e:
                logger.warning(f"Could not start client {client_index}: {e}")

            logger.info(f"🔁 Using client {client_index} ({getattr(client,'session_name', 'unknown')}) for download")
            try:
                res = await _download_with_dc_handling(
                    client_index, client, message,
                    chunk_size=chunk_size,
                    timeout=timeout,
                    max_retries=max_retries,
                    save_dir=save_dir,
                    stall_timeout=stall_timeout,
                    use_in_memory_override=use_in_memory_override
                )
                return res
            except Exception as e:
                logger.error(f"❌ download_file failed for message {getattr(message,'message_id',None)}: {e}")
                raise

async def download_multiple(messages: List[Message], **kwargs) -> List[Any]:
    tasks = [asyncio.create_task(download_file(m, **kwargs)) for m in messages]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return results

# ----------------------------
# Watchdog helpers
# ----------------------------
async def _client_watchdog(client: Client, name: str, interval: int = 300):
    while True:
        await asyncio.sleep(interval)
        try:
            await client.get_me()
        except Exception:
            logger.warning(f"🔁 Watchdog: client {name} disconnected, attempting restart...")
            try:
                await client.stop()
            except Exception:
                pass
            await asyncio.sleep(5)
            try:
                await client.start()
                logger.info(f"✅ Watchdog restarted client {name}")
            except Exception as e:
                logger.error(f"❌ Watchdog restart failed for {name}: {e}")
                # keep trying in next loop

async def start_watchdogs():
    for i, c in enumerate(multi_clients):
        asyncio.create_task(_client_watchdog(c, getattr(c, "session_name", f"client_{i}")))

# ----------------------------
# CLI quick tester (run as script)
# ----------------------------
if __name__ == "__main__":
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(description="Thunder safe_download CLI tester")
    parser.add_argument("--chat", type=int, help="chat_id (channel/group)", required=False)
    parser.add_argument("--msg", type=int, help="message_id", required=False)
    parser.add_argument("--output", type=str, default=SAVE_DIR, help="download directory")
    parser.add_argument("--chunk", type=int, default=CHUNK_SIZE, help="chunk size bytes")
    parser.add_argument("--timeout", type=int, default=TIMEOUT, help="per-chunk timeout")
    parser.add_argument("--retries", type=int, default=MAX_RETRIES, help="max retries per chunk")
    parser.add_argument("--in-memory", type=str, choices=["auto", "true", "false"], default="auto", help="Override adaptive in_memory mode")
    args = parser.parse_args()

    use_override = None
    if args.in_memory == "true":
        use_override = True
    elif args.in_memory == "false":
        use_override = False

    async def _cli_main():
        if not multi_clients:
            logger.error("No multi_clients configured. Exiting.")
            return
        client = multi_clients[0]
        await client.start()
        logger.info("Client started for CLI fetch.")
        if args.chat is None or args.msg is None:
            logger.info("No chat/msg provided; exiting CLI.")
            await client.stop()
            return
        msg = await client.get_messages(args.chat, args.msg)
        if not msg:
            logger.error("Message not found")
            await client.stop()
            return
        try:
            res = await download_file(msg,
                                      chunk_size=args.chunk,
                                      timeout=args.timeout,
                                      max_retries=args.retries,
                                      save_dir=args.output,
                                      use_in_memory_override=use_override)
            logger.info(f"Download finished: {res}")
        except Exception as e:
            logger.error(f"Download error: {e}")
        await client.stop()

    asyncio.run(_cli_main())
