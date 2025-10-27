#!/usr/bin/env python3
"""
Thunder/safe_download.py
Smart Downloader v4.0 - FULL FEATURED

Features included:
- Multi-client selection & load balancing (uses Thunder.bot.multi_clients & work_loads)
- Smart DC selection (measures per-client latency via get_me ping)
- Adaptive in_memory switching:
    * Uses psutil to detect system memory pressure
    * Also toggles based on observed download speed / retries
- Robust chunked downloader using raw GetFile when possible
- Fallbacks: client.download_media(in_memory=True/False)
- .part resume & atomic finalize (os.replace / os.rename)
- Stall detection, exponential backoff + jitter, empty-chunk handling
- Periodic message refetch to refresh file_reference
- Client rotation on repeated failures (round-robin)
- DC-aware recreation attempt on MIGRATE errors (best-effort)
- Download state tracking: progress, speed history, status, retries
- Pause / Resume / Cancel controls via API functions
- WebSocket & HTTP JSON status endpoints (optional) for realtime progress
- CLI runner for testing (single or batch messages)
- No unsupported kwargs to Client constructor (fixes Pyromod TypeError)

Drop this file into Thunder/safe_download.py and restart your service.
"""

from __future__ import annotations

import asyncio
import os
import time
import random
import math
import logging
import traceback
import secrets
import json
import signal
from contextlib import asynccontextmanager
from typing import Optional, Any, List, Dict, Tuple, Callable

from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError
from pyrogram.raw.functions.upload import GetFile
from pyrogram.raw.types import InputDocumentFileLocation, InputPhotoFileLocation
from pyrogram.types import Message

# Optional libs
try:
    import psutil
except Exception:
    psutil = None

# aiohttp for optional websocket / http status server
try:
    from aiohttp import web, WSCloseCode
except Exception:
    web = None

# Load env if running standalone
load_dotenv("config.env")

# Import project's multi_clients, work_loads and Var
try:
    from Thunder.bot import multi_clients, work_loads
except Exception:
    multi_clients = []
    work_loads = {}

try:
    from Thunder.vars import Var
except Exception:
    class Var:  # fallback defaults if Var not available
        CHUNK_SIZE = 4 * 1024 * 1024
        TIMEOUT = 90
        MAX_RETRIES = 8
        DOWNLOAD_DIR = None
        MAX_CONCURRENT_DOWNLOADS = 3
        STALL_TIMEOUT = 60
        DEFAULT_IN_MEMORY = False
        WORKDIR = "/tmp"

# ----------------------------
# Logging
# ----------------------------
logger = logging.getLogger("Thunder.safe_download")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# ----------------------------
# Config (derived from Var/env)
# ----------------------------
CHUNK_SIZE = int(getattr(Var, "CHUNK_SIZE", int(os.getenv("CHUNK_SIZE", 4 * 1024 * 1024))))
TIMEOUT = int(getattr(Var, "TIMEOUT", int(os.getenv("TIMEOUT", 90))))
MAX_RETRIES = int(getattr(Var, "MAX_RETRIES", int(os.getenv("MAX_RETRIES", 8))))
# Save dir: 1) Var.DOWNLOAD_DIR 2) /mnt/data/files if exists 3) ./downloads
DEFAULT_SAVE_DIR = (
    getattr(Var, "DOWNLOAD_DIR", None)
    or ("/mnt/data/files" if os.path.exists("/mnt/data/files") else None)
    or os.path.join(os.getcwd(), "downloads")
)
SAVE_DIR = os.getenv("SAVE_DIR", DEFAULT_SAVE_DIR)
MAX_CONCURRENT_DOWNLOADS = int(getattr(Var, "MAX_CONCURRENT_DOWNLOADS", int(os.getenv("MAX_CONCURRENT_DOWNLOADS", 3))))
STALL_TIMEOUT = int(getattr(Var, "STALL_TIMEOUT", int(os.getenv("STALL_TIMEOUT", 60))))
USE_IN_MEMORY_DEFAULT = bool(getattr(Var, "DEFAULT_IN_MEMORY", os.getenv("DEFAULT_IN_MEMORY", "False").lower() in ("1", "true", "yes")))

# Websocket defaults
WS_HOST = os.getenv("WS_HOST", "0.0.0.0")
WS_PORT = int(os.getenv("WS_PORT", "8765"))

# Make directories
os.makedirs(SAVE_DIR, exist_ok=True)

# Semaphore for concurrent downloads
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

# ----------------------------
# Runtime state trackers
# ----------------------------
# download_states keyed by unique_id (message.chat_id:message_id) or message_id if chat implied
download_states: Dict[str, Dict[str, Any]] = {}
download_state_lock = asyncio.Lock()

# WebSocket clients (if WS server started)
ws_clients: List[Any] = []

# ----------------------------
# Helpers
# ----------------------------
def sanitize_filename(name: str) -> str:
    if not name:
        return f"file_{secrets.token_hex(6)}"
    safe = "".join(c for c in name if c.isalnum() or c in (" ", ".", "_", "-")).strip()
    return safe or f"file_{secrets.token_hex(6)}"

def jitter_sleep(base: float = 1.0, jitter: float = 1.0) -> float:
    return base + random.random() * jitter

def key_for_message(message: Message) -> str:
    """Unique key for identifying downloads across downloads_state"""
    try:
        chat = message.chat.id if message.chat else message.chat_id if hasattr(message, "chat_id") else None
        mid = message.message_id if hasattr(message, "message_id") else getattr(message, "id", None)
        return f"{chat}:{mid}" if chat is not None and mid is not None else str(mid or secrets.token_hex(6))
    except Exception:
        return secrets.token_hex(8)

def mem_pressure_threshold() -> float:
    """Return used memory percent threshold above which we prefer disk mode (0-100)."""
    # Prefer disk when memory usage > 70% (configurable here)
    return 70.0

def get_system_memory_percent() -> float:
    if psutil:
        return psutil.virtual_memory().percent
    # If psutil not present, return 0 so we don't falsely flip to disk
    return 0.0

async def broadcast_progress(ev: Dict[str, Any]):
    """Broadcast progress event to all WS clients (non-blocking)."""
    if not web:
        return
    msg_text = json.dumps(ev)
    coros = []
    for ws in list(ws_clients):
        try:
            coros.append(ws.send_str(msg_text))
        except Exception:
            # ignore
            pass
    if coros:
        await asyncio.gather(*coros, return_exceptions=True)

# ----------------------------
# Raw GetFile helper & location extraction
# ----------------------------
async def raw_getfile_chunk(client: Client, location_obj: Any, offset: int, limit: int) -> bytes:
    req = GetFile(location=location_obj, offset=offset, limit=limit)
    res = await client.invoke(req)
    data = getattr(res, "bytes", None) or getattr(res, "file_bytes", None) or None
    if data is None:
        file_attr = getattr(res, "file", None)
        if file_attr is not None and hasattr(file_attr, "bytes"):
            data = getattr(file_attr, "bytes")
    return data or b""

def extract_location_from_message(message: Message) -> Tuple[Optional[Any], int, Optional[str], Optional[int]]:
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
# Smart DC selector (measure latency)
# ----------------------------
async def measure_client_latency(client: Client, samples: int = 1) -> float:
    """Return average latency to client.get_me in seconds (best-effort)."""
    latencies = []
    for _ in range(samples):
        try:
            t0 = time.time()
            await client.get_me()
            latencies.append(time.time() - t0)
        except Exception:
            latencies.append(10.0)  # large value on failure
    return sum(latencies) / len(latencies) if latencies else 10.0

async def choose_best_client(prefer_dc: Optional[int] = None) -> Tuple[int, Client]:
    """
    Choose client by lowest workload then measure latency among the least-loaded subset.
    prefer_dc: if set, prefer clients that may be on that DC (best-effort via session_name tag)
    """
    if not work_loads:
        raise RuntimeError("No clients available")
    # pick few least-loaded indices
    sorted_idxs = sorted(work_loads.items(), key=lambda kv: kv[1])
    candidate_count = max(1, min(len(sorted_idxs), 3))
    candidates = [idx for idx, _ in sorted_idxs[:candidate_count]]
    # optionally reorder candidates that match prefer_dc in session_name
    if prefer_dc is not None:
        matches = [i for i in candidates if f"dc{prefer_dc}" in getattr(multi_clients[i], "session_name", "")]
        if matches:
            candidates = matches + [c for c in candidates if c not in matches]
    # measure latency concurrently
    coros = [measure_client_latency(multi_clients[i], samples=1) for i in candidates]
    lat_res = await asyncio.gather(*coros, return_exceptions=True)
    best_idx = candidates[0]
    best_lat = float("inf")
    for i, r in zip(candidates, lat_res):
        try:
            lat = float(r)
        except Exception:
            lat = 10.0
        if lat < best_lat:
            best_lat = lat
            best_idx = i
    return best_idx, multi_clients[best_idx]

# ----------------------------
# Download state management APIs
# ----------------------------
async def _init_state(key: str, info: Dict[str, Any]):
    async with download_state_lock:
        download_states[key] = {
            "status": "queued",
            "progress": 0,
            "downloaded": 0,
            "size": info.get("size", 0),
            "filename": info.get("filename", ""),
            "speed_history": [],
            "avg_speed": 0.0,
            "retries": 0,
            "start_time": None,
            "end_time": None,
            "message": info.get("message"),
            "pause_event": asyncio.Event(),
            "cancel": False,
            "last_update": time.time(),
            "dc": info.get("dc"),
            "history": [],
        }
        # initially not paused
        download_states[key]["pause_event"].set()

async def pause_download(key: str):
    async with download_state_lock:
        st = download_states.get(key)
        if not st:
            return False
        st["pause_event"].clear()
        st["status"] = "paused"
        st["last_update"] = time.time()
        return True

async def resume_download(key: str):
    async with download_state_lock:
        st = download_states.get(key)
        if not st:
            return False
        st["pause_event"].set()
        st["status"] = "resuming"
        st["last_update"] = time.time()
        return True

async def cancel_download(key: str):
    async with download_state_lock:
        st = download_states.get(key)
        if not st:
            return False
        st["cancel"] = True
        st["pause_event"].set()
        st["status"] = "cancelling"
        st["last_update"] = time.time()
        return True

def get_state_snapshot(key: str) -> Dict[str, Any]:
    st = download_states.get(key)
    if not st:
        return {}
    # shallow copy for safety
    s = {k: (v if k not in ("pause_event", "message") else None) for k, v in st.items()}
    return s

# ----------------------------
# Re-fetch message helper (for expired file_reference)
# ----------------------------
async def refetch_message_and_extract(client: Client, message: Message) -> Tuple[Message, Optional[Any], int, Optional[str], Optional[int]]:
    try:
        chat_id = message.chat.id if message.chat else getattr(message, "chat_id", None)
        msg_id = message.message_id if hasattr(message, "message_id") else getattr(message, "id", None)
        if chat_id is None or msg_id is None:
            return message, *extract_location_from_message(message)
        fresh = await client.get_messages(chat_id, msg_id)
        if not fresh:
            return message, *extract_location_from_message(message)
        return fresh, *extract_location_from_message(fresh)
    except Exception:
        return message, *extract_location_from_message(message)

# ----------------------------
# Core adaptive chunked download (full featured)
# ----------------------------
async def _adaptive_chunked_download(
    client: Client,
    message: Message,
    *,
    key: Optional[str] = None,
    chunk_size: int = CHUNK_SIZE,
    timeout: int = TIMEOUT,
    max_retries: int = MAX_RETRIES,
    save_dir: str = SAVE_DIR,
    stall_timeout: int = STALL_TIMEOUT,
    use_in_memory_override: Optional[bool] = None
) -> str:
    """
    Main robust downloader with adaptive in_memory switching, pause/resume, WS events and history.
    key: unique identifier for this download in download_states
    """
    # prepare metadata
    location_obj, file_size, suggested_name, detected_dc = extract_location_from_message(message)
    raw_name = suggested_name or getattr(getattr(message, "document", None) or getattr(message, "video", None) or getattr(message, "audio", None) or getattr(message, "photo", None), "file_name", None) or f"file_{secrets.token_hex(4)}"
    filename = sanitize_filename(raw_name)
    os.makedirs(save_dir, exist_ok=True)
    final_path = os.path.join(save_dir, filename)
    part_path = final_path + ".part"

    if not key:
        key = key_for_message(message)

    # initialize state
    await _init_state(key, {"size": file_size, "filename": filename, "message": message, "dc": detected_dc})

    state = download_states[key]
    state["status"] = "running"
    state["start_time"] = time.time()
    state["size"] = file_size
    state["filename"] = filename
    state["dc"] = detected_dc

    # resume offset
    offset = 0
    if os.path.exists(part_path):
        offset = os.path.getsize(part_path)

    # if final exists and looks complete, finish
    if os.path.exists(final_path) and file_size and os.path.getsize(final_path) >= file_size:
        state["status"] = "finished"
        state["downloaded"] = os.path.getsize(final_path)
        state["progress"] = 100.0
        state["end_time"] = time.time()
        await broadcast_progress({"key": key, "status": "finished", "path": final_path})
        return final_path

    # decide initial in_memory: override > auto mem pressure check > default
    auto_in_memory = USE_IN_MEMORY_DEFAULT if use_in_memory_override is None else use_in_memory_override
    mem_percent = get_system_memory_percent()
    if use_in_memory_override is None and mem_percent < mem_pressure_threshold():
        # if memory is healthy we can favor in_memory for small files
        if file_size and file_size < (200 * 1024 * 1024):  # 200MB threshold
            auto_in_memory = True
    else:
        # memory high -> prefer disk
        if mem_percent >= mem_pressure_threshold():
            auto_in_memory = False

    speed_history: List[float] = []
    retries = 0
    consecutive_empty = 0
    last_progress_time = time.time()
    backoff = 1.0

    logger.info(f"📥 Start download key={key} file={filename} size={file_size/1024/1024:.2f}MB dc={detected_dc} offset={offset} in_memory_start={auto_in_memory}")

    # if no raw location -> fallback direct disk write
    if location_obj is None:
        logger.info("ℹ️ No raw location found; using download_media fallback (disk)")
        tmp = part_path
        fallback_tries = 0
        while True:
            # allow pause/cancel check
            if state["cancel"]:
                state["status"] = "cancelled"
                await broadcast_progress({"key": key, "status": "cancelled"})
                raise asyncio.CancelledError("Download cancelled")
            await state["pause_event"].wait()

            try:
                await asyncio.wait_for(client.download_media(message, file_name=tmp, in_memory=False), timeout=timeout)
                try:
                    os.replace(tmp, final_path)
                except Exception:
                    os.rename(tmp, final_path)
                state["status"] = "finished"
                state["downloaded"] = os.path.getsize(final_path)
                state["progress"] = 100.0
                state["end_time"] = time.time()
                await broadcast_progress({"key": key, "status": "finished", "path": final_path})
                return final_path
            except FloodWait as e:
                logger.warning(f"🕓 FloodWait during fallback: sleeping {e.value}s")
                await asyncio.sleep(e.value + 1)
            except asyncio.TimeoutError:
                fallback_tries += 1
                logger.warning(f"⏳ Fallback timeout retry {fallback_tries}/{max_retries}")
                if fallback_tries >= max_retries:
                    state["status"] = "failed"
                    raise Exception("Fallback download_media timed out repeatedly")
                await asyncio.sleep(jitter_sleep(2, 2))
            except Exception as e:
                fallback_tries += 1
                logger.warning(f"⚠️ Fallback error: {e} (retry {fallback_tries}/{max_retries})")
                if fallback_tries >= max_retries:
                    state["status"] = "failed"
                    raise
                await asyncio.sleep(jitter_sleep(2, 2))

    # main chunk loop with improved resilience
    mode = "r+b" if os.path.exists(part_path) else "wb"
    with open(part_path, mode) as f:
        if os.path.exists(part_path):
            f.seek(offset)
            logger.info(f"🔁 Resuming part at {offset} bytes")
        consecutive_timeouts = 0
        last_refetch_time = time.time()
        REFRESH_INTERVAL = 60  # seconds to re-fetch message to refresh file_reference
        # detect client index if possible
        client_idx = None
        try:
            client_idx = multi_clients.index(client) if client in multi_clients else None
        except Exception:
            client_idx = None

        while True:
            # pause/cancel handling
            if state["cancel"]:
                state["status"] = "cancelled"
                await broadcast_progress({"key": key, "status": "cancelled"})
                raise asyncio.CancelledError("Download cancelled")
            await state["pause_event"].wait()

            if file_size and offset >= file_size:
                break

            # periodic refetch to refresh file_reference
            if time.time() - last_refetch_time > REFRESH_INTERVAL:
                try:
                    message, location_obj, file_size, suggested_name, detected_dc = await refetch_message_and_extract(client, message)
                    last_refetch_time = time.time()
                    logger.debug("ℹ️ Periodic message refetch to refresh file_reference")
                except Exception as e:
                    logger.debug(f"Periodic refetch failed: {e}")

            # stall detection
            if time.time() - last_progress_time > stall_timeout:
                retries += 1
                logger.warning(f"⚠️ Stall detected (no progress in {stall_timeout}s). retry {retries}/{max_retries}")
                backoff = min(backoff * 2, 30)
                await asyncio.sleep(jitter_sleep(backoff, 3))
                last_progress_time = time.time()
                if retries >= max_retries:
                    # try rotating client before full abort
                    if client_idx is not None and len(multi_clients) > 1:
                        new_idx = (client_idx + 1) % len(multi_clients)
                        logger.warning(f"🔁 Rotating client due to stall: {client_idx} -> {new_idx}")
                        client_idx = new_idx
                        client = multi_clients[client_idx]
                        try:
                            await ensure_client_started(client)
                        except Exception:
                            pass
                        retries = 0
                        backoff = 1.0
                        continue
                    state["status"] = "failed"
                    raise Exception("Download stalled too many times")
                continue

            try:
                start_time = time.time()
                # prefer raw chunk if possible
                if not auto_in_memory and location_obj is not None:
                    chunk = await asyncio.wait_for(raw_getfile_chunk(client, location_obj, offset, chunk_size), timeout=timeout)
                else:
                    # mem fallback: download_media in_memory (full file sometimes)
                    memobj = await asyncio.wait_for(client.download_media(message, in_memory=True), timeout=timeout)
                    if hasattr(memobj, "getbuffer"):
                        chunk = memobj.getbuffer().tobytes()
                    else:
                        chunk = memobj or b""

                if not chunk or len(chunk) == 0:
                    consecutive_empty += 1
                    retries += 1
                    logger.warning(f"⚠️ Empty chunk at offset {offset} (count={consecutive_empty}). retry {retries}/{max_retries}")
                    if consecutive_empty >= 2:
                        try:
                            message, location_obj, file_size, suggested_name, detected_dc = await refetch_message_and_extract(client, message)
                            logger.warning("🔁 Re-fetched message after empty chunk to refresh file_reference")
                        except Exception:
                            pass
                        consecutive_empty = 0
                    await asyncio.sleep(jitter_sleep(1, 1))
                    if retries >= max_retries:
                        # rotate client before abort
                        if client_idx is not None and len(multi_clients) > 1:
                            new_idx = (client_idx + 1) % len(multi_clients)
                            logger.warning(f"🔁 Rotating client due to repeated empty chunks/timeouts: {client_idx} -> {new_idx}")
                            client_idx = new_idx
                            client = multi_clients[client_idx]
                            try:
                                await ensure_client_started(client)
                            except Exception:
                                pass
                            retries = 0
                            backoff = 1.0
                            continue
                        state["status"] = "failed"
                        raise Exception("Repeated empty chunks, aborting")
                    continue

                # write chunk to file
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
                consecutive_timeouts = 0
                backoff = 1.0

                elapsed = max(1e-6, time.time() - start_time)
                speed = (written / 1024 / 1024) / elapsed
                speed_history.append(speed)
                if len(speed_history) > 20:
                    speed_history.pop(0)
                avg_speed = sum(speed_history) / len(speed_history) if speed_history else speed

                # update state
                state["downloaded"] = offset
                state["progress"] = (offset / file_size) * 100 if file_size else 0.0
                state["speed_history"] = speed_history[-10:]
                state["avg_speed"] = avg_speed
                state["retries"] = retries
                state["last_update"] = time.time()
                state["history"].append({"t": time.time(), "offset": offset, "speed": speed})
                await broadcast_progress({"key": key, "status": "running", "progress": state["progress"], "downloaded": offset, "size": file_size, "avg_speed": avg_speed})

                logger.info(f"⬇️ {state['progress']:.2f}% | chunk={written/1024:.1f}KB | {speed:.2f} MB/s | avg={avg_speed:.2f} MB/s | offset={offset/1024/1024:.2f}MB")

                # auto in_memory decisions: based on memory pressure & avg speed
                mem_percent = get_system_memory_percent()
                if mem_percent >= mem_pressure_threshold():
                    if auto_in_memory:
                        auto_in_memory = False
                        logger.warning("⚠️ Memory pressure detected -> switching to disk mode (in_memory=False)")
                else:
                    if avg_speed < 0.5 or retries >= 2:
                        if not auto_in_memory:
                            auto_in_memory = True
                            logger.warning("⚠️ Low speed/retries -> enabling in_memory mode")
                    elif avg_speed > 2.5 and retries == 0:
                        if auto_in_memory:
                            auto_in_memory = False
                            logger.info("✅ Speed stable -> disabling in_memory mode")

            except asyncio.TimeoutError:
                retries += 1
                consecutive_timeouts += 1
                backoff = min(backoff * 2, 30)
                logger.warning(f"⏳ Chunk timeout at offset {offset}. retry {retries}/{max_retries}. backoff {backoff}s")
                await asyncio.sleep(jitter_sleep(backoff, 2))
                # rotate client on repeated timeouts
                if consecutive_timeouts >= 2 and client_idx is not None and len(multi_clients) > 1:
                    new_idx = (client_idx + 1) % len(multi_clients)
                    logger.warning(f"🔁 Rotating client due to repeated timeouts: {client_idx} -> {new_idx}")
                    client_idx = new_idx
                    client = multi_clients[client_idx]
                    try:
                        await ensure_client_started(client)
                    except Exception:
                        pass
                    # attempt to refetch message on new client
                    try:
                        message, location_obj, file_size, suggested_name, detected_dc = await refetch_message_and_extract(client, message)
                    except Exception:
                        pass
                    consecutive_timeouts = 0
                    retries = 0
                    continue
                if retries >= max_retries:
                    # final fallback try in_memory full download
                    if not auto_in_memory:
                        logger.warning("⚠️ Final fallback enabling in_memory full download")
                        auto_in_memory = True
                        continue
                    state["status"] = "failed"
                    raise Exception("Chunk timeout: too many retries")
            except FloodWait as e:
                logger.warning(f"⏳ FloodWait {e.value}s — sleeping...")
                await asyncio.sleep(e.value + 1)
                continue
            except RPCError as e:
                err_text = str(e).upper()
                if "FILE_REFERENCE_EXPIRED" in err_text or "FILE_REFERENCE" in err_text:
                    logger.warning("🔁 FILE_REFERENCE_EXPIRED -> refetching message")
                    try:
                        message, location_obj, file_size, suggested_name, detected_dc = await refetch_message_and_extract(client, message)
                    except Exception:
                        pass
                    await asyncio.sleep(jitter_sleep(1.0, 1.5))
                    continue
                if "MIGRATE" in err_text or "PHONE_MIGRATE" in err_text or "NETWORK_MIGRATE" in err_text:
                    logger.warning(f"🔄 RPCError indicates DC migration: {e}")
                    # bubble up to caller wrapper to attempt DC recreate
                    raise
                if "TIMEOUT" in err_text:
                    retries += 1
                    backoff = min(backoff * 2, 30)
                    logger.warning(f"⚠️ RPC TIMEOUT from Telegram. retry {retries}/{max_retries}")
                    await asyncio.sleep(jitter_sleep(backoff, 2))
                    if retries >= max_retries and client_idx is not None and len(multi_clients) > 1:
                        new_idx = (client_idx + 1) % len(multi_clients)
                        logger.warning(f"🔁 Rotating client after RPC TIMEOUTs: {client_idx} -> {new_idx}")
                        client_idx = new_idx
                        client = multi_clients[client_idx]
                        try:
                            await ensure_client_started(client)
                        except Exception:
                            pass
                        retries = 0
                        continue
                    continue
                retries += 1
                backoff = min(backoff * 2, 30)
                logger.warning(f"❌ RPCError {type(e).__name__}: {e} retry {retries}/{max_retries}")
                if retries >= max_retries:
                    state["status"] = "failed"
                    raise
                await asyncio.sleep(jitter_sleep(backoff, 2))
                continue
            except Exception as e:
                retries += 1
                backoff = min(backoff * 2, 30)
                logger.error(f"⚠️ Unknown error during download chunk: {e}. retry {retries}/{max_retries}")
                traceback.print_exc()
                if retries >= max_retries:
                    # try rotating client once before giving up
                    if client_idx is not None and len(multi_clients) > 1:
                        new_idx = (client_idx + 1) % len(multi_clients)
                        logger.warning(f"🔁 Rotating client as last attempt: {client_idx} -> {new_idx}")
                        client_idx = new_idx
                        client = multi_clients[client_idx]
                        try:
                            await ensure_client_started(client)
                        except Exception:
                            pass
                        retries = 0
                        continue
                    state["status"] = "failed"
                    raise
                await asyncio.sleep(jitter_sleep(backoff, 2))
                continue

    # finalize
    try:
        os.replace(part_path, final_path)
    except Exception:
        try:
            os.rename(part_path, final_path)
        except Exception as e:
            logger.error(f"❌ Finalize failed: {e}")
            state["status"] = "failed"
            raise

    state["status"] = "finished"
    state["end_time"] = time.time()
    state["downloaded"] = os.path.getsize(final_path) if os.path.exists(final_path) else offset
    state["progress"] = 100.0
    await broadcast_progress({"key": key, "status": "finished", "path": final_path})
    logger.info(f"✅ Download finished key={key} path={final_path}")
    return final_path

# ----------------------------
# DC-aware wrapper: recreate client if MIGRATE error seen (best-effort)
# ----------------------------
async def _download_with_dc_handling(client_index: int, client: Client, message: Message, **kwargs):
    attempts = 0
    while True:
        try:
            return await _adaptive_chunked_download(client, message, **kwargs)
        except RPCError as e:
            s = str(e).upper()
            if "MIGRATE" in s or "PHONE_MIGRATE" in s or "NETWORK_MIGRATE" in s:
                logger.warning(f"🔄 DC migrate detected: {e}")
                dc_id = getattr(e, "value", None) or getattr(e, "new_dc", None) or getattr(e, "dc_id", None)
                try:
                    import re
                    m = re.search(r"MIGRATE_(\d+)", str(e), re.IGNORECASE)
                    if m:
                        dc_id = int(m.group(1))
                except Exception:
                    pass
                # Attempt to stop old client and recreate bound to dc (best-effort)
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
                        raise Exception("Missing credentials to recreate client")
                    await new_client.start()
                    multi_clients[client_index] = new_client
                    client = new_client
                    attempts += 1
                    if attempts >= MAX_RETRIES:
                        raise Exception("Too many DC migration attempts")
                    logger.info(f"✅ Recreated client index {client_index} for DC {dc_id}; retrying")
                    continue
                except Exception as ex:
                    logger.error(f"❌ Failed to recreate client for DC {dc_id}: {ex}")
                    raise
            else:
                raise

# ----------------------------
# Public multi-client wrapper
# ----------------------------
@asynccontextmanager
async def _track_workload_idx(idx: int):
    work_loads[idx] += 1
    try:
        yield
    finally:
        work_loads[idx] = max(0, work_loads[idx] - 1)

def _get_optimal_client_index() -> int:
    if not work_loads:
        raise RuntimeError("No clients loaded")
    return min(work_loads, key=lambda k: work_loads[k])

async def download_file(message: Message, *,
                        chunk_size: Optional[int] = None,
                        timeout: Optional[int] = None,
                        max_retries: Optional[int] = None,
                        save_dir: Optional[str] = None,
                        stall_timeout: Optional[int] = None,
                        use_in_memory_override: Optional[bool] = None,
                        prefer_dc: Optional[int] = None) -> str:
    """
    Public function used by stream_routes.py. Returns final path or raises.
    """
    chunk_size = chunk_size or CHUNK_SIZE
    timeout = timeout or TIMEOUT
    max_retries = max_retries or MAX_RETRIES
    save_dir = save_dir or SAVE_DIR
    stall_timeout = stall_timeout or STALL_TIMEOUT

    # choose best client: attempt latency-aware selection
    try:
        if prefer_dc is not None:
            idx, client = await choose_best_client(prefer_dc=prefer_dc)
        else:
            # pick least loaded subset & measure latency
            idx_candidate = _get_optimal_client_index()
            # measure small pool around idx_candidate
            idx, client = await choose_best_client(prefer_dc=None)
    except Exception:
        # fallback to simple index
        idx = _get_optimal_client_index()
        client = multi_clients[idx]

    async with _track_workload_idx(idx):
        async with DOWNLOAD_SEMAPHORE:
            # ensure client started
            try:
                if not getattr(client, "_is_connected", False):
                    await client.start()
            except Exception:
                try:
                    await client.start()
                except Exception as e:
                    logger.warning(f"Could not start client idx={idx}: {e}")

            key = key_for_message(message)
            try:
                result = await _download_with_dc_handling(idx, client, message,
                                                         key=key,
                                                         chunk_size=chunk_size,
                                                         timeout=timeout,
                                                         max_retries=max_retries,
                                                         save_dir=save_dir,
                                                         stall_timeout=stall_timeout,
                                                         use_in_memory_override=use_in_memory_override)
                return result
            except Exception as e:
                logger.error(f"❌ download_file failed for key={key} error={e}")
                raise

async def download_multiple(messages: List[Message], **kwargs) -> List[Any]:
    tasks = [asyncio.create_task(download_file(m, **kwargs)) for m in messages]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return results

# ----------------------------
# Watchdog (start if desired)
# ----------------------------
async def _client_watchdog(client: Client, name: str, interval: int = 300):
    while True:
        await asyncio.sleep(interval)
        try:
            await client.get_me()
        except Exception:
            logger.warning(f"🔁 Watchdog: client {name} disconnected; attempting restart...")
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

async def start_watchdogs():
    for i, c in enumerate(multi_clients):
        asyncio.create_task(_client_watchdog(c, getattr(c, "session_name", f"client_{i}")))

# ----------------------------
# Simple aiohttp WebSocket & HTTP server for realtime progress (optional)
# ----------------------------
def _make_ws_app():
    if web is None:
        raise RuntimeError("aiohttp not available; install aiohttp to use WS server")

    app = web.Application()
    routes = web.RouteTableDef()

    @routes.get("/ws")
    async def ws_handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        ws_clients.append(ws)
        logger.info("🔌 WebSocket client connected")
        try:
            async for msg in ws:
                if msg.type == web.WSMsgType.TEXT:
                    # support simple commands in WS: {"action":"list"} etc.
                    try:
                        data = json.loads(msg.data)
                        if data.get("action") == "list":
                            # send list of current downloads
                            async with download_state_lock:
                                snapshot = {k: get_state_snapshot(k) for k in download_states.keys()}
                            await ws.send_str(json.dumps({"type": "list", "data": snapshot}))
                    except Exception:
                        await ws.send_str(json.dumps({"error": "invalid command"}))
                elif msg.type == web.WSMsgType.ERROR:
                    logger.warning(f"WS connection closed with exception {ws.exception()}")
        finally:
            try:
                ws_clients.remove(ws)
            except Exception:
                pass
            logger.info("🔌 WebSocket client disconnected")
        return ws

    @routes.get("/status/{key}")
    async def status_handler(request):
        key = request.match_info["key"]
        async with download_state_lock:
            st = get_state_snapshot(key)
        if not st:
            raise web.HTTPNotFound(text=json.dumps({"error": "not found"}), content_type="application/json")
        return web.json_response(st)

    @routes.get("/status", )
    async def status_all(request):
        async with download_state_lock:
            data = {k: get_state_snapshot(k) for k in download_states.keys()}
        return web.json_response(data)

    app.add_routes(routes)
    return app

# ----------------------------
# CLI & module runner
# ----------------------------
def _install_signal_handlers(loop):
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(_shutdown(loop)))
        loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(_shutdown(loop)))
    except Exception:
        pass

async def _shutdown(loop):
    logger.info("🛑 Shutting down safe_download module...")
    # close ws clients
    if web:
        for ws in list(ws_clients):
            try:
                await ws.close(code=WSCloseCode.GOING_AWAY, message=b"shutting down")
            except Exception:
                pass
    # stop multi_clients? main app handles lifecycle. We'll not stop here.
    await asyncio.sleep(0.1)
    loop.stop()

async def _cli_run(args):
    # start first client for fetching messages
    if not multi_clients:
        logger.error("No multi_clients configured. Exiting.")
        return
    # optional: start watchdogs
    asyncio.create_task(start_watchdogs())
    client = multi_clients[0]
    await client.start()
    logger.info("Client started for CLI.")

    if args.ws and web:
        app = _make_ws_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, WS_HOST, WS_PORT)
        await site.start()
        logger.info(f"WebSocket server listening at ws://{WS_HOST}:{WS_PORT}/ws")

    if args.chat is None or args.msg is None:
        logger.info("No chat/msg provided for CLI download. WS server active (if requested). Press Ctrl+C to exit.")
        # keep running if ws enabled
        if args.ws and web:
            await asyncio.get_event_loop().create_future()
        await client.stop()
        return

    # fetch message
    msg = await client.get_messages(args.chat, args.msg)
    if not msg:
        logger.error("Message not found")
        await client.stop()
        return

    # start download
    key = key_for_message(msg)
    try:
        res = await download_file(msg,
                                  chunk_size=args.chunk,
                                  timeout=args.timeout,
                                  max_retries=args.retries,
                                  save_dir=args.output,
                                  use_in_memory_override=(True if args.in_memory == "true" else False if args.in_memory == "false" else None))
        logger.info(f"Download complete: {res}")
    except Exception as e:
        logger.error(f"Download error: {e}")
    await client.stop()

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Thunder safe_download v4.0 CLI & Runner")
    parser.add_argument("--chat", type=int, help="chat id")
    parser.add_argument("--msg", type=int, help="message id")
    parser.add_argument("--output", type=str, default=SAVE_DIR, help="download directory")
    parser.add_argument("--chunk", type=int, default=CHUNK_SIZE, help="chunk size bytes")
    parser.add_argument("--timeout", type=int, default=TIMEOUT, help="per-chunk timeout")
    parser.add_argument("--retries", type=int, default=MAX_RETRIES, help="max retries per chunk")
    parser.add_argument("--in-memory", choices=["auto", "true", "false"], default="auto", help="override in_memory mode")
    parser.add_argument("--ws", action="store_true", help="start WS progress server (default port 8765)")
    args = parser.parse_args()

    if args.in_memory == "true":
        args.in_memory = "true"
    elif args.in_memory == "false":
        args.in_memory = "false"
    else:
        args.in_memory = "auto"

    loop = asyncio.get_event_loop()
    _install_signal_handlers(loop)
    try:
        loop.run_until_complete(_cli_run(args))
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        # attempt graceful shutdown
        pending = asyncio.all_tasks(loop=loop)
        for t in pending:
            t.cancel()
        try:
            loop.run_until_complete(asyncio.sleep(0.1))
        except Exception:
            pass

if __name__ == "__main__":
    main()
