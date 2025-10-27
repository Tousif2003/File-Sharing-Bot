#!/usr/bin/env python3
"""
Thunder/safe_download.py
Final Smart Downloader — hybrid save+stream, auto-resume, DC-aware, in_memory-adaptive.

Usage:
- Import and call `await download_file(message)` (returns local path).
- Or call `await stream_and_download(message, request)` from an aiohttp handler
  to stream to the incoming HTTP request while downloading to disk.

Place this file at: Thunder/safe_download.py (replace existing).
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
from contextlib import asynccontextmanager
from typing import Optional, Any, Tuple, List, Dict

from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError
from pyrogram.raw.functions.upload import GetFile
from pyrogram.raw.types import InputDocumentFileLocation, InputPhotoFileLocation
from pyrogram.types import Message

# optional
try:
    import psutil
except Exception:
    psutil = None

# aiohttp used only if streaming via HTTP
try:
    from aiohttp import web
except Exception:
    web = None

# load env
load_dotenv("config.env")

# import existing multi_clients and work_loads from your bot loader (Thunder.bot)
try:
    from Thunder.bot import multi_clients, work_loads
except Exception:
    multi_clients = []
    work_loads = {}

# Var config fallback
try:
    from Thunder.vars import Var
except Exception:
    class Var:
        DOWNLOAD_DIR = None
        CHUNK_SIZE = 4 * 1024 * 1024
        TIMEOUT = 90
        MAX_RETRIES = 8
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
# Config (env / Var)
# ----------------------------
CHUNK_SIZE = int(getattr(Var, "CHUNK_SIZE", os.getenv("CHUNK_SIZE", 1024 * 1024)))  # 1 MB default
TIMEOUT = int(getattr(Var, "TIMEOUT", os.getenv("TIMEOUT", 30)))
MAX_RETRIES = int(getattr(Var, "MAX_RETRIES", os.getenv("MAX_RETRIES", 6)))
DEFAULT_SAVE_DIR = getattr(Var, "DOWNLOAD_DIR", None) or ("/mnt/data/files" if os.path.exists("/mnt/data/files") else os.path.join(os.getcwd(), "downloads"))
SAVE_DIR = os.getenv("SAVE_DIR", DEFAULT_SAVE_DIR)
MAX_CONCURRENT_DOWNLOADS = int(getattr(Var, "MAX_CONCURRENT_DOWNLOADS", os.getenv("MAX_CONCURRENT_DOWNLOADS", 5)))
STALL_TIMEOUT = int(getattr(Var, "STALL_TIMEOUT", os.getenv("STALL_TIMEOUT", 60)))
USE_IN_MEMORY_DEFAULT = bool(getattr(Var, "DEFAULT_IN_MEMORY", os.getenv("DEFAULT_IN_MEMORY", "False").lower() in ("1", "true", "yes")))

# Ensure save dir
os.makedirs(SAVE_DIR, exist_ok=True)

# Concurrent control
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

# Download states (for pause/resume/progress)
download_states: Dict[str, Dict[str, Any]] = {}
download_state_lock = asyncio.Lock()

# ----------------------------
# Utilities
# ----------------------------
def sanitize_filename(name: str) -> str:
    if not name:
        return f"file_{secrets.token_hex(6)}"
    safe = "".join(c for c in name if c.isalnum() or c in (" ", ".", "_", "-")).strip()
    return safe or f"file_{secrets.token_hex(6)}"

def key_for_message(message: Message) -> str:
    try:
        chat = message.chat.id if message.chat else getattr(message, "chat_id", None)
        mid = message.message_id if hasattr(message, "message_id") else getattr(message, "id", None)
        return f"{chat}:{mid}" if chat is not None and mid is not None else str(mid or secrets.token_hex(6))
    except Exception:
        return secrets.token_hex(8)

def jitter_sleep(base: float = 1.0, jitter: float = 1.0) -> float:
    return base + random.random() * jitter

def mem_pressure_threshold() -> float:
    return float(os.getenv("MEM_PRESSURE_PCT", "80.0"))

def get_system_memory_percent() -> float:
    if psutil:
        try:
            return psutil.virtual_memory().percent
        except Exception:
            return 0.0
    return 0.0

# ----------------------------
# Raw GetFile + location extract
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
# Smart client selection: least loaded + latency probe
# ----------------------------
async def measure_client_latency(client: Client) -> float:
    """Return latency for client.get_me (seconds)."""
    try:
        t0 = time.time()
        await client.get_me()
        return time.time() - t0
    except Exception:
        return 10.0

async def choose_best_client(prefer_dc: Optional[int] = None) -> Tuple[int, Client]:
    if not work_loads:
        raise RuntimeError("No clients loaded")
    sorted_idxs = sorted(work_loads.items(), key=lambda kv: kv[1])
    candidate_count = max(1, min(len(sorted_idxs), 3))
    candidates = [idx for idx, _ in sorted_idxs[:candidate_count]]
    # prefer clients with session_name containing dc tag if prefer_dc provided
    if prefer_dc is not None:
        pref = [i for i in candidates if f"dc{prefer_dc}" in getattr(multi_clients[i], "session_name", "")]
        if pref:
            candidates = pref + [c for c in candidates if c not in pref]
    coros = [measure_client_latency(multi_clients[i]) for i in candidates]
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
# Download state helpers (pause/resume/cancel)
# ----------------------------
async def _init_state(key: str, info: Dict[str, Any]):
    async with download_state_lock:
        download_states[key] = {
            "status": "queued",
            "progress": 0.0,
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
        }
        download_states[key]["pause_event"].set()

async def pause_download(key: str) -> bool:
    async with download_state_lock:
        st = download_states.get(key)
        if not st:
            return False
        st["pause_event"].clear()
        st["status"] = "paused"
        st["last_update"] = time.time()
        return True

async def resume_download(key: str) -> bool:
    async with download_state_lock:
        st = download_states.get(key)
        if not st:
            return False
        st["pause_event"].set()
        st["status"] = "running"
        st["last_update"] = time.time()
        return True

async def cancel_download(key: str) -> bool:
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
    s = {k: (v if k not in ("pause_event", "message") else None) for k, v in st.items()}
    return s

# ----------------------------
# Re-fetch message helper
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
# Core adaptive chunked downloader (returns final path)
# ----------------------------
async def _adaptive_chunked_download(client: Client, message: Message,
                                     *,
                                     key: Optional[str] = None,
                                     chunk_size: int = CHUNK_SIZE,
                                     timeout: int = TIMEOUT,
                                     max_retries: int = MAX_RETRIES,
                                     save_dir: str = SAVE_DIR,
                                     stall_timeout: int = STALL_TIMEOUT,
                                     use_in_memory_override: Optional[bool] = None) -> str:
    location_obj, file_size, suggested_name, detected_dc = extract_location_from_message(message)
    raw_name = suggested_name or getattr(getattr(message, "document", None) or getattr(message, "video", None) or getattr(message, "audio", None) or getattr(message, "photo", None), "file_name", None) or f"file_{secrets.token_hex(4)}"
    filename = sanitize_filename(raw_name)
    os.makedirs(save_dir, exist_ok=True)
    final_path = os.path.join(save_dir, filename)
    part_path = final_path + ".part"

    if not key:
        key = key_for_message(message)
    await _init_state(key, {"size": file_size, "filename": filename, "message": message, "dc": detected_dc})
    state = download_states[key]
    state["status"] = "running"
    state["start_time"] = time.time()
    state["size"] = file_size
    state["filename"] = filename
    state["dc"] = detected_dc

    offset = 0
    if os.path.exists(part_path):
        offset = os.path.getsize(part_path)
    if os.path.exists(final_path) and file_size and os.path.getsize(final_path) >= file_size:
        state["status"] = "finished"
        state["downloaded"] = os.path.getsize(final_path)
        state["progress"] = 100.0
        state["end_time"] = time.time()
        return final_path

    auto_in_memory = USE_IN_MEMORY_DEFAULT if use_in_memory_override is None else use_in_memory_override
    mem_pct = get_system_memory_percent()
    if use_in_memory_override is None:
        if mem_pct < mem_pressure_threshold() and file_size and file_size < (200 * 1024 * 1024):
            auto_in_memory = True
        elif mem_pct >= mem_pressure_threshold():
            auto_in_memory = False

    speed_history: List[float] = []
    retries = 0
    consecutive_empty = 0
    last_progress_time = time.time()
    backoff = 1.0

    logger.info(f"📥 Download start key={key} file={filename} size={file_size/1024/1024:.2f}MB dc={detected_dc} offset={offset} in_memory={auto_in_memory}")

    # fallback if no location = download_media to disk
    if location_obj is None:
        tmp = part_path
        fallback_retries = 0
        while True:
            if state["cancel"]:
                state["status"] = "cancelled"
                raise asyncio.CancelledError("cancelled")
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
                return final_path
            except FloodWait as e:
                logger.warning(f"FloodWait fallback {e.value}s")
                await asyncio.sleep(e.value + 1)
            except asyncio.TimeoutError:
                fallback_retries += 1
                logger.warning(f"Fallback timeout {fallback_retries}/{max_retries}")
                if fallback_retries >= max_retries:
                    state["status"] = "failed"
                    raise Exception("fallback timeout")
                await asyncio.sleep(jitter_sleep(2,2))
            except Exception as e:
                fallback_retries += 1
                logger.warning(f"Fallback error: {e}")
                if fallback_retries >= max_retries:
                    state["status"] = "failed"
                    raise
                await asyncio.sleep(jitter_sleep(2,2))

    # main loop
    mode = "r+b" if os.path.exists(part_path) else "wb"
    with open(part_path, mode) as f:
        if os.path.exists(part_path):
            f.seek(offset)
            logger.info(f"Resuming part at {offset} bytes")
        consecutive_timeouts = 0
        last_refetch_time = time.time()
        REFRESH_INTERVAL = 60
        client_idx = None
        try:
            client_idx = multi_clients.index(client) if client in multi_clients else None
        except Exception:
            client_idx = None

        while True:
            if state["cancel"]:
                state["status"] = "cancelled"
                raise asyncio.CancelledError("cancelled")
            await state["pause_event"].wait()

            if file_size and offset >= file_size:
                break

            if time.time() - last_refetch_time > REFRESH_INTERVAL:
                try:
                    message, location_obj, file_size, suggested_name, detected_dc = await refetch_message_and_extract(client, message)
                    last_refetch_time = time.time()
                    logger.debug("refetched message to refresh file_reference")
                except Exception:
                    pass

            if time.time() - last_progress_time > stall_timeout:
                retries += 1
                logger.warning(f"Stall detected (no progress {stall_timeout}s) retry {retries}/{max_retries}")
                backoff = min(backoff * 2, 30)
                await asyncio.sleep(jitter_sleep(backoff, 3))
                last_progress_time = time.time()
                if retries >= max_retries:
                    # try rotating client
                    if client_idx is not None and len(multi_clients) > 1:
                        new_idx = (client_idx + 1) % len(multi_clients)
                        logger.warning(f"Rotating client due to stall {client_idx} -> {new_idx}")
                        client_idx = new_idx
                        client = multi_clients[client_idx]
                        try:
                            await client.start()
                        except Exception:
                            pass
                        retries = 0
                        backoff = 1.0
                        continue
                    state["status"] = "failed"
                    raise Exception("stalled too many times")
                continue

            try:
                start = time.time()
                if not auto_in_memory and location_obj is not None:
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
                    logger.warning(f"Empty chunk at {offset} (count {consecutive_empty})")
                    if consecutive_empty >= 2:
                        try:
                            message, location_obj, file_size, suggested_name, detected_dc = await refetch_message_and_extract(client, message)
                            logger.warning("re-fetched after empty chunk")
                        except Exception:
                            pass
                        consecutive_empty = 0
                    await asyncio.sleep(jitter_sleep(1,1))
                    if retries >= max_retries:
                        if client_idx is not None and len(multi_clients) > 1:
                            new_idx = (client_idx + 1) % len(multi_clients)
                            client_idx = new_idx
                            client = multi_clients[client_idx]
                            try:
                                await client.start()
                            except Exception:
                                pass
                            retries = 0
                            backoff = 1.0
                            continue
                        state["status"] = "failed"
                        raise Exception("repeated empty chunks")
                    continue

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

                elapsed = max(1e-6, time.time() - start)
                speed = (written / 1024 / 1024) / elapsed
                speed_history = state.get("speed_history", [])
                speed_history.append(speed)
                if len(speed_history) > 20:
                    speed_history.pop(0)
                avg_speed = sum(speed_history) / len(speed_history) if speed_history else speed

                state["downloaded"] = offset
                state["progress"] = (offset / file_size) * 100 if file_size else 0.0
                state["speed_history"] = speed_history[-10:]
                state["avg_speed"] = avg_speed
                state["retries"] = retries
                state["last_update"] = time.time()

                # memory-based switch
                mem_pct = get_system_memory_percent()
                if mem_pct >= mem_pressure_threshold():
                    if auto_in_memory:
                        auto_in_memory = False
                        logger.warning("Memory pressure -> switch to disk")
                else:
                    if avg_speed < 0.5 or retries >= 2:
                        if not auto_in_memory:
                            auto_in_memory = True
                            logger.warning("Low speed -> enabling in_memory")
                    elif avg_speed > 2.5 and retries == 0:
                        if auto_in_memory:
                            auto_in_memory = False
                            logger.info("Speed stable -> disabling in_memory")

            except asyncio.TimeoutError:
                retries += 1
                consecutive_timeouts += 1
                backoff = min(backoff * 2, 30)
                logger.warning(f"Chunk timeout at {offset}. retry {retries}/{max_retries}")
                await asyncio.sleep(jitter_sleep(backoff, 2))
                if consecutive_timeouts >= 2 and client_idx is not None and len(multi_clients) > 1:
                    new_idx = (client_idx + 1) % len(multi_clients)
                    logger.warning(f"Rotating client after repeated timeouts {client_idx} -> {new_idx}")
                    client_idx = new_idx
                    client = multi_clients[client_idx]
                    try:
                        await client.start()
                    except Exception:
                        pass
                    try:
                        message, location_obj, file_size, suggested_name, detected_dc = await refetch_message_and_extract(client, message)
                    except Exception:
                        pass
                    consecutive_timeouts = 0
                    retries = 0
                    continue
                if retries >= max_retries:
                    if not auto_in_memory:
                        logger.warning("Final fallback to in_memory full download")
                        auto_in_memory = True
                        continue
                    state["status"] = "failed"
                    raise Exception("chunk timeout too many times")
            except FloodWait as e:
                logger.warning(f"FloodWait {e.value}s")
                await asyncio.sleep(e.value + 1)
                continue
            except RPCError as e:
                err = str(e).upper()
                if "FILE_REFERENCE_EXPIRED" in err or "FILE_REFERENCE" in err:
                    logger.warning("FILE_REFERENCE_EXPIRED -> refetching message")
                    try:
                        message, location_obj, file_size, suggested_name, detected_dc = await refetch_message_and_extract(client, message)
                    except Exception:
                        pass
                    await asyncio.sleep(jitter_sleep(1.0, 1.5))
                    continue
                if "MIGRATE" in err or "PHONE_MIGRATE" in err or "NETWORK_MIGRATE" in err:
                    logger.warning(f"DC migrate error: {e}")
                    raise
                if "TIMEOUT" in err:
                    retries += 1
                    backoff = min(backoff * 2, 30)
                    await asyncio.sleep(jitter_sleep(backoff, 2))
                    if retries >= max_retries and client_idx is not None and len(multi_clients) > 1:
                        new_idx = (client_idx + 1) % len(multi_clients)
                        client_idx = new_idx
                        client = multi_clients[client_idx]
                        try:
                            await client.start()
                        except Exception:
                            pass
                        retries = 0
                        continue
                    continue
                retries += 1
                backoff = min(backoff * 2, 30)
                logger.warning(f"RPCError {e} retry {retries}/{max_retries}")
                if retries >= max_retries:
                    state["status"] = "failed"
                    raise
                await asyncio.sleep(jitter_sleep(backoff, 2))
                continue
            except Exception as e:
                retries += 1
                backoff = min(backoff * 2, 30)
                logger.error(f"Unknown error: {e}")
                traceback.print_exc()
                if retries >= max_retries:
                    if client_idx is not None and len(multi_clients) > 1:
                        new_idx = (client_idx + 1) % len(multi_clients)
                        client_idx = new_idx
                        client = multi_clients[client_idx]
                        try:
                            await client.start()
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
            logger.error(f"Finalize failed: {e}")
            state["status"] = "failed"
            raise
    state["status"] = "finished"
    state["end_time"] = time.time()
    state["downloaded"] = os.path.getsize(final_path) if os.path.exists(final_path) else offset
    state["progress"] = 100.0
    logger.info(f"Download finished key={key} path={final_path}")
    return final_path

# ----------------------------
# DC-aware wrapper: try recreate client on MIGRATE (best-effort)
# ----------------------------
async def _download_with_dc_handling(client_index: int, client: Client, message: Message, **kwargs) -> str:
    attempts = 0
    while True:
        try:
            return await _adaptive_chunked_download(client, message, **kwargs)
        except RPCError as e:
            s = str(e).upper()
            if "MIGRATE" in s or "PHONE_MIGRATE" in s or "NETWORK_MIGRATE" in s:
                # parse dc
                dc_id = getattr(e, "value", None) or getattr(e, "new_dc", None) or getattr(e, "dc_id", None)
                try:
                    import re
                    m = re.search(r"MIGRATE_(\d+)", str(e), re.IGNORECASE)
                    if m:
                        dc_id = int(m.group(1))
                except Exception:
                    pass
                logger.warning(f"DC migrate requested -> {dc_id}. Attempting recreate client")
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
                        raise Exception("Missing credentials")
                    await new_client.start()
                    multi_clients[client_index] = new_client
                    client = new_client
                    attempts += 1
                    if attempts >= MAX_RETRIES:
                        raise Exception("Too many DC recreate attempts")
                    logger.info(f"Recreated client idx={client_index} for dc {dc_id}")
                    continue
                except Exception as ex:
                    logger.error(f"Failed recreate client for dc {dc_id}: {ex}")
                    raise
            else:
                raise

# ----------------------------
# Public wrapper: choose best client and run
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
        raise RuntimeError("No clients")
    return min(work_loads, key=lambda k: work_loads[k])

async def download_file(message: Message, *,
                        chunk_size: Optional[int] = None,
                        timeout: Optional[int] = None,
                        max_retries: Optional[int] = None,
                        save_dir: Optional[str] = None,
                        stall_timeout: Optional[int] = None,
                        use_in_memory_override: Optional[bool] = None,
                        prefer_dc: Optional[int] = None) -> str:
    chunk_size = chunk_size or CHUNK_SIZE
    timeout = timeout or TIMEOUT
    max_retries = max_retries or MAX_RETRIES
    save_dir = save_dir or SAVE_DIR
    stall_timeout = stall_timeout or STALL_TIMEOUT

    try:
        if prefer_dc is not None:
            idx, client = await choose_best_client(prefer_dc=prefer_dc)
        else:
            idx, client = await choose_best_client(prefer_dc=None)
    except Exception:
        idx = _get_optimal_client_index()
        client = multi_clients[idx]

    async with _track_workload_idx(idx):
        async with DOWNLOAD_SEMAPHORE:
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
                res = await _download_with_dc_handling(idx, client, message,
                                                      key=key,
                                                      chunk_size=chunk_size,
                                                      timeout=timeout,
                                                      max_retries=max_retries,
                                                      save_dir=save_dir,
                                                      stall_timeout=stall_timeout,
                                                      use_in_memory_override=use_in_memory_override)
                return res
            except Exception as e:
                logger.error(f"download_file failed for key={key}: {e}")
                raise

# ----------------------------
# Streaming helper: stream while downloading (for aiohttp request)
# ----------------------------
async def stream_and_download(message: Message, request: web.Request, *,
                              chunk_size: Optional[int] = None,
                              timeout: Optional[int] = None,
                              save_dir: Optional[str] = None,
                              prefer_dc: Optional[int] = None) -> web.StreamResponse:
    """
    Stream file to aiohttp request while downloading to disk.
    Use this in your stream_routes: await stream_and_download(msg, request)
    """
    if web is None:
        raise RuntimeError("aiohttp not installed")

    chunk_size = chunk_size or CHUNK_SIZE
    timeout = timeout or TIMEOUT
    save_dir = save_dir or SAVE_DIR

    key = key_for_message(message)
    # choose client
    try:
        if prefer_dc is not None:
            idx, client = await choose_best_client(prefer_dc=prefer_dc)
        else:
            idx, client = await choose_best_client(prefer_dc=None)
    except Exception:
        idx = _get_optimal_client_index()
        client = multi_clients[idx]

    # start download in background and concurrently stream available bytes
    download_task = asyncio.create_task(download_file(message,
                                                      chunk_size=chunk_size,
                                                      timeout=timeout,
                                                      save_dir=save_dir,
                                                      prefer_dc=prefer_dc))
    # prepare response headers (use content-disposition)
    _, file_size, suggested_name, _ = extract_location_from_message(message)
    orig_name = suggested_name or f"file_{secrets.token_hex(4)}"
    filename = sanitize_filename(orig_name)
    resp = web.StreamResponse(status=200, headers={
        "Content-Type": "application/octet-stream",
        "Content-Disposition": f"attachment; filename*=UTF-8''{filename}",
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    })
    await resp.prepare(request)

    # stream loop: read from .part as it grows, send to client
    final_path = os.path.join(save_dir, filename)
    part_path = final_path + ".part"

    sent = 0
    last_sent = 0
    heartbeat_interval = 15  # seconds, write a tiny heartbeat if nothing sent to keep connection alive
    last_activity = time.time()

    try:
        while True:
            # if client canceled (browser closed) stop streaming
            if request.transport is None or request.transport.is_closing():
                logger.info("Client closed connection while streaming")
                break

            # if file exists final -> stream remaining bytes and finish
            if os.path.exists(final_path):
                size = os.path.getsize(final_path)
                # stream any remaining
                with open(final_path, "rb") as rf:
                    rf.seek(sent)
                    while True:
                        data = rf.read(chunk_size)
                        if not data:
                            break
                        await resp.write(data)
                        sent += len(data)
                        last_activity = time.time()
                await resp.write_eof()
                return resp

            # else if part exists -> stream new bytes
            if os.path.exists(part_path):
                current_size = os.path.getsize(part_path)
                if current_size > sent:
                    with open(part_path, "rb") as rf:
                        rf.seek(sent)
                        to_read = current_size - sent
                        while to_read > 0:
                            read_bytes = rf.read(min(chunk_size, to_read))
                            if not read_bytes:
                                break
                            await resp.write(read_bytes)
                            sent += len(read_bytes)
                            to_read -= len(read_bytes)
                            last_activity = time.time()
                    # small sleep to yield
                    await asyncio.sleep(0)
                    continue

            # if download finished or failed check task
            if download_task.done():
                # if it succeeded, loop will catch final_path existence above and stream. If failed, raise
                exc = download_task.exception()
                if exc:
                    logger.error(f"Background download failed: {exc}")
                    try:
                        await resp.write_eof()
                    except Exception:
                        pass
                    raise exc
                # else loop will handle final_path
                await asyncio.sleep(0.1)
                continue

            # heartbeat: send a tiny keepalive comment every heartbeat_interval seconds to prevent idle timeout
            if time.time() - last_activity > heartbeat_interval:
                try:
                    # send small whitespace chunk as keepalive - some clients may ignore; it's okay
                    await resp.write(b"")
                except Exception:
                    # some servers/clients reject empty writes; just skip
                    pass
                last_activity = time.time()

            await asyncio.sleep(0.5)

    finally:
        # if streaming stopped and download still running, we let it continue on server to finish saving.
        if not download_task.done():
            logger.info("Streaming ended but background download continues to save file on server")
        # do not cancel download_task automatically; server may need file persisted
    return resp

# ----------------------------
# CLI quick-run
# ----------------------------
if __name__ == "__main__":
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(description="Thunder safe_download hybrid CLI")
    parser.add_argument("--chat", type=int, help="chat_id", required=False)
    parser.add_argument("--msg", type=int, help="message_id", required=False)
    parser.add_argument("--output", type=str, default=SAVE_DIR, help="download directory")
    parser.add_argument("--chunk", type=int, default=CHUNK_SIZE, help="chunk size")
    parser.add_argument("--timeout", type=int, default=TIMEOUT, help="per-chunk timeout")
    parser.add_argument("--retries", type=int, default=MAX_RETRIES, help="max retries")
    parser.add_argument("--in-memory", choices=["auto", "true", "false"], default="auto", help="override in_memory")
    args = parser.parse_args()

    use_override = None
    if args.in_memory == "true":
        use_override = True
    elif args.in_memory == "false":
        use_override = False

    async def _run():
        if not multi_clients:
            logger.error("No multi_clients available. Exiting.")
            return
        client = multi_clients[0]
        await client.start()
        if args.chat is None or args.msg is None:
            logger.info("No chat/msg provided; exiting.")
            await client.stop()
            return
        msg = await client.get_messages(args.chat, args.msg)
        if not msg:
            logger.error("Message not found")
            await client.stop()
            return
        try:
            path = await download_file(msg, chunk_size=args.chunk, timeout=args.timeout, max_retries=args.retries, save_dir=args.output, use_in_memory_override=use_override)
            logger.info(f"Downloaded to {path}")
        except Exception as e:
            logger.error(f"Download failed: {e}")
        await client.stop()

    asyncio.run(_run())
