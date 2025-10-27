#!/usr/bin/env python3
"""
Thunder/safe_download.py - Hybrid Fix Version (v2) + Debug-Enhanced

Features:
- Hybrid high-speed chunking (like original fast version)
- Robust resume using .part files and offset metadata
- Adaptive in_memory switching (psutil-based + speed-based)
- Smart DC-aware client selection + client rotation on failures
- DC recreate best-effort for MIGRATE errors (no unsupported kwargs)
- Heartbeat and small keepalive writes when streaming to reduce 499
- Stream-to-browser while saving to disk (stream_and_download)
- download_file(message) returns final saved path
- Detailed debug logging & crash context dump for postmortem
- Pause / Resume / Cancel API
- CLI test runner and optional websocket progress server (debug)
- Designed for Heroku/Koyeb free-tier constraints (low-memory, short timeouts)
"""

from __future__ import annotations

import asyncio
import os
import time
import math
import random
import logging
import traceback
import secrets
import json
import sys
import signal
from typing import Optional, Any, Dict, List, Tuple
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError
from pyrogram.raw.functions.upload import GetFile
from pyrogram.raw.types import InputDocumentFileLocation, InputPhotoFileLocation
from pyrogram.types import Message

# optional extras
try:
    import psutil
except Exception:
    psutil = None

# aiohttp optional for streaming + WS debug server
try:
    from aiohttp import web, WSCloseCode
except Exception:
    web = None
    WSCloseCode = None

load_dotenv("config.env")

# Import existing multi_clients and work_loads from Thunder.bot
try:
    from Thunder.bot import multi_clients, work_loads
except Exception:
    multi_clients = []
    work_loads = {}

# Import Var for env config
try:
    from Thunder.vars import Var
except Exception:
    class Var:
        DOWNLOAD_DIR = None
        CHUNK_SIZE = 1024 * 1024
        TIMEOUT = 30
        MAX_RETRIES = 6
        MAX_CONCURRENT_DOWNLOADS = 5
        STALL_TIMEOUT = 60
        DEFAULT_IN_MEMORY = False
        WORKDIR = "/tmp"

# -------------------------
# Logging & Debugging Setup
# -------------------------
logger = logging.getLogger("Thunder.safe_download.v2")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(os.getenv("THUNDER_LOG_LEVEL", "INFO"))

# Create a debug file logger if requested
DEBUG_LOG_FILE = os.getenv("DEBUG_LOG_FILE", "")
if DEBUG_LOG_FILE:
    fh = logging.FileHandler(DEBUG_LOG_FILE)
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(fh)

# -------------------------
# Configuration
# -------------------------
CHUNK_SIZE = int(getattr(Var, "CHUNK_SIZE", int(os.getenv("CHUNK_SIZE", 1024 * 1024))))  # default 1MB
TIMEOUT = int(getattr(Var, "TIMEOUT", int(os.getenv("TIMEOUT", 30))))
MAX_RETRIES = int(getattr(Var, "MAX_RETRIES", int(os.getenv("MAX_RETRIES", 6))))
MAX_CONCURRENT_DOWNLOADS = int(getattr(Var, "MAX_CONCURRENT_DOWNLOADS", int(os.getenv("MAX_CONCURRENT_DOWNLOADS", 5))))
STALL_TIMEOUT = int(getattr(Var, "STALL_TIMEOUT", int(os.getenv("STALL_TIMEOUT", 60))))
DEFAULT_SAVE_DIR = getattr(Var, "DOWNLOAD_DIR", None) or ("/mnt/data/files" if os.path.exists("/mnt/data/files") else os.path.join(os.getcwd(), "downloads"))
SAVE_DIR = os.getenv("SAVE_DIR", DEFAULT_SAVE_DIR)
USE_IN_MEMORY_DEFAULT = bool(getattr(Var, "DEFAULT_IN_MEMORY", os.getenv("DEFAULT_IN_MEMORY", "False").lower() in ("1", "true", "yes")))
MEM_PRESSURE_PCT = float(os.getenv("MEM_PRESSURE_PCT", "80.0"))
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "15"))  # keepalive writes when streaming
REFRESH_FILE_REF_INTERVAL = int(os.getenv("REFRESH_FILE_REF_INTERVAL", "60"))  # seconds

# Ensure SAVE_DIR exists
os.makedirs(SAVE_DIR, exist_ok=True)

# Semaphore
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

# -------------------------
# Runtime state
# -------------------------
download_states: Dict[str, Dict[str, Any]] = {}
download_state_lock = asyncio.Lock()
ws_clients: List[Any] = []

# -------------------------
# Helper utilities
# -------------------------
def sanitize_filename(name: Optional[str]) -> str:
    if not name:
        return f"file_{secrets.token_hex(6)}"
    safe = "".join(c for c in name if c.isalnum() or c in (" ", ".", "_", "-", "(", ")")).strip()
    return safe or f"file_{secrets.token_hex(6)}"

def key_for_message(message: Message) -> str:
    try:
        chat = getattr(message, "chat", None)
        chat_id = chat.id if chat else getattr(message, "chat_id", None)
        mid = getattr(message, "message_id", None) or getattr(message, "id", None)
        return f"{chat_id}:{mid}" if chat_id is not None and mid is not None else f"{mid or secrets.token_hex(6)}"
    except Exception:
        return secrets.token_hex(8)

def jitter_sleep(base: float = 1.0, jitter: float = 1.0) -> float:
    return base + random.random() * jitter

def get_system_memory_percent() -> float:
    if psutil:
        try:
            return psutil.virtual_memory().percent
        except Exception:
            return 0.0
    return 0.0

# -------------------------
# Raw GetFile & location extraction
# -------------------------
async def raw_getfile_chunk(client: Client, location_obj: Any, offset: int, limit: int) -> bytes:
    """Invoke raw.upload.GetFile and return bytes (best-effort)."""
    try:
        req = GetFile(location=location_obj, offset=offset, limit=limit)
        res = await client.invoke(req)
        data = getattr(res, "bytes", None) or getattr(res, "file_bytes", None)
        if data is None:
            file_attr = getattr(res, "file", None)
            if file_attr is not None and hasattr(file_attr, "bytes"):
                data = getattr(file_attr, "bytes")
        return data or b""
    except Exception as e:
        logger.debug(f"raw_getfile_chunk exception: {e}")
        raise

def extract_location_from_message(message: Message) -> Tuple[Optional[Any], int, Optional[str], Optional[int]]:
    """Try to craft InputDocumentFileLocation/InputPhotoFileLocation from message internals."""
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

# -------------------------
# Smart client selection (least-loaded + latency)
# -------------------------
async def measure_client_latency(client: Client, samples: int = 1) -> float:
    latencies = []
    for _ in range(samples):
        try:
            t0 = time.time()
            await client.get_me()
            latencies.append(time.time() - t0)
        except Exception:
            latencies.append(10.0)
    return sum(latencies) / len(latencies) if latencies else 10.0

async def choose_best_client(prefer_dc: Optional[int] = None) -> Tuple[int, Client]:
    if not work_loads:
        raise RuntimeError("No clients loaded")
    sorted_idxs = sorted(work_loads.items(), key=lambda kv: kv[1])
    candidate_count = max(1, min(len(sorted_idxs), 3))
    candidates = [idx for idx, _ in sorted_idxs[:candidate_count]]
    if prefer_dc is not None:
        pref_matches = [i for i in candidates if f"dc{prefer_dc}" in getattr(multi_clients[i], "session_name", "")]
        if pref_matches:
            candidates = pref_matches + [c for c in candidates if c not in pref_matches]
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

# -------------------------
# Download state APIs (pause/resume/cancel/status)
# -------------------------
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
            "debug": [],
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
        logger.debug(f"pause_download: {key}")
        return True

async def resume_download(key: str) -> bool:
    async with download_state_lock:
        st = download_states.get(key)
        if not st:
            return False
        st["pause_event"].set()
        st["status"] = "running"
        st["last_update"] = time.time()
        logger.debug(f"resume_download: {key}")
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
        logger.debug(f"cancel_download: {key}")
        return True

def get_state_snapshot(key: str) -> Dict[str, Any]:
    st = download_states.get(key)
    if not st:
        return {}
    s = {k: (v if k not in ("pause_event", "message") else None) for k, v in st.items()}
    return s

# -------------------------
# Re-fetch helper for FILE_REFERENCE_EXPIRED
# -------------------------
async def refetch_message_and_extract(client: Client, message: Message) -> Tuple[Message, Optional[Any], int, Optional[str], Optional[int]]:
    try:
        chat_id = getattr(message, "chat", None).id if getattr(message, "chat", None) else getattr(message, "chat_id", None)
        msg_id = getattr(message, "message_id", None) or getattr(message, "id", None)
        if chat_id is None or msg_id is None:
            return message, *extract_location_from_message(message)
        fresh = await client.get_messages(chat_id, msg_id)
        if not fresh:
            return message, *extract_location_from_message(message)
        return fresh, *extract_location_from_message(fresh)
    except Exception as e:
        logger.debug(f"refetch_message_and_extract error: {e}")
        return message, *extract_location_from_message(message)

# -------------------------
# Core adaptive chunked download (v2): high-speed + stability
# -------------------------
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
    use_in_memory_override: Optional[bool] = None,
    prefer_dc: Optional[int] = None,
) -> str:
    """
    Returns final file path after completing download.
    Implements high-speed chunking (raw_getfile when possible) and robust resume.
    """
    location_obj, file_size, suggested_name, detected_dc = extract_location_from_message(message)
    raw_name = suggested_name or getattr(getattr(message, "document", None) or getattr(message, "video", None) or getattr(message, "audio", None) or getattr(message, "photo", None), "file_name", None) or f"file_{secrets.token_hex(4)}"
    filename = sanitize_filename(raw_name)
    os.makedirs(save_dir, exist_ok=True)
    final_path = os.path.join(save_dir, filename)
    part_path = final_path + ".part"
    meta_path = final_path + ".meta.json"  # to store offset/last-update etc

    if not key:
        key = key_for_message(message)

    # init state
    await _init_state(key, {"size": file_size, "filename": filename, "message": message, "dc": detected_dc})
    state = download_states[key]
    state["status"] = "running"
    state["start_time"] = time.time()
    state["size"] = file_size
    state["filename"] = filename
    state["dc"] = detected_dc

    # resume offset from part file or meta
    offset = 0
    if os.path.exists(part_path):
        try:
            offset = os.path.getsize(part_path)
        except Exception:
            offset = 0
    else:
        # check meta if exists (safer)
        if os.path.exists(meta_path):
            try:
                with open(meta_path, "r") as mf:
                    md = json.load(mf)
                    offset = int(md.get("offset", 0))
            except Exception:
                offset = 0

    # if final exists and seems complete, return immediately
    if os.path.exists(final_path) and file_size and os.path.getsize(final_path) >= file_size:
        state["status"] = "finished"
        state["downloaded"] = os.path.getsize(final_path)
        state["progress"] = 100.0
        state["end_time"] = time.time()
        logger.info(f"Already fully downloaded: {final_path}")
        return final_path

    # decide initial in_memory
    auto_in_memory = USE_IN_MEMORY_DEFAULT if use_in_memory_override is None else use_in_memory_override
    system_mem = get_system_memory_percent()
    if use_in_memory_override is None:
        if system_mem < (MEM_PRESSURE_PCT - 10) and file_size and file_size < (200 * 1024 * 1024):
            auto_in_memory = True
        elif system_mem >= MEM_PRESSURE_PCT:
            auto_in_memory = False

    # speed & retry trackers
    speed_history: List[float] = []
    retries = 0
    consecutive_empty = 0
    last_progress_time = time.time()
    backoff = 1.0
    consecutive_timeouts = 0
    last_meta_write = time.time()
    REFRESH_INTERVAL = REFRESH_FILE_REF_INTERVAL

    logger.info(f"START key={key} file={filename} size={file_size/1024/1024:.2f}MB dc={detected_dc} offset={offset} in_memory_start={auto_in_memory}")
    state["debug"].append({"t": time.time(), "evt": "start", "offset": offset, "in_memory": auto_in_memory})

    # if location is None -> fallback directly to download_media(in_memory=False) with resume unsupported
    if location_obj is None:
        logger.debug("No raw location - using fallback download_media(disk)")
        tmp = part_path
        fallback_attempts = 0
        while True:
            if state["cancel"]:
                state["status"] = "cancelled"; raise asyncio.CancelledError("cancelled")
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
                logger.info(f"Fallback download finished: {final_path}")
                return final_path
            except FloodWait as e:
                logger.warning(f"FloodWait during fallback: {e.value}s")
                await asyncio.sleep(e.value + 1)
            except asyncio.TimeoutError:
                fallback_attempts += 1
                logger.warning(f"Fallback timeout {fallback_attempts}/{max_retries}")
                if fallback_attempts >= max_retries:
                    state["status"] = "failed"
                    raise Exception("Fallback timed out repeatedly")
                await asyncio.sleep(jitter_sleep(2,2))
            except Exception as e:
                fallback_attempts += 1
                logger.warning(f"Fallback error: {e} (attempt {fallback_attempts})")
                if fallback_attempts >= max_retries:
                    state["status"] = "failed"; raise
                await asyncio.sleep(jitter_sleep(2,2))

    # determine client_idx if available
    client_idx = None
    try:
        client_idx = multi_clients.index(client) if client in multi_clients else None
    except Exception:
        client_idx = None

    # open part file for append/resume
    mode = "r+b" if os.path.exists(part_path) else "wb"
    with open(part_path, mode) as f:
        if os.path.exists(part_path):
            f.seek(offset)
            logger.info(f"Resuming from part at {offset} bytes")
        last_refetch_time = time.time()

        while True:
            # pause/cancel checks
            if state["cancel"]:
                state["status"] = "cancelled"
                logger.info(f"Download cancelled key={key}")
                raise asyncio.CancelledError("cancelled")
            await state["pause_event"].wait()

            # done?
            if file_size and offset >= file_size:
                break

            # periodic file_reference refresh
            if time.time() - last_refetch_time > REFRESH_INTERVAL:
                try:
                    message, location_obj, file_size, suggested_name, detected_dc = await refetch_message_and_extract(client, message)
                    last_refetch_time = time.time()
                    logger.debug("Periodic refetch of message to refresh file_reference")
                except Exception as e:
                    logger.debug(f"Periodic refetch failed: {e}")

            # stall detection
            if time.time() - last_progress_time > stall_timeout:
                retries += 1
                logger.warning(f"Stall detected (no progress in {stall_timeout}s) retry {retries}/{max_retries}")
                backoff = min(backoff * 2, 30)
                await asyncio.sleep(jitter_sleep(backoff, 3))
                last_progress_time = time.time()
                if retries >= max_retries:
                    # rotate client if possible instead of abort
                    if client_idx is not None and len(multi_clients) > 1:
                        new_idx = (client_idx + 1) % len(multi_clients)
                        logger.warning(f"Rotating client due to stall: {client_idx} -> {new_idx}")
                        client_idx = new_idx
                        client = multi_clients[client_idx]
                        try:
                            await client.start()
                        except Exception:
                            logger.debug("client.start after rotate failed")
                        retries = 0
                        backoff = 1.0
                        continue
                    state["status"] = "failed"
                    raise Exception("Download stalled too many times")
                continue

            try:
                t0 = time.time()
                # choose raw or in_memory method
                if not auto_in_memory and location_obj is not None:
                    # raw chunk via GetFile
                    chunk = await asyncio.wait_for(raw_getfile_chunk(client, location_obj, offset, chunk_size), timeout=timeout)
                else:
                    # download in-memory full (or partial) and extract bytes
                    memobj = await asyncio.wait_for(client.download_media(message, in_memory=True), timeout=timeout)
                    if hasattr(memobj, "getbuffer"):
                        chunk = memobj.getbuffer().tobytes()
                    else:
                        chunk = memobj or b""

                if not chunk or len(chunk) == 0:
                    consecutive_empty += 1
                    retries += 1
                    logger.warning(f"Empty chunk at offset {offset} count={consecutive_empty} retries={retries}")
                    state["debug"].append({"t": time.time(), "evt": "empty_chunk", "offset": offset, "count": consecutive_empty})
                    if consecutive_empty >= 2:
                        try:
                            message, location_obj, file_size, suggested_name, detected_dc = await refetch_message_and_extract(client, message)
                            logger.warning("Refetched message after empty chunk")
                        except Exception:
                            logger.debug("Refetch after empty chunk failed")
                        consecutive_empty = 0
                    await asyncio.sleep(jitter_sleep(1,1))
                    if retries >= max_retries:
                        if client_idx is not None and len(multi_clients) > 1:
                            new_idx = (client_idx + 1) % len(multi_clients)
                            logger.warning(f"Rotating client due to repeated empty chunks: {client_idx} -> {new_idx}")
                            client_idx = new_idx
                            client = multi_clients[client_idx]
                            try:
                                await client.start()
                            except Exception:
                                logger.debug("rotate start failed")
                            retries = 0; backoff = 1.0
                            continue
                        state["status"] = "failed"
                        raise Exception("Repeated empty chunks")
                    continue

                # write chunk
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

                elapsed = max(1e-6, time.time() - t0)
                speed = (written / 1024 / 1024) / elapsed
                speed_history.append(speed)
                if len(speed_history) > 30:
                    speed_history.pop(0)
                avg_speed = sum(speed_history) / len(speed_history) if speed_history else speed

                # update state and meta (write meta every 2s)
                state["downloaded"] = offset
                state["progress"] = (offset / file_size) * 100 if file_size else 0.0
                state["speed_history"] = speed_history[-10:]
                state["avg_speed"] = avg_speed
                state["retries"] = retries
                state["last_update"] = time.time()
                state["debug"].append({"t": time.time(), "evt": "chunk_written", "offset": offset, "written": written, "speed": speed})
                logger.info(f"⬇️ {state['progress']:.2f}% | {speed:.2f} MB/s | offset={offset/1024/1024:.2f}MB | in_memory={auto_in_memory}")

                # meta write
                if time.time() - last_meta_write > 2:
                    try:
                        with open(meta_path, "w") as mf:
                            json.dump({"offset": offset, "last_update": time.time(), "in_memory": auto_in_memory}, mf)
                    except Exception:
                        pass
                    last_meta_write = time.time()

                # adaptive in_memory switching: mem-based & speed-based
                mem_pct = get_system_memory_percent()
                if mem_pct >= MEM_PRESSURE_PCT:
                    if auto_in_memory:
                        auto_in_memory = False
                        logger.warning("Memory pressure detected -> switching to disk mode")
                        state["debug"].append({"t": time.time(), "evt": "mem_switch_off", "mem_pct": mem_pct})
                else:
                    if avg_speed < 0.5 or retries >= 2:
                        if not auto_in_memory:
                            auto_in_memory = True
                            logger.warning("Low speed/retries -> enabling in_memory")
                            state["debug"].append({"t": time.time(), "evt": "enable_in_memory", "avg_speed": avg_speed})
                    elif avg_speed > 3.0 and retries == 0:
                        if auto_in_memory:
                            auto_in_memory = False
                            logger.info("Speed stable -> disabling in_memory")
                            state["debug"].append({"t": time.time(), "evt": "disable_in_memory", "avg_speed": avg_speed})

            except asyncio.TimeoutError:
                retries += 1
                consecutive_timeouts += 1
                backoff = min(backoff * 2, 30)
                logger.warning(f"Chunk timeout at offset {offset}. retry {retries}/{max_retries}. backoff {backoff}s")
                state["debug"].append({"t": time.time(), "evt": "timeout", "offset": offset, "retries": retries})
                await asyncio.sleep(jitter_sleep(backoff, 2))
                if consecutive_timeouts >= 2 and client_idx is not None and len(multi_clients) > 1:
                    new_idx = (client_idx + 1) % len(multi_clients)
                    logger.warning(f"Rotating client after repeated timeouts {client_idx} -> {new_idx}")
                    client_idx = new_idx
                    client = multi_clients[client_idx]
                    try:
                        await client.start()
                    except Exception:
                        logger.debug("rotate start failed after timeouts")
                    try:
                        message, location_obj, file_size, suggested_name, detected_dc = await refetch_message_and_extract(client, message)
                    except Exception:
                        pass
                    consecutive_timeouts = 0
                    retries = 0
                    continue
                if retries >= max_retries:
                    if not auto_in_memory:
                        logger.warning("Final fallback: switching to in_memory for recovery")
                        auto_in_memory = True
                        continue
                    state["status"] = "failed"
                    raise Exception("Chunk timeout: too many retries")
            except FloodWait as e:
                logger.warning(f"FloodWait {e.value}s - sleeping")
                state["debug"].append({"t": time.time(), "evt": "floodwait", "value": e.value})
                await asyncio.sleep(e.value + 1)
                continue
            except RPCError as e:
                err = str(e).upper()
                logger.warning(f"RPCError: {e}")
                state["debug"].append({"t": time.time(), "evt": "rpc_error", "err": str(e)})
                if "FILE_REFERENCE_EXPIRED" in err or "FILE_REFERENCE" in err:
                    logger.warning("FILE_REFERENCE_EXPIRED detected - refetching message")
                    try:
                        message, location_obj, file_size, suggested_name, detected_dc = await refetch_message_and_extract(client, message)
                    except Exception:
                        logger.debug("Refetch failed after FILE_REFERENCE_EXPIRED")
                    await asyncio.sleep(jitter_sleep(0.5, 1.0))
                    continue
                if "MIGRATE" in err or "PHONE_MIGRATE" in err or "NETWORK_MIGRATE" in err:
                    logger.warning("DC migration RPCError - bubbling up for DC handling")
                    raise
                if "TIMEOUT" in err:
                    retries += 1
                    backoff = min(backoff * 2, 30)
                    await asyncio.sleep(jitter_sleep(backoff, 2))
                    if retries >= max_retries and client_idx is not None and len(multi_clients) > 1:
                        new_idx = (client_idx + 1) % len(multi_clients)
                        logger.warning(f"Rotating client after RPC TIMEOUTs {client_idx} -> {new_idx}")
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
                if retries >= max_retries:
                    state["status"] = "failed"
                    raise
                await asyncio.sleep(jitter_sleep(backoff, 2))
                continue
            except Exception as e:
                retries += 1
                backoff = min(backoff * 2, 30)
                logger.error(f"Unknown error during chunk: {e}")
                traceback.print_exc()
                state["debug"].append({"t": time.time(), "evt": "unknown_error", "err": str(e)})
                if retries >= max_retries:
                    if client_idx is not None and len(multi_clients) > 1:
                        new_idx = (client_idx + 1) % len(multi_clients)
                        logger.warning(f"Rotating client as last attempt: {client_idx} -> {new_idx}")
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

    # finalize atomic move
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
    logger.info(f"Finished download key={key} path={final_path}")
    # write final meta debug
    try:
        with open(final_path + ".meta.json", "w") as mf:
            json.dump({"finished": True, "end_time": state["end_time"], "debug": state.get("debug", [])[-50:]}, mf)
    except Exception:
        pass
    return final_path

# -------------------------
# DC-aware wrapper for MIGRATE errors (best-effort)
# -------------------------
async def _download_with_dc_handling(client_index: int, client: Client, message: Message, **kwargs):
    attempts = 0
    while True:
        try:
            return await _adaptive_chunked_download(client, message, **kwargs)
        except RPCError as e:
            s = str(e).upper()
            if "MIGRATE" in s or "PHONE_MIGRATE" in s or "NETWORK_MIGRATE" in s:
                logger.warning(f"DC migration requested by Telegram: {e}")
                # parse DC id
                dc_id = getattr(e, "value", None) or getattr(e, "new_dc", None) or getattr(e, "dc_id", None)
                try:
                    import re
                    m = re.search(r"MIGRATE_(\d+)", str(e), re.IGNORECASE)
                    if m:
                        dc_id = int(m.group(1))
                except Exception:
                    pass
                logger.info(f"Attempting to recreate client for DC {dc_id} (best-effort)")
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
                        raise Exception("Too many DC recreate attempts")
                    logger.info(f"Recreated client at index {client_index} for DC {dc_id}; retrying download")
                    continue
                except Exception as ex:
                    logger.error(f"Failed to recreate client for DC {dc_id}: {ex}")
                    raise
            else:
                raise

# -------------------------
# Public wrapper used by stream_routes.py
# -------------------------
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
            logger.info(f"Selected client idx={idx} session={getattr(client,'session_name', None)} for key={key}")
            try:
                res = await _download_with_dc_handling(idx, client, message,
                                                      key=key,
                                                      chunk_size=chunk_size,
                                                      timeout=timeout,
                                                      max_retries=max_retries,
                                                      save_dir=save_dir,
                                                      stall_timeout=stall_timeout,
                                                      use_in_memory_override=use_in_memory_override,
                                                      prefer_dc=prefer_dc)
                return res
            except Exception as e:
                logger.error(f"download_file failed for key={key}: {e}")
                raise

# -------------------------
# Stream while writing to disk: for aiohttp integration
# -------------------------
async def stream_and_download(message: Message, request: "web.Request", *,
                              chunk_size: Optional[int] = None,
                              timeout: Optional[int] = None,
                              save_dir: Optional[str] = None,
                              prefer_dc: Optional[int] = None) -> "web.StreamResponse":
    """
    Use this in stream_routes.py to both stream to browser and save to disk with resume.
    If browser closes (499), the background download will continue on server.
    """
    if web is None:
        raise RuntimeError("aiohttp not installed in environment")

    chunk_size = chunk_size or CHUNK_SIZE
    timeout = timeout or TIMEOUT
    save_dir = save_dir or SAVE_DIR

    key = key_for_message(message)
    try:
        if prefer_dc is not None:
            idx, client = await choose_best_client(prefer_dc=prefer_dc)
        else:
            idx, client = await choose_best_client(prefer_dc=None)
    except Exception:
        idx = _get_optimal_client_index()
        client = multi_clients[idx]

    # start background download task
    download_task = asyncio.create_task(download_file(message, chunk_size=chunk_size, timeout=timeout, save_dir=save_dir, prefer_dc=prefer_dc))

    # prepare HTTP headers (force attachment)
    _, file_size, suggested_name, _ = extract_location_from_message(message)
    filename = sanitize_filename(suggested_name or f"file_{secrets.token_hex(4)}")
    headers = {
        "Content-Type": "application/octet-stream",
        "Content-Disposition": f"attachment; filename*=UTF-8''{filename}",
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    }
    resp = web.StreamResponse(status=200, headers=headers)
    await resp.prepare(request)

    final_path = os.path.join(save_dir, filename)
    part_path = final_path + ".part"

    sent = 0
    last_activity = time.time()
    heartbeat = HEARTBEAT_INTERVAL

    try:
        while True:
            # if client closed connection
            if request.transport is None or request.transport.is_closing():
                logger.info(f"Client closed during streaming for key={key}")
                break

            # stream final file if exists
            if os.path.exists(final_path):
                size = os.path.getsize(final_path)
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

            # stream available bytes from .part
            if os.path.exists(part_path):
                cur = os.path.getsize(part_path)
                if cur > sent:
                    with open(part_path, "rb") as rf:
                        rf.seek(sent)
                        to_read = cur - sent
                        while to_read > 0:
                            block = rf.read(min(chunk_size, to_read))
                            if not block:
                                break
                            try:
                                await resp.write(block)
                            except Exception as e:
                                logger.debug(f"Stream write failed: {e}")
                                raise
                            sent += len(block)
                            to_read -= len(block)
                            last_activity = time.time()
                    await asyncio.sleep(0)
                    continue

            # check background download task
            if download_task.done():
                exc = download_task.exception()
                if exc:
                    logger.error(f"Background download error for key={key}: {exc}")
                    try:
                        await resp.write_eof()
                    except Exception:
                        pass
                    raise exc
                # else wait for final file to exist and loop will stream it
                await asyncio.sleep(0.2)
                continue

            # heartbeat if idle to keep connection open at load balancer
            if time.time() - last_activity > heartbeat:
                try:
                    # some servers don't like empty writes; sending small whitespace may still be ignored, but we try
                    await resp.write(b"")
                except Exception:
                    pass
                last_activity = time.time()

            await asyncio.sleep(0.4)

    finally:
        # don't cancel background download automatically; let it finish and save
        if not download_task.done():
            logger.info(f"Stream ended for key={key}. Background download continues to save file.")
        # respond ended
    return resp

# -------------------------
# Watchdog for clients
# -------------------------
async def _client_watchdog(client: Client, name: str, interval: int = 300):
    while True:
        await asyncio.sleep(interval)
        try:
            await client.get_me()
        except Exception:
            logger.warning(f"Watchdog: client {name} disconnected; attempting restart")
            try:
                await client.stop()
            except Exception:
                pass
            await asyncio.sleep(5)
            try:
                await client.start()
                logger.info(f"Watchdog: restarted client {name}")
            except Exception as e:
                logger.error(f"Watchdog restart failed for {name}: {e}")

async def start_watchdogs():
    for i, c in enumerate(multi_clients):
        asyncio.create_task(_client_watchdog(c, getattr(c, "session_name", f"client_{i}")))

# -------------------------
# Optional WS / HTTP debug server (very lightweight)
# -------------------------
def make_debug_app():
    if web is None:
        raise RuntimeError("aiohttp not installed")
    app = web.Application()
    routes = web.RouteTableDef()

    @routes.get("/ws")
    async def ws_handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        ws_clients.append(ws)
        logger.info("WS debug client connected")
        try:
            async for msg in ws:
                if msg.type == web.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        if data.get("action") == "list":
                            async with download_state_lock:
                                snapshot = {k: get_state_snapshot(k) for k in download_states.keys()}
                            await ws.send_str(json.dumps({"type": "list", "data": snapshot}))
                    except Exception:
                        await ws.send_str(json.dumps({"error": "invalid command"}))
                elif msg.type == web.WSMsgType.ERROR:
                    logger.warning(f"WS error: {ws.exception()}")
        finally:
            try:
                ws_clients.remove(ws)
            except Exception:
                pass
            logger.info("WS debug client disconnected")
        return ws

    @routes.get("/status/{key}")
    async def status_key(request):
        key = request.match_info["key"]
        async with download_state_lock:
            st = get_state_snapshot(key)
        if not st:
            raise web.HTTPNotFound(text=json.dumps({"error": "not found"}), content_type="application/json")
        return web.json_response(st)

    @routes.get("/status")
    async def status_all(request):
        async with download_state_lock:
            data = {k: get_state_snapshot(k) for k in download_states.keys()}
        return web.json_response(data)

    app.add_routes(routes)
    return app

# -------------------------
# CLI runner and main
# -------------------------
def _install_signal_handlers(loop):
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(_shutdown(loop)))
        loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(_shutdown(loop)))
    except Exception:
        pass

async def _shutdown(loop):
    logger.info("safe_download v2 shutting down...")
    # close ws
    if web and ws_clients:
        for ws in list(ws_clients):
            try:
                await ws.close(code=WSCloseCode.GOING_AWAY, message=b"shutting down")
            except Exception:
                pass
    await asyncio.sleep(0.1)
    loop.stop()

async def _cli_run(args):
    if not multi_clients:
        logger.error("No multi_clients configured. Exiting.")
        return
    # start first client
    client = multi_clients[0]
    await client.start()
    logger.info("Client started for CLI")

    if args.watchdogs:
        asyncio.create_task(start_watchdogs())

    if args.ws:
        if web is None:
            logger.error("aiohttp not installed; cannot start ws server")
        else:
            app = make_debug_app()
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("DEBUG_WS_PORT", "8765")))
            await site.start()
            logger.info(f"Debug WS server started at port {os.getenv('DEBUG_WS_PORT','8765')}")

    if args.chat is None or args.msg is None:
        logger.info("No chat/msg provided; running in server mode for WS/debug")
        await asyncio.get_event_loop().create_future()
        await client.stop()
        return

    msg = await client.get_messages(args.chat, args.msg)
    if not msg:
        logger.error("Message not found")
        await client.stop()
        return

    try:
        path = await download_file(msg, chunk_size=args.chunk, timeout=args.timeout, max_retries=args.retries, save_dir=args.output, use_in_memory_override=(True if args.in_memory == "true" else False if args.in_memory == "false" else None))
        logger.info(f"Downloaded to {path}")
    except Exception as e:
        logger.error(f"Download failed: {e}")
    await client.stop()

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Thunder safe_download v2 - Hybrid Fix + Debug")
    parser.add_argument("--chat", type=int, help="chat id")
    parser.add_argument("--msg", type=int, help="message id")
    parser.add_argument("--output", type=str, default=SAVE_DIR, help="download directory")
    parser.add_argument("--chunk", type=int, default=CHUNK_SIZE, help="chunk size bytes")
    parser.add_argument("--timeout", type=int, default=TIMEOUT, help="per-chunk timeout")
    parser.add_argument("--retries", type=int, default=MAX_RETRIES, help="max retries per chunk")
    parser.add_argument("--in-memory", choices=["auto", "true", "false"], default="auto", help="override in_memory")
    parser.add_argument("--ws", action="store_true", help="start debug websocket server (port 8765)")
    parser.add_argument("--watchdogs", action="store_true", help="start client watchdogs")
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
        logger.info("Interrupted")
    finally:
        pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
        for t in pending:
            t.cancel()
        try:
            loop.run_until_complete(asyncio.sleep(0.1))
        except Exception:
            pass

if __name__ == "__main__":
    main()
