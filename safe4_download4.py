#!/usr/bin/env python3
"""
Thunder/safe_download.py
Option 1 implementation (background fast downloader).
- Uses existing ByteStreamer (from your old repo)
- Writes to .part files and supports resume
- DC-aware client selection and client rotation on failure
- Adaptive in_memory switching based on psutil
- Async disk writes with aiofiles (if available)
- CLI runner for quick testing and optional debug HTTP status app
"""

from __future__ import annotations

import os
import sys
import time
import json
import math
import secrets
import random
import logging
import asyncio
import traceback
from contextlib import asynccontextmanager
from typing import Optional, Any, Dict, Tuple, List

from dotenv import load_dotenv

# pyrogram imports
from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import FloodWait, RPCError

# local imports (provided by your repo)
# ByteStreamer and get_file_ids are expected to exist under Thunder.utils or similar
# In your repo earlier they were in Adarsh.utils.custom_dl and Adarsh.utils.file_properties.
try:
    from Thunder.utils.custom_dl import ByteStreamer  # prefer new path
    from Thunder.utils.file_properties import get_file_ids
except Exception:
    # fallback to old locations used previously
    try:
        from Adarsh.utils.custom_dl import ByteStreamer  # old repo path
        from Adarsh.utils.file_properties import get_file_ids
    except Exception:
        ByteStreamer = None
        get_file_ids = None

# try aiofiles for non-blocking writes
try:
    import aiofiles
except Exception:
    aiofiles = None

# optional psutil for memory checks
try:
    import psutil
except Exception:
    psutil = None

# optional aiohttp for debug webserver
try:
    from aiohttp import web
except Exception:
    web = None

load_dotenv("config.env")

# import multi_clients and work_loads from your bot loader
try:
    from Thunder.bot import multi_clients, work_loads
except Exception:
    try:
        from Adarsh.bot import multi_clients, work_loads
    except Exception:
        multi_clients = []
        work_loads = {}

# import Var if available
try:
    from Thunder.vars import Var
except Exception:
    try:
        from Adarsh.vars import Var
    except Exception:
        class Var:
            SAVE_DIR = None
            CHUNK_SIZE = 4 * 1024 * 1024
            TIMEOUT = 30
            MAX_RETRIES = 6
            MAX_CONCURRENT_DOWNLOADS = 4
            STALL_TIMEOUT = 60
            DEFAULT_IN_MEMORY = False
            WORKDIR = "/tmp"
            CACHE_SIZE = 64

# -------------------------
# Logging
# -------------------------
logger = logging.getLogger("Thunder.safe_download")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
logger.setLevel(os.getenv("THUNDER_LOG_LEVEL", "INFO"))

# -------------------------
# Config (env overrides)
# -------------------------
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", getattr(Var, "CHUNK_SIZE", 4 * 1024 * 1024)))
TIMEOUT = int(os.getenv("TIMEOUT", getattr(Var, "TIMEOUT", 30)))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", getattr(Var, "MAX_RETRIES", 6)))
MAX_CONCURRENT_DOWNLOADS = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", getattr(Var, "MAX_CONCURRENT_DOWNLOADS", 4)))
STALL_TIMEOUT = int(os.getenv("STALL_TIMEOUT", getattr(Var, "STALL_TIMEOUT", 60)))
DEFAULT_SAVE_DIR = getattr(Var, "DOWNLOAD_DIR", None) or os.path.join(os.getcwd(), "downloads")
SAVE_DIR = os.getenv("SAVE_DIR", DEFAULT_SAVE_DIR)
USE_IN_MEMORY_DEFAULT = bool(str(os.getenv("DEFAULT_IN_MEMORY", str(getattr(Var, "DEFAULT_IN_MEMORY", False)))).lower() in ("1","true","yes"))
MEM_PRESSURE_PCT = float(os.getenv("MEM_PRESSURE_PCT", "80.0"))
REFRESH_FILE_REF_INTERVAL = int(os.getenv("REFRESH_FILE_REF_INTERVAL", "60"))

os.makedirs(SAVE_DIR, exist_ok=True)

DOWNLOAD_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
file_write_locks: Dict[str, asyncio.Lock] = {}
download_states: Dict[str, Dict[str, Any]] = {}
download_state_lock = asyncio.Lock()

# -------------------------
# Helpers
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
        return f"{chat_id}:{mid}" if chat_id is not None and mid is not None else f"{mid or secrets.token_hex(8)}"
    except Exception:
        return secrets.token_hex(8)

def get_system_memory_percent() -> float:
    if psutil:
        try:
            return psutil.virtual_memory().percent
        except Exception:
            return 0.0
    return 0.0

def jitter_sleep(base: float = 1.0, jitter: float = 1.0) -> float:
    return base + random.random() * jitter

# -------------------------
# ByteStreamer wrapper helpers
# -------------------------
async def get_streamer_for_client(client: Client) -> ByteStreamer:
    """Return ByteStreamer instance bound to passed client (cached on client)."""
    # store streamer on client to reuse sessions
    st = getattr(client, "_byte_streamer", None)
    if st is None:
        if ByteStreamer is None:
            raise RuntimeError("ByteStreamer not found in project. Provide Thunder.utils.custom_dl.ByteStreamer")
        st = ByteStreamer(client)
        setattr(client, "_byte_streamer", st)
    return st

# -------------------------
# Choose best client by load + latency
# -------------------------
async def measure_client_latency(client: Client, timeout_s: float = 5.0) -> float:
    try:
        t0 = time.time()
        await asyncio.wait_for(client.get_me(), timeout=timeout_s)
        return time.time() - t0
    except Exception:
        return float("inf")

async def choose_best_client(prefer_dc: Optional[int] = None) -> Tuple[int, Client]:
    if not work_loads:
        raise RuntimeError("No clients loaded")
    sorted_by_load = sorted(work_loads.items(), key=lambda x: x[1])
    candidates = [idx for idx, _ in sorted_by_load[:max(1, min(len(sorted_by_load), 3))]]
    # prefer DC if asked
    if prefer_dc is not None:
        pref = [i for i in candidates if f"dc{prefer_dc}" in getattr(multi_clients[i], "session_name", "")]
        if pref:
            candidates = pref + [c for c in candidates if c not in pref]
    lat_tasks = [measure_client_latency(multi_clients[i]) for i in candidates]
    lat_vals = await asyncio.gather(*lat_tasks, return_exceptions=True)
    best_idx = candidates[0]
    best_lat = float("inf")
    for i, lat in zip(candidates, lat_vals):
        try:
            l = float(lat)
        except Exception:
            l = float("inf")
        if l < best_lat:
            best_lat = l
            best_idx = i
    return best_idx, multi_clients[best_idx]

# -------------------------
# State APIs
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

# -------------------------
# Refresh helper for expired file_reference
# -------------------------
async def refetch_message_and_extract(client: Client, message: Message) -> Tuple[Message, Optional[Any], int, Optional[str], Optional[int]]:
    try:
        chat_id = getattr(message, "chat", None).id if getattr(message, "chat", None) else getattr(message, "chat_id", None)
        msg_id = getattr(message, "message_id", None) or getattr(message, "id", None)
        if chat_id is None or msg_id is None:
            return message, None, 0, None, None
        fresh = await client.get_messages(chat_id, msg_id)
        if not fresh:
            return message, None, 0, None, None
        # use get_file_ids helper to construct a FileId-like object and derive properties
        file_id = await get_file_ids(client, chat_id, msg_id)
        size = getattr(file_id, "file_size", 0)
        name = getattr(file_id, "file_name", None)
        dc = getattr(file_id, "dc_id", None)
        # message object can be returned too
        return fresh, file_id, size, name, dc
    except Exception:
        return message, None, 0, None, None

# -------------------------
# Core: download to disk using ByteStreamer (resume-capable)
# -------------------------
async def _download_to_disk(
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
    Downloads the message -> saves to disk with .part and resume support.
    Uses ByteStreamer to fetch exact byte ranges from Telegram (fast).
    """
    # fetch file metadata using ByteStreamer/get_file_ids
    # Determine chat_id and message id from message object
    try:
        chat = getattr(message, "chat", None)
        chat_id = chat.id if chat else getattr(message, "chat_id", None)
        msg_id = getattr(message, "message_id", None) or getattr(message, "id", None)
    except Exception:
        chat_id = None
        msg_id = None

    # Use get_file_ids to build file metadata if possible
    file_id = None
    file_size = 0
    file_name = None
    detected_dc = None
    if get_file_ids is not None and chat_id is not None and msg_id is not None:
        try:
            file_id = await get_file_ids(client, chat_id, msg_id)
            file_size = getattr(file_id, "file_size", 0) or 0
            file_name = getattr(file_id, "file_name", None)
            detected_dc = getattr(file_id, "dc_id", None)
        except Exception:
            file_id = None

    # fallback names
    raw_name = file_name or getattr(getattr(message, "document", None) or getattr(message, "video", None) or getattr(message, "audio", None) or getattr(message, "photo", None), "file_name", None) or f"file_{secrets.token_hex(4)}"
    filename = sanitize_filename(raw_name)
    os.makedirs(save_dir, exist_ok=True)
    final_path = os.path.join(save_dir, filename)
    part_path = final_path + ".part"
    meta_path = final_path + ".meta.json"

    if not key:
        key = key_for_message(message)
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
        try:
            offset = os.path.getsize(part_path)
        except Exception:
            offset = 0
    else:
        if os.path.exists(meta_path):
            try:
                with open(meta_path, "r") as mf:
                    md = json.load(mf)
                    offset = int(md.get("offset", 0))
            except Exception:
                offset = 0

    # if already finished
    if os.path.exists(final_path) and file_size and os.path.getsize(final_path) >= file_size:
        state["status"] = "finished"
        state["downloaded"] = os.path.getsize(final_path)
        state["progress"] = 100.0
        state["end_time"] = time.time()
        return final_path

    # determine in_memory preference
    auto_in_memory = USE_IN_MEMORY_DEFAULT if use_in_memory_override is None else use_in_memory_override
    system_mem = get_system_memory_percent()
    if use_in_memory_override is None:
        if system_mem < (MEM_PRESSURE_PCT - 10) and file_size and file_size < (150 * 1024 * 1024):
            auto_in_memory = True
        elif system_mem >= MEM_PRESSURE_PCT:
            auto_in_memory = False

    # state trackers
    speed_history: List[float] = []
    retries = 0
    last_progress_time = time.time()
    backoff = 1.0
    last_meta_write = time.time()
    REFRESH_INTERVAL = REFRESH_FILE_REF_INTERVAL

    logger.info(f"START key={key} file={filename} size={file_size/1024/1024:.2f}MB dc={detected_dc} offset={offset} in_memory={auto_in_memory}")
    state["debug"].append({"t": time.time(), "evt": "start", "offset": offset, "in_memory": auto_in_memory})

    # open file lock
    lock = file_write_locks.get(part_path)
    if lock is None:
        lock = asyncio.Lock()
        file_write_locks[part_path] = lock

    # if we have a ByteStreamer and file_id, use it for precise chunk fetch
    streamer = None
    if file_id is not None and ByteStreamer is not None:
        try:
            streamer = await get_streamer_for_client(client)
        except Exception:
            streamer = None

    # open part file (sync handle used for fsync), but prefer aiofiles for writes
    mode = "r+b" if os.path.exists(part_path) else "wb"
    with open(part_path, mode) as f_sync:
        if os.path.exists(part_path):
            f_sync.seek(offset)
        last_refetch_time = time.time()
        client_idx = None
        try:
            client_idx = multi_clients.index(client) if client in multi_clients else None
        except Exception:
            client_idx = None

        while True:
            if state["cancel"]:
                state["status"] = "cancelled"
                logger.info(f"Cancelled download key={key}")
                raise asyncio.CancelledError("cancelled")
            await state["pause_event"].wait()

            if file_size and offset >= file_size:
                break

            # refresh file_reference periodically
            if time.time() - last_refetch_time > REFRESH_INTERVAL:
                try:
                    fresh_msg, new_file_id, new_size, new_name, new_dc = await refetch_message_and_extract(client, message)
                    if new_file_id:
                        file_id = new_file_id
                        file_size = new_size or file_size
                        if not streamer:
                            streamer = await get_streamer_for_client(client)
                    last_refetch_time = time.time()
                    logger.debug("Refetched message to refresh file_reference")
                except Exception:
                    pass

            # stall detection
            if time.time() - last_progress_time > stall_timeout:
                retries += 1
                logger.warning(f"Stall (no progress in {stall_timeout}s). retry {retries}/{max_retries}")
                backoff = min(backoff * 2, 30)
                await asyncio.sleep(jitter_sleep(backoff, 3))
                last_progress_time = time.time()
                if retries >= max_retries:
                    # rotate client if possible
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
                t0 = time.time()
                # Fetch chunk:
                chunk = b""
                if streamer and file_id is not None and (not auto_in_memory):
                    # use ByteStreamer to fetch this one chunk at exact offset
                    # ByteStreamer.yield_file is an async generator which yields chunks of size chunk_size
                    # But we need to request starting offset. We can call yield_file with offset and part_count=1
                    part_count = 1
                    # The streamer yields raw bytes blocks; to fetch only one chunk, we can iterate once
                    async for data in streamer.yield_file(file_id, multi_clients.index(client), offset, 0, None, part_count, chunk_size):
                        chunk = data or b""
                        break
                    # if no data, chunk remains empty
                else:
                    # fallback: use client.download_media to fetch whole file in-memory (only for small files or as fallback)
                    mem = await asyncio.wait_for(client.download_media(message, in_memory=True), timeout=timeout)
                    if hasattr(mem, "getbuffer"):
                        chunk = mem.getbuffer().tobytes()
                    else:
                        chunk = mem or b""

                if not chunk:
                    retries += 1
                    logger.warning(f"Empty chunk at offset {offset}. retry {retries}/{max_retries}")
                    await asyncio.sleep(jitter_sleep(0.5, 1.0))
                    if retries >= max_retries:
                        if client_idx is not None and len(multi_clients) > 1:
                            new_idx = (client_idx + 1) % len(multi_clients)
                            logger.warning(f"Rotating client due to empty chunks {client_idx} -> {new_idx}")
                            client_idx = new_idx
                            client = multi_clients[client_idx]
                            try:
                                await client.start()
                            except Exception:
                                pass
                            retries = 0
                            continue
                        state["status"] = "failed"
                        raise Exception("repeated empty chunks")
                    continue

                # write chunk to disk
                written = 0
                async with lock:
                    if aiofiles:
                        # use aiofiles to write at offset
                        mode_af = 'r+b' if os.path.exists(part_path) else 'wb'
                        async with aiofiles.open(part_path, mode=mode_af) as af:
                            await af.seek(offset)
                            await af.write(chunk)
                            try:
                                await af.flush()
                            except Exception:
                                pass
                        # sync fsync via sync file descriptor
                        try:
                            f_sync.flush()
                            os.fsync(f_sync.fileno())
                        except Exception:
                            pass
                    else:
                        f_sync.seek(offset)
                        f_sync.write(chunk)
                        f_sync.flush()
                        try:
                            os.fsync(f_sync.fileno())
                        except Exception:
                            pass
                    written = len(chunk)

                offset += written
                last_progress_time = time.time()
                retries = 0
                backoff = 1.0

                elapsed = max(1e-6, time.time() - t0)
                speed = (written / 1024 / 1024) / elapsed
                speed_history.append(speed)
                if len(speed_history) > 40:
                    speed_history.pop(0)
                avg_speed = sum(speed_history) / len(speed_history) if speed_history else speed

                # update state
                state["downloaded"] = offset
                state["progress"] = (offset / file_size) * 100 if file_size else 0.0
                state["speed_history"] = speed_history[-10:]
                state["avg_speed"] = avg_speed
                state["retries"] = retries
                state["last_update"] = time.time()
                state["debug"].append({"t": time.time(), "evt": "chunk", "offset": offset, "written": written, "speed": speed})

                logger.info(f"⬇️ {state['progress']:.2f}% | {avg_speed:.2f} MB/s | offset={offset/1024/1024:.2f}MB | in_memory={auto_in_memory}")

                # write meta every 2s
                if time.time() - last_meta_write > 2:
                    try:
                        with open(meta_path, "w") as mf:
                            json.dump({"offset": offset, "last_update": time.time(), "in_memory": auto_in_memory}, mf)
                    except Exception:
                        pass
                    last_meta_write = time.time()

                # adapt in_memory if memory pressure
                mem_pct = get_system_memory_percent()
                if mem_pct >= MEM_PRESSURE_PCT and auto_in_memory:
                    auto_in_memory = False
                    logger.warning("Memory pressure -> switching to disk mode")
                else:
                    if avg_speed < 0.5 and not auto_in_memory:
                        auto_in_memory = True
                        logger.info("Low avg speed -> enabling in_memory fallback")

            except asyncio.TimeoutError:
                retries += 1
                logger.warning(f"Chunk timeout at offset {offset}. retry {retries}/{max_retries}")
                await asyncio.sleep(jitter_sleep(1, 2))
                if retries >= max_retries:
                    if not auto_in_memory:
                        auto_in_memory = True
                        logger.warning("Switching to in_memory fallback after timeouts")
                        continue
                    if client_idx is not None and len(multi_clients) > 1:
                        new_idx = (client_idx + 1) % len(multi_clients)
                        logger.warning(f"Rotating client after timeouts {client_idx} -> {new_idx}")
                        client_idx = new_idx
                        client = multi_clients[client_idx]
                        try:
                            await client.start()
                        except Exception:
                            pass
                        retries = 0
                        continue
                    state["status"] = "failed"
                    raise Exception("chunk timeout too many times")
            except FloodWait as e:
                logger.warning(f"FloodWait {e.value}s - sleeping")
                await asyncio.sleep(e.value + 1)
                continue
            except RPCError as e:
                serr = str(e).upper()
                logger.warning(f"RPCError: {e}")
                if "FILE_REFERENCE_EXPIRED" in serr or "FILE_REFERENCE" in serr:
                    logger.info("FILE_REFERENCE_EXPIRED -> refetching message")
                    try:
                        message, file_id, file_size, file_name, detected_dc = await refetch_message_and_extract(client, message)
                        if file_id and not streamer:
                            streamer = await get_streamer_for_client(client)
                    except Exception:
                        pass
                    await asyncio.sleep(jitter_sleep(0.5, 1.0))
                    continue
                if "MIGRATE" in serr or "PHONE_MIGRATE" in serr or "NETWORK_MIGRATE" in serr:
                    logger.warning("DC migrate error -> bubble up for DC handler")
                    raise
                retries += 1
                await asyncio.sleep(jitter_sleep(1, 2))
                if retries >= max_retries:
                    if client_idx is not None and len(multi_clients) > 1:
                        new_idx = (client_idx + 1) % len(multi_clients)
                        logger.warning(f"Rotating client after RPC errors {client_idx} -> {new_idx}")
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
            except Exception as e:
                retries += 1
                logger.error(f"Unknown error during chunk: {e}")
                traceback.print_exc()
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
                await asyncio.sleep(jitter_sleep(1, 2))
                continue

    # finalize
    try:
        os.replace(part_path, final_path)
    except Exception:
        try:
            os.rename(part_path, final_path)
        except Exception as e:
            state["status"] = "failed"
            logger.error(f"Finalize failed: {e}")
            raise

    state["status"] = "finished"
    state["end_time"] = time.time()
    state["downloaded"] = os.path.getsize(final_path) if os.path.exists(final_path) else offset
    state["progress"] = 100.0
    logger.info(f"Finished download key={key} path={final_path}")

    try:
        with open(final_path + ".meta.json", "w") as mf:
            json.dump({"finished": True, "end_time": state["end_time"], "debug": state.get("debug", [])[-50:]}, mf)
    except Exception:
        pass
    return final_path

# -------------------------
# DC-aware wrapper to recreate client on migrate errors (best-effort)
# -------------------------
async def _download_with_dc_handling(client_index: int, client: Client, message: Message, **kwargs) -> str:
    attempts = 0
    while True:
        try:
            return await _download_to_disk(client, message, **kwargs)
        except RPCError as e:
            s = str(e).upper()
            if "MIGRATE" in s or "PHONE_MIGRATE" in s or "NETWORK_MIGRATE" in s:
                logger.warning(f"DC migrate requested -> {e}")
                dc_id = None
                try:
                    import re
                    m = re.search(r"MIGRATE_(\d+)", str(e), re.IGNORECASE)
                    if m:
                        dc_id = int(m.group(1))
                except Exception:
                    pass
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
                    continue
                except Exception as ex:
                    logger.error(f"Failed recreate client for DC {dc_id}: {ex}")
                    raise
            else:
                raise

# -------------------------
# Public wrapper selecting client and running download
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
# Optional debug HTTP status app (aiohttp)
# -------------------------
def make_debug_app():
    if web is None:
        raise RuntimeError("aiohttp not installed")
    app = web.Application()
    routes = web.RouteTableDef()

    @routes.get("/status")
    async def status_all(request):
        async with download_state_lock:
            data = {k: get_state_snapshot(k) for k in download_states.keys()}
        return web.json_response(data)

    @routes.get("/status/{key}")
    async def status_key(request):
        key = request.match_info["key"]
        async with download_state_lock:
            st = get_state_snapshot(key)
        if not st:
            raise web.HTTPNotFound(text=json.dumps({"error": "not found"}), content_type="application/json")
        return web.json_response(st)

    app.add_routes(routes)
    return app

# -------------------------
# CLI runner
# -------------------------
def _install_signal_handlers(loop):
    try:
        import signal
        loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(_shutdown(loop)))
        loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(_shutdown(loop)))
    except Exception:
        pass

async def _shutdown(loop):
    logger.info("safe_download shutting down...")
    await asyncio.sleep(0.1)
    loop.stop()

async def _cli_run(args):
    if not multi_clients:
        logger.error("No multi_clients available. Exiting.")
        return
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
            logger.info(f"Debug WS server started on port {os.getenv('DEBUG_WS_PORT', '8765')}")
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
    parser = argparse.ArgumentParser(description="Thunder safe_download (fast + resume)")
    parser.add_argument("--chat", type=int, help="chat id")
    parser.add_argument("--msg", type=int, help="message id")
    parser.add_argument("--output", type=str, default=SAVE_DIR, help="download directory")
    parser.add_argument("--chunk", type=int, default=CHUNK_SIZE, help="chunk size bytes")
    parser.add_argument("--timeout", type=int, default=TIMEOUT, help="per-chunk timeout")
    parser.add_argument("--retries", type=int, default=MAX_RETRIES, help="max retries per chunk")
    parser.add_argument("--ws", action="store_true", help="start debug WS")
    parser.add_argument("--watchdogs", action="store_true", help="start client watchdogs")
    parser.add_argument("--in-memory", choices=["auto", "true", "false"], default="auto", help="override in_memory")
    args = parser.parse_args()

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
