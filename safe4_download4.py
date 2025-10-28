#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Thunder.server.safe_download
Hybrid V3 — DC-aware, resumable downloader for Telegram media with .part + metadata + streaming.

Exports:
    - async download_file(message, *, key=None, client=None, save_dir=None, chunk_size=None)
    - async stream_and_download(message, request, *, client=None, save_dir=None)
    - async pause_download(key), resume_download(key), cancel_download(key), get_state_snapshot(key)

Intended to be used together with stream_routes.py which will call download_file or stream_and_download.
"""
from __future__ import annotations

import os
import time
import json
import math
import random
import asyncio
import logging
import traceback
import secrets
import typing as t
from contextlib import asynccontextmanager

# Optional imports; require pyrogram installed in your environment
try:
    from pyrogram import Client
    from pyrogram.types import Message
    from pyrogram.errors import FloodWait, RPCError
    from pyrogram.raw.functions.upload import GetFile
    from pyrogram.raw.types import InputDocumentFileLocation, InputPhotoFileLocation
except Exception:
    # type stubs for linting if pyrogram missing; runtime will error if used without pyrogram
    Client = object
    Message = object
    GetFile = object
    InputDocumentFileLocation = object
    InputPhotoFileLocation = object
    FloodWait = Exception
    RPCError = Exception

# aiohttp used only for stream_and_download wrapper exceptions
try:
    from aiohttp import web
except Exception:
    web = None

# Try to pull project config from Thunder.vars if present
try:
    from Thunder.vars import Var
except Exception:
    class Var:
        # sensible defaults if not provided by consumer
        DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/tmp/thunder_downloads")
        CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 1 * 1024 * 1024))
        TIMEOUT = int(os.getenv("TIMEOUT", 60))
        MAX_RETRIES = int(os.getenv("MAX_RETRIES", 6))
        MAX_CONCURRENT_DOWNLOADS = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", 3))
        STALL_TIMEOUT = int(os.getenv("STALL_TIMEOUT", 60))
        DEFAULT_IN_MEMORY = False
        WORKDIR = "/tmp"
        CACHE_SIZE = 32

# Try to load multi_clients, work_loads if in project namespace
try:
    from Thunder.bot import multi_clients, work_loads
except Exception:
    multi_clients = []
    work_loads = {}

# Logging
logger = logging.getLogger("Thunder.safe_download")
if not logger.handlers:
    # configure simple console handler if not configured already
    handler = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Config (use Var values when available)
CHUNK_SIZE = int(getattr(Var, "CHUNK_SIZE", 1 * 1024 * 1024))
TIMEOUT = int(getattr(Var, "TIMEOUT", 60))
MAX_RETRIES = int(getattr(Var, "MAX_RETRIES", 6))
SAVE_DIR = getattr(Var, "DOWNLOAD_DIR", os.getenv("DOWNLOAD_DIR", os.path.join(os.getcwd(), "downloads")))
MAX_CONCURRENT_DOWNLOADS = int(getattr(Var, "MAX_CONCURRENT_DOWNLOADS", 3))
STALL_TIMEOUT = int(getattr(Var, "STALL_TIMEOUT", 60))
USE_IN_MEMORY_DEFAULT = bool(getattr(Var, "DEFAULT_IN_MEMORY", False))

# Ensure save dir exists
try:
    os.makedirs(SAVE_DIR, exist_ok=True)
except Exception as e:
    logger.warning(f"Could not create SAVE_DIR '{SAVE_DIR}': {e}")

# Semaphore limits concurrent active downloads
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

# Global in-process state store for pause/resume/cancel/monitoring
download_states: t.Dict[str, t.Dict[str, t.Any]] = {}
download_state_lock = asyncio.Lock()

# Utilities -------------------------------------------------------------
def sanitize_filename(name: t.Optional[str]) -> str:
    """Return a safe filename fallbacking to random token if needed."""
    if not name:
        return f"file_{secrets.token_hex(6)}"
    # allow alnum and safe punctuation
    safe = "".join(c for c in name if c.isalnum() or c in " ._-()[]{}").strip()
    return safe or f"file_{secrets.token_hex(6)}"

def key_for_message(message: Message) -> str:
    """Unique key for a message used in download_states and meta files."""
    try:
        # message.chat.id may exist, fallback to chat_id attribute
        chat = getattr(message, "chat", None)
        chat_id = getattr(chat, "id", None) if chat else getattr(message, "chat_id", None)
        mid = getattr(message, "message_id", None) or getattr(message, "id", None) or getattr(message, "message", None) or secrets.token_hex(6)
        return f"{chat_id}:{mid}"
    except Exception:
        return secrets.token_hex(8)

def _meta_path_for(part_path: str) -> str:
    return f"{part_path}.meta.json"

def _load_meta(part_path: str) -> t.Dict[str, t.Any]:
    meta_path = _meta_path_for(part_path)
    if not os.path.exists(meta_path):
        return {}
    try:
        with open(meta_path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}

def _save_meta(part_path: str, obj: t.Dict[str, t.Any]) -> None:
    meta_path = _meta_path_for(part_path)
    try:
        with open(meta_path, "w", encoding="utf-8") as fh:
            json.dump(obj, fh)
    except Exception as e:
        logger.debug(f"Failed to save meta {meta_path}: {e}")

def _remove_meta(part_path: str) -> None:
    meta_path = _meta_path_for(part_path)
    try:
        if os.path.exists(meta_path):
            os.remove(meta_path)
    except Exception:
        pass

# Raw GetFile wrapper ---------------------------------------------------
async def raw_getfile_chunk(client: Client, location_obj: t.Any, offset: int, limit: int) -> bytes:
    """
    Uses raw GetFile to fetch a chunk. Returns bytes or b''.
    """
    try:
        req = GetFile(location=location_obj, offset=offset, limit=limit)
        res = await client.invoke(req)
        # try common fields
        data = getattr(res, "bytes", None) or getattr(res, "file_bytes", None) or None
        if data is None:
            # sometimes GetFile returns .file which contains bytes
            file_attr = getattr(res, "file", None)
            if file_attr is not None and hasattr(file_attr, "bytes"):
                data = getattr(file_attr, "bytes")
        return data or b""
    except Exception as e:
        # propagate exception to caller so retry/backoff logic can handle
        raise

# Extract location helper ------------------------------------------------
def extract_location_from_message(message: Message) -> t.Tuple[t.Optional[t.Any], int, t.Optional[str], t.Optional[int]]:
    """
    Try to extract a raw InputDocumentFileLocation or InputPhotoFileLocation and file_size/file_name/dc.
    Returns (location_obj|None, file_size, suggested_name, detected_dc).
    """
    media = getattr(message, "document", None) or getattr(message, "video", None) or getattr(message, "audio", None) or getattr(message, "photo", None)
    if not media:
        return None, 0, None, None

    suggested_name = getattr(media, "file_name", None) or getattr(media, "file_unique_id", None) or None
    file_size = getattr(media, "file_size", None) or getattr(media, "size", None) or 0
    detected_dc = None

    raw = getattr(media, "_raw", None) or getattr(media, "__dict__", None) or {}
    # try to look up ID/access_hash/file_reference/dc
    try:
        doc_id = raw.get("id") if isinstance(raw, dict) else getattr(raw, "id", None)
        access_hash = raw.get("access_hash") if isinstance(raw, dict) else getattr(raw, "access_hash", None)
        file_ref = raw.get("file_reference") if isinstance(raw, dict) else getattr(raw, "file_reference", None)
        dc_val = raw.get("dc_id") if isinstance(raw, dict) else getattr(raw, "dc_id", None)
    except Exception:
        doc_id = access_hash = file_ref = dc_val = None

    try:
        if doc_id and access_hash and file_ref is not None:
            detected_dc = int(dc_val) if dc_val else None
            return InputDocumentFileLocation(id=int(doc_id), access_hash=int(access_hash), file_reference=file_ref, thumb_size=""), int(file_size), suggested_name, detected_dc
    except Exception:
        pass

    try:
        photo_id = raw.get("id") if isinstance(raw, dict) else getattr(raw, "id", None)
        photo_access = raw.get("access_hash") if isinstance(raw, dict) else getattr(raw, "access_hash", None)
        photo_ref = raw.get("file_reference") if isinstance(raw, dict) else getattr(raw, "file_reference", None)
        if photo_id and photo_access and photo_ref is not None:
            detected_dc = int(dc_val) if dc_val else None
            return InputPhotoFileLocation(id=int(photo_id), access_hash=int(photo_access), file_reference=photo_ref, thumb_size=""), int(file_size), suggested_name, detected_dc
    except Exception:
        pass

    return None, int(file_size or 0), suggested_name, None

# Choose client (DC-aware) ------------------------------------------------
async def measure_client_latency(client: Client) -> float:
    """Measure latency of a client by calling get_me."""
    try:
        t0 = time.time()
        await client.get_me()
        return time.time() - t0
    except Exception:
        return float("inf")

async def choose_best_client(prefer_dc: t.Optional[int] = None) -> t.Tuple[int, Client]:
    if not work_loads:
        raise RuntimeError("No clients loaded")
    sorted_idxs = sorted(work_loads.items(), key=lambda kv: kv[1])
    candidate_count = max(1, min(len(sorted_idxs), 3))
    candidates = [idx for idx, _ in sorted_idxs[:candidate_count]]

    if prefer_dc is not None:
        pref = [i for i in candidates if f"dc{prefer_dc}" in getattr(multi_clients[i], "session_name", "")]
        if pref:
            candidates = pref + [c for c in candidates if c not in pref]

    lat_tasks = [measure_client_latency(multi_clients[i]) for i in candidates]
    lat_res = await asyncio.gather(*lat_tasks, return_exceptions=True)
    best_idx = candidates[0]
    best_lat = float("inf")
    for idx, res in zip(candidates, lat_res):
        try:
            lat = float(res)
        except Exception:
            lat = float("inf")
        if lat < best_lat:
            best_lat = lat
            best_idx = idx
    return best_idx, multi_clients[best_idx]

# Download state helpers ---------------------------------------------------
async def _init_state(key: str, info: t.Dict[str, t.Any]):
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

def get_state_snapshot(key: str) -> t.Dict[str, t.Any]:
    st = download_states.get(key)
    if not st:
        return {}
    out = {k: (v if k not in ("pause_event", "message") else None) for k, v in st.items()}
    return out

# ----------------------------
# Core adaptive chunked downloader (resumable)
# ----------------------------
async def _adaptive_chunked_download(client: Client, message: Message,
                                     *,
                                     key: t.Optional[str] = None,
                                     chunk_size: int = CHUNK_SIZE,
                                     timeout: int = TIMEOUT,
                                     max_retries: int = MAX_RETRIES,
                                     save_dir: str = SAVE_DIR,
                                     stall_timeout: int = STALL_TIMEOUT,
                                     use_in_memory_override: t.Optional[bool] = None) -> str:
    """
    Downloads message into save_dir using chunked raw GetFile requests when possible.
    Persists .part and .part.meta.json for robust resume.
    Returns final path.
    """
    location_obj, file_size, suggested_name, detected_dc = extract_location_from_message(message)
    raw_name = suggested_name or getattr(getattr(message, "document", None) or getattr(message, "video", None) or getattr(message, "audio", None) or getattr(message, "photo", None), "file_name", None) or f"file_{secrets.token_hex(4)}"
    filename = sanitize_filename(raw_name)
    os.makedirs(save_dir, exist_ok=True)
    final_path = os.path.join(save_dir, filename)
    part_path = final_path + ".part"
    meta = _load_meta(part_path)

    if not key:
        key = key_for_message(message)
    await _init_state(key, {"size": file_size, "filename": filename, "message": message, "dc": detected_dc})
    state = download_states[key]
    state["status"] = "running"
    state["start_time"] = time.time()
    state["size"] = file_size
    state["filename"] = filename
    state["dc"] = detected_dc

    # resume position
    offset = 0
    if os.path.exists(part_path):
        offset = os.path.getsize(part_path)
    elif meta.get("downloaded"):
        try:
            offset = int(meta.get("downloaded", 0))
        except Exception:
            offset = 0

    # final file already exists and complete?
    if os.path.exists(final_path) and file_size and os.path.getsize(final_path) >= file_size:
        state["status"] = "finished"
        state["downloaded"] = os.path.getsize(final_path)
        state["progress"] = 100.0
        state["end_time"] = time.time()
        _remove_meta(part_path)
        return final_path

    # choose in-memory vs disk heuristics
    mem_pct = 0.0
    try:
        import psutil
        mem_pct = psutil.virtual_memory().percent
    except Exception:
        mem_pct = 0.0

    auto_in_memory = USE_IN_MEMORY_DEFAULT if use_in_memory_override is None else use_in_memory_override
    if use_in_memory_override is None:
        try:
            if mem_pct < 80.0 and file_size and file_size < (200 * 1024 * 1024):
                auto_in_memory = True
            elif mem_pct >= 80.0:
                auto_in_memory = False
        except Exception:
            auto_in_memory = USE_IN_MEMORY_DEFAULT

    logger.info(f"Download start key={key} file={filename} size={file_size/1024/1024 if file_size else 0:.2f}MB dc={detected_dc} offset={offset} in_memory={auto_in_memory}")

    # fallback to client.download_media if location_obj missing
    if location_obj is None:
        tmp = part_path
        fallback_retries = 0
        while True:
            if state["cancel"]:
                state["status"] = "cancelled"
                raise asyncio.CancelledError("cancelled")
            await state["pause_event"].wait()
            try:
                # use Pyrogram's download_media to tmp path (it's resumable in some cases)
                await asyncio.wait_for(client.download_media(message, file_name=tmp, in_memory=False), timeout=timeout)
                try:
                    os.replace(tmp, final_path)
                except Exception:
                    os.rename(tmp, final_path)
                state["status"] = "finished"
                state["downloaded"] = os.path.getsize(final_path)
                state["progress"] = 100.0
                state["end_time"] = time.time()
                _remove_meta(part_path)
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
                await asyncio.sleep(random.random() * 2 + 1)
            except Exception as e:
                fallback_retries += 1
                logger.warning(f"Fallback error: {e}")
                if fallback_retries >= max_retries:
                    state["status"] = "failed"
                    raise
                await asyncio.sleep(random.random() * 2 + 1)

    # open part file and append chunks
    mode = "r+b" if os.path.exists(part_path) else "wb"
    with open(part_path, mode) as f:
        if os.path.exists(part_path):
            f.seek(offset)
            logger.info(f"Resuming part at {offset} bytes")
        consecutive_empty = 0
        retries = 0
        last_progress_time = time.time()
        backoff = 1.0
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

            # refresh message reference periodically to avoid FileReferenceExpired
            if time.time() - last_progress_time > 30 and (retries % 10 == 0):
                try:
                    # try to re-get message to refresh file reference
                    chat_id = getattr(message, "chat", None)
                    chat_id = getattr(chat_id, "id", None) if chat_id else getattr(message, "chat_id", None)
                    msg_id = getattr(message, "message_id", None) or getattr(message, "id", None)
                    if chat_id and msg_id:
                        fresh = await client.get_messages(chat_id, msg_id)
                        if fresh:
                            message = fresh
                            location_obj, file_size, suggested_name, detected_dc = extract_location_from_message(fresh)
                            logger.debug("Refetched message to refresh file_reference")
                except Exception:
                    pass

            # stall detection
            if time.time() - last_progress_time > stall_timeout:
                retries += 1
                logger.warning(f"Stall detected (no progress {stall_timeout}s) retry {retries}/{max_retries}")
                backoff = min(backoff * 2, 30)
                await asyncio.sleep(random.random() * backoff + 1)
                last_progress_time = time.time()
                if retries >= max_retries:
                    # attempt client rotation if possible
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
                start_time = time.time()
                if not auto_in_memory and location_obj is not None:
                    chunk = await asyncio.wait_for(raw_getfile_chunk(client, location_obj, offset, chunk_size), timeout=timeout)
                else:
                    # fallback: use pyrogram download_media in-memory chunking if possible — for now call raw_getfile_chunk anyway
                    chunk = await asyncio.wait_for(raw_getfile_chunk(client, location_obj, offset, chunk_size), timeout=timeout)
            except FloodWait as e:
                logger.warning(f"FloodWait {e.value}s")
                await asyncio.sleep(e.value + 1)
                continue
            except asyncio.TimeoutError:
                retries += 1
                logger.warning(f"Timeout fetching chunk at offset {offset} (retry {retries}/{max_retries})")
                if retries >= max_retries:
                    state["status"] = "failed"
                    raise
                await asyncio.sleep(min(5 * retries, 30))
                continue
            except RPCError as e:
                retries += 1
                logger.warning(f"RPCError at offset {offset}: {e} (retry {retries}/{max_retries})")
                if retries >= max_retries:
                    state["status"] = "failed"
                    raise
                await asyncio.sleep(min(3 * retries, 20))
                continue
            except Exception as e:
                retries += 1
                logger.warning(f"Unexpected error reading chunk at {offset}: {e} (retry {retries}/{max_retries})")
                if retries >= max_retries:
                    state["status"] = "failed"
                    raise
                await asyncio.sleep(min(3 * retries, 20))
                continue

            if not chunk:
                consecutive_empty += 1
                if consecutive_empty > 5:
                    logger.warning(f"No data returned for several attempts at offset {offset}")
                    await asyncio.sleep(1)
                    consecutive_empty = 0
                await asyncio.sleep(0.1)
                continue

            # Write and flush chunk
            try:
                f.write(chunk)
                f.flush()
            except Exception as e:
                logger.error(f"Write error to part file: {e}")
                raise

            offset += len(chunk)
            now = time.time()
            duration = now - start_time if now - start_time > 0 else 0.0001
            speed = len(chunk) / duration
            state["downloaded"] = offset
            state["last_update"] = now
            state["speed_history"].append(speed)
            if len(state["speed_history"]) > 10:
                state["speed_history"].pop(0)
            state["avg_speed"] = sum(state["speed_history"]) / len(state["speed_history"]) if state["speed_history"] else 0.0
            state["progress"] = (offset / file_size * 100.0) if file_size else 0.0
            last_progress_time = now
            retries = 0
            consecutive_empty = 0

            _save_meta(part_path, {"downloaded": offset, "size": file_size, "filename": filename, "timestamp": time.time()})

        # finished loop
    # finalize: rename part -> final
    try:
        try:
            os.replace(part_path, final_path)
        except Exception:
            os.rename(part_path, final_path)
        state["status"] = "finished"
        state["end_time"] = time.time()
        state["downloaded"] = os.path.getsize(final_path) if os.path.exists(final_path) else state.get("downloaded", 0)
        state["progress"] = 100.0
        _remove_meta(part_path)
        logger.info(f"Download finished: {final_path}")
        return final_path
    except Exception as e:
        state["status"] = "failed"
        logger.error(f"Failed to finalize download {e}")
        raise

# Public facade -----------------------------------------------------------
async def download_file(message: Message,
                        *,
                        key: t.Optional[str] = None,
                        client: t.Optional[Client] = None,
                        save_dir: t.Optional[str] = None,
                        chunk_size: t.Optional[int] = None) -> str:
    """
    Start/resume a download and return final file path.
    This function will resume existing .part file if present.
    """
    if client is None:
        # attempt to select a default client (if project uses multi_clients)
        if multi_clients:
            client = multi_clients[0]
        else:
            raise RuntimeError("No pyrogram client provided and multi_clients is empty")
    if save_dir is None:
        save_dir = SAVE_DIR
    if chunk_size is None:
        chunk_size = CHUNK_SIZE

    # normalize key
    if not key:
        key = key_for_message(message)

    async with DOWNLOAD_SEMAPHORE:
        try:
            return await _adaptive_chunked_download(client, message, key=key, chunk_size=chunk_size, save_dir=save_dir)
        except asyncio.CancelledError:
            logger.info("Download cancelled by user")
            raise
        except Exception as e:
            logger.exception(f"download_file failed: {e}")
            raise

# Streaming helper: stream from .part while background download runs ------------
async def stream_and_download(message: Message, request, *,
                              client: t.Optional[Client] = None,
                              save_dir: t.Optional[str] = None,
                              key: t.Optional[str] = None,
                              chunk_size: t.Optional[int] = None):
    """
    Stream the file to an HTTP client while ensuring a downloader has started.
    - If Range header present, we attempt to honor it.
    - Streams from .part as it grows; if final file appears we switch to it.
    """
    if web is None:
        raise RuntimeError("aiohttp is required for stream_and_download")

    # Inspect incoming headers for debugging
    try:
        headers = dict(request.headers)
        logger.info(f"🔍 Incoming headers: {json.dumps(headers)}")
    except Exception:
        logger.debug("Could not serialize incoming headers")

    if client is None:
        if multi_clients:
            client = multi_clients[0]
        else:
            raise RuntimeError("No client available for streaming")

    save_dir = save_dir or SAVE_DIR
    chunk_size = chunk_size or CHUNK_SIZE

    # pick key & launch background download if needed
    if not key:
        key = key_for_message(message)

    # Ensure a downloader task is running for this message
    # We attempt to avoid starting parallel duplicate downloads by examining download_states
    async with download_state_lock:
        st = download_states.get(key)
        need_start = (st is None) or (st.get("status") in ("queued", "failed", "cancelled"))
    # start downloader in background if not already running/finished
    downloader_task: t.Optional[asyncio.Task] = None
    if need_start:
        # start background task but don't await it here
        async def _bg():
            try:
                await download_file(message, key=key, client=client, save_dir=save_dir, chunk_size=chunk_size)
            except asyncio.CancelledError:
                logger.info("Background download task cancelled")
            except Exception:
                logger.exception("Background download task error")

        downloader_task = asyncio.create_task(_bg())

    # Resolve expected filename & file paths
    _, file_size, suggested_name, _ = extract_location_from_message(message)
    raw_name = suggested_name or getattr(getattr(message, "document", None) or getattr(message, "video", None) or getattr(message, "audio", None) or getattr(message, "photo", None), "file_name", None) or f"file_{secrets.token_hex(6)}"
    filename = sanitize_filename(raw_name)
    final_path = os.path.join(save_dir, filename)
    part_path = final_path + ".part"

    # Parse Range header
    range_header = request.headers.get("Range", "")
    req_start = 0
    req_end = None
    if range_header:
        try:
            m = re.search(r"bytes=(\d*)-(\d*)", range_header)
            if m:
                s_str, e_str = m.groups()
                req_start = int(s_str) if s_str else 0
                req_end = int(e_str) if e_str else None
        except Exception:
            raise web.HTTPBadRequest(text=json.dumps({"error": "Malformed Range header"}), content_type="application/json")

    # Compute content length when possible
    total_size = file_size if file_size else None
    start = req_start or 0
    end = None
    if req_end is not None:
        end = req_end
    elif total_size is not None:
        end = total_size - 1

    if total_size is not None:
        if start < 0 or start >= total_size:
            raise web.HTTPRequestRangeNotSatisfiable(headers={"Content-Range": f"bytes */{total_size}"})
        if end is None:
            end = total_size - 1
        elif end >= total_size:
            end = total_size - 1
        if start > end:
            raise web.HTTPRequestRangeNotSatisfiable(headers={"Content-Range": f"bytes */{total_size}"})

    status = 206 if range_header else 200
    resp_headers = {
        "Content-Type": "application/octet-stream",
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Disposition": f"attachment; filename*=UTF-8''{filename}",
        "X-Content-Type-Options": "nosniff",
    }
    if end is not None and total_size is not None:
        resp_headers["Content-Range"] = f"bytes {start}-{end}/{total_size}"
        resp_headers["Content-Length"] = str(end - start + 1)
    elif total_size is not None:
        resp_headers["Content-Length"] = str(total_size - start)

    # Prepare StreamResponse
    resp = web.StreamResponse(status=status, headers=resp_headers)
    await resp.prepare(request)

    # streaming generator: read from part file as it grows, switch to final when present
    async def stream_part(path: str, final: str, start_byte: int, end_byte: t.Optional[int], bg_task: t.Optional[asyncio.Task], chunk: int = CHUNK_SIZE):
        pos = start_byte
        # wait for file to appear (part or final)
        wait_loops = 0
        while not os.path.exists(path) and not os.path.exists(final):
            if bg_task is not None and bg_task.done():
                break
            await asyncio.sleep(0.1)
            wait_loops += 1
            if wait_loops > 600:  # waited ~60s
                logger.warning(f"Waited too long for part file {path}")
                break

        # open whichever exists
        current_path = path if os.path.exists(path) else (final if os.path.exists(final) else None)
        if current_path is None:
            return

        while True:
            # prefer reading from part if it exists (growing)
            current_path = path if os.path.exists(path) else (final if os.path.exists(final) else None)
            if current_path is None:
                break
            try:
                with open(current_path, "rb") as rf:
                    # ensure pos is not beyond file length
                    size_now = os.path.getsize(current_path)
                    if pos > size_now:
                        # nothing to read
                        pass
                    else:
                        rf.seek(pos)
                        while True:
                            if end_byte is not None:
                                to_send_remaining = (end_byte - pos + 1)
                                if to_send_remaining <= 0:
                                    return
                                to_read = min(chunk, to_send_remaining)
                            else:
                                to_read = chunk
                            data = rf.read(to_read)
                            if data:
                                pos += len(data)
                                yield data
                                # if we've reached requested end, stop
                                if end_byte is not None and pos > end_byte:
                                    return
                                await asyncio.sleep(0)
                                continue
                            # no data read
                            break
            except Exception as e:
                logger.error(f"Error reading part/final: {e}")
                return

            # if background download still running, wait a bit and continue
            if bg_task is not None and not bg_task.done():
                await asyncio.sleep(0.2)
                continue

            # if final exists and is larger, loop will reopen and read remaining bytes
            if os.path.exists(final) and os.path.getsize(final) > pos:
                await asyncio.sleep(0.05)
                continue

            # otherwise no more data expected
            break
        return

    # Use stream_part generator to write to resp
    try:
        async for chunk_bytes in stream_part(part_path, final_path, start, end, downloader_task, CHUNK_SIZE):
            if not chunk_bytes:
                continue
            # check client disconnect
            if request.transport is None or request.transport.is_closing():
                logger.info("Client disconnected while streaming")
                break
            await resp.write(chunk_bytes)
        # if downloader finished and final exists but we may still need to send remaining bytes
        if downloader_task is not None and downloader_task.done() and os.path.exists(final_path):
            # check if there are remaining bytes between current file pos and requested end
            # simplest: open final and stream any bytes not yet sent from 'start'
            try:
                with open(final_path, "rb") as rf:
                    rf.seek(start)
                    remaining = None if end is None else (end - start + 1)
                    while True:
                        to_read = CHUNK_SIZE if remaining is None else min(CHUNK_SIZE, remaining)
                        data = rf.read(to_read)
                        if not data:
                            break
                        if request.transport is None or request.transport.is_closing():
                            break
                        await resp.write(data)
                        if remaining is not None:
                            remaining -= len(data)
                            if remaining <= 0:
                                break
            except Exception as e:
                logger.debug(f"Error streaming remaining final: {e}")
        try:
            await resp.write_eof()
        except Exception:
            pass
    except ConnectionResetError:
        logger.info("Client hung up during streaming")
    except Exception as e:
        logger.exception(f"Streaming error: {e}")
        try:
            await resp.write_eof()
        except Exception:
            pass

    return resp

# if used as script, provide minimal testing harness (disabled)
if __name__ == "__main__":
    print("safe_download module — not intended for direct run")
