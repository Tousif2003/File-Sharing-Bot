# Thunder/utils/safe_download.py
# VPS-like downloader: DC-aware, parallel raw chunking, stream+save (batched),
# governor, smart retry/rotate, file_reference refresh, ETA, Content-Length.
# Framework: aiohttp (StreamResponse). Requires pyrogram. psutil optional.

import os, asyncio, time, random, secrets, logging, traceback
from typing import Optional, Tuple, Any, Dict, List
from aiohttp import web
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError, FileReferenceExpired
from pyrogram.file_id import FileId, FileType
from pyrogram.raw.functions.upload import GetFile
from pyrogram.raw.types import InputDocumentFileLocation, InputPhotoFileLocation

log = logging.getLogger("safe_download")
if not log.handlers:
    logging.basicConfig(level=logging.INFO)

# ================== TURBO DEFAULTS (env optional) ==================

PLATFORM = os.getenv("PLATFORM", "").lower()              # "vps", "heroku", "koyeb", ...
IS_PAAS  = PLATFORM in ("heroku", "koyeb", "render")

# Start with 1MB per GetFile; auto-downgrade to 512KB if DC rejects (LIMIT_INVALID)
TG_MAX_CHUNK = int(os.getenv("TG_GETFILE_MAX", 1024 * 1024))
CURRENT_MAX_CHUNK = TG_MAX_CHUNK

# Parallel lanes & queues
FETCHERS    = int(os.getenv("FETCHERS", 4))                # parallel telegram lanes
PREFETCH    = int(os.getenv("PREFETCH", 16))               # queue depth

# Target chunk/batch; governor will adapt between SAFE/NORMAL
CHUNK_SIZE_NORMAL   = int(os.getenv("CHUNK_SIZE_NORMAL", 4 * 1024 * 1024))   # 4MB target
CHUNK_SIZE_SAFE     = int(os.getenv("CHUNK_SIZE_SAFE",   1 * 1024 * 1024))   # 1MB target
WRITE_BATCH_NORMAL  = int(os.getenv("WRITE_BATCH_NORMAL",32 * 1024 * 1024))  # 32MB
WRITE_BATCH_SAFE    = int(os.getenv("WRITE_BATCH_SAFE",  16 * 1024 * 1024))  # 16MB

DRAIN_BYTES         = int(os.getenv("DRAIN_BYTES",       4 * 1024 * 1024))   # 4MB writer drain (TLS smooth)
FSYNC_INTERVAL      = int(os.getenv("FSYNC_INTERVAL",    8 * 1024 * 1024))   # fsync every ~8MB
STALL_TIMEOUT       = int(os.getenv("STALL_TIMEOUT",     60))
BACKOFF_CAP_NORM    = int(os.getenv("BACKOFF_CAP_NORM",  30))
BACKOFF_CAP_SAFE    = int(os.getenv("BACKOFF_CAP_SAFE",  60))

SAVE_DIR = os.getenv("SAVE_DIR") or ("/tmp/files" if IS_PAAS else "./downloads")
os.makedirs(SAVE_DIR, exist_ok=True)

# Concurrency guard (per-client)
MAX_CONCURRENT_PER_CLIENT = int(os.getenv("MAX_CONCURRENT_PER_CLIENT", 2 if IS_PAAS else 3))

# Optional: RAM guard
try:
    import psutil
except Exception:
    psutil = None

def mem_pct() -> float:
    if psutil:
        try: return float(psutil.virtual_memory().percent)
        except: return 0.0
    return 0.0

# ================== CLIENT POOL (import from bot) ==================
try:
    from Thunder.bot import multi_clients, work_loads   # type: ignore
except Exception:
    multi_clients: List[Client] = []
    work_loads: Dict[int, int] = {}

def _ensure_workloads():
    if not work_loads and multi_clients:
        for i in range(len(multi_clients)):
            work_loads[i] = 0

_client_sem: Dict[int, asyncio.Semaphore] = {}
def get_sema(cid: int) -> asyncio.Semaphore:
    sem = _client_sem.get(cid)
    if not sem:
        sem = _client_sem[cid] = asyncio.Semaphore(max(1, MAX_CONCURRENT_PER_CLIENT))
    return sem

# ================== SMALL UTILS ==================
def _round_4k(n: int) -> int:
    n = int(max(1, n))
    r = (n // 4096) * 4096
    return r if r > 0 else n

def sanitize_filename(name: Optional[str]) -> str:
    if not name:
        return f"file_{secrets.token_hex(4)}"
    s = "".join(c for c in name if c.isalnum() or c in (" ", ".", "_", "-")).strip()
    return s or f"file_{secrets.token_hex(4)}"

def jitter(base=0.8, spread=1.2) -> float:
    return base + random.random() * spread

async def measure_latency(client: Client) -> float:
    t0 = time.time()
    try: await client.get_me()
    except Exception: return 9.9
    return time.time() - t0

def key_for_message(message) -> str:
    try:
        chat = message.chat.id if message.chat else getattr(message, "chat_id", None)
        mid  = message.id if hasattr(message, "id") else getattr(message, "message_id", None)
        return f"{chat}:{mid}" if (chat is not None and mid is not None) else secrets.token_hex(6)
    except Exception:
        return secrets.token_hex(6)

# ================== DC-AWARE PICK ==================
def _dc_from_media(msg) -> Optional[int]:
    media = getattr(msg, "document", None) or getattr(msg, "video", None) or getattr(msg, "audio", None) or getattr(msg, "photo", None)
    if not media: return None
    raw = getattr(media, "_raw", None) or getattr(media, "__dict__", None)
    if not raw: return None
    did = getattr(raw, "dc_id", None) or getattr(raw, "dc", None)
    try: return int(did) if did is not None else None
    except Exception: return None

async def choose_best_client(prefer_dc: Optional[int] = None) -> Tuple[int, Client]:
    if not multi_clients:
        raise RuntimeError("No pyrogram clients configured")
    _ensure_workloads()

    cand = [i for i,_ in sorted(work_loads.items(), key=lambda kv: kv[1])[:max(1, min(3, len(work_loads)))]]
    if prefer_dc is not None:
        preferred = [i for i in cand if f"dc{prefer_dc}" in getattr(multi_clients[i], "session_name", "")]
        if preferred:
            cand = preferred + [x for x in cand if x not in preferred]

    lats = await asyncio.gather(*[measure_latency(multi_clients[i]) for i in cand], return_exceptions=True)
    best, best_lat = cand[0], 9e9
    for i, lat in zip(cand, lats):
        try: v = float(lat)
        except: v = 9.9
        if v < best_lat:
            best, best_lat = i, v
    return best, multi_clients[best]

# ================== TELEGRAM FILE HELPERS ==================
def _build_location_from_message(msg):
    media = getattr(msg, "document", None) or getattr(msg, "video", None) \
            or getattr(msg, "audio", None) or getattr(msg, "photo", None)
    if not media:
        return None
    try:
        fid = FileId.decode(media.file_id)
    except Exception:
        return None

    thumb_size = getattr(fid, "thumbnail_source", "") or ""

    if fid.file_type in (
        FileType.DOCUMENT, FileType.VIDEO, FileType.AUDIO,
        FileType.VOICE, FileType.VIDEO_NOTE, FileType.STICKER, FileType.ANIMATION
    ):
        return InputDocumentFileLocation(
            id=fid.media_id,
            access_hash=fid.access_hash,
            file_reference=fid.file_reference,
            thumb_size=thumb_size
        )
    elif fid.file_type == FileType.PHOTO:
        return InputPhotoFileLocation(
            id=fid.media_id,
            access_hash=fid.access_hash,
            file_reference=fid.file_reference,
            thumb_size=thumb_size
        )
    return InputDocumentFileLocation(
        id=fid.media_id,
        access_hash=fid.access_hash,
        file_reference=fid.file_reference,
        thumb_size=thumb_size
    )

def _extract_meta(msg) -> Tuple[int, str]:
    f = getattr(msg, "document", None) or getattr(msg, "video", None) or getattr(msg, "audio", None) or getattr(msg, "photo", None)
    if not f:
        return 0, f"file_{secrets.token_hex(4)}"
    size  = getattr(f, "file_size", None) or getattr(f, "size", None) or 0
    fname = getattr(f, "file_name", None) or getattr(f, "file_unique_id", None) or f"file_{secrets.token_hex(4)}"
    return int(size or 0), sanitize_filename(fname)

async def _ensure_location(client: Client, msg):
    loc = _build_location_from_message(msg)
    if loc: return loc
    fresh = await client.get_messages(msg.chat.id, msg.id)
    return _build_location_from_message(fresh)

async def _raw_getfile_chunk(client: Client, location, offset: int, limit: int, message=None) -> Tuple[bytes, Any]:
    if location is None and message is not None:
        location = await _ensure_location(client, message)
    if location is None:
        raise RuntimeError("InputFileLocation is None")
    try:
        res = await client.invoke(GetFile(location=location, offset=offset, limit=limit))
    except FileReferenceExpired:
        if message is None:
            raise
        fresh = await client.get_messages(message.chat.id, message.id)
        new_loc = _build_location_from_message(fresh)
        res = await client.invoke(GetFile(location=new_loc, offset=offset, limit=limit))
        location = new_loc
    data = getattr(res, "bytes", None) or getattr(res, "file_bytes", None)
    if data is None:
        f = getattr(res, "file", None)
        if f is not None and hasattr(f, "bytes"):
            data = getattr(f, "bytes")
    return (data or b""), location

# ================== PROGRESS STATE ==================
_download_states: Dict[str, Dict[str, Any]] = {}
_state_lock = asyncio.Lock()

async def _init_state(key: str, meta: Dict[str, Any]):
    async with _state_lock:
        _download_states[key] = {
            "status": "queued",
            "progress": 0.0,
            "downloaded": 0,
            "size": meta.get("size", 0),
            "filename": meta.get("filename", ""),
            "avg_speed": 0.0,          # MB/s
            "eta_seconds": None,
            "eta_human": "calculating…",
            "retries": 0,
            "start": None,
            "end": None,
            "last_update": time.time(),
        }

def get_state_snapshot(key: str) -> Dict[str, Any]:
    return _download_states.get(key, {})

# ================== GOVERNOR ==================
class Governor:
    __slots__ = ("mode", "last_flip", "window", "sample")
    def __init__(self):
        self.mode = "normal"
        self.last_flip = time.time()
        self.window: List[Tuple[float,float,int]] = []
        self.sample = 0

    def update(self, last_speed: float, timeout_happened: bool, waiters: bool) -> Dict[str,int]:
        now = time.time()
        self.window.append((now, max(0.0, last_speed), 1 if timeout_happened else 0))
        # keep 60s
        while self.window and now - self.window[0][0] > 60:
            self.window.pop(0)
        self.sample += 1
        if self.sample < 8:
            return self.profile()
        self.sample = 0

        avg = (sum(x[1] for x in self.window)/len(self.window)) if self.window else 0.0
        timeouts = sum(x[2] for x in self.window)
        bad = 0
        if avg < 0.6: bad += 1
        if timeouts >= 2: bad += 1
        if waiters: bad += 1

        if self.mode == "normal" and bad >= 2 and now - self.last_flip > 10:
            self.mode = "conservative"; self.last_flip = now
        elif self.mode == "conservative" and (avg > 2.5 and timeouts <= 1 and not waiters) and now - self.last_flip > 90:
            self.mode = "normal"; self.last_flip = now
        return self.profile()

    def profile(self) -> Dict[str,int]:
        if self.mode == "conservative":
            return dict(chunk=CHUNK_SIZE_SAFE, batch=WRITE_BATCH_SAFE, cap=BACKOFF_CAP_SAFE)
        else:
            return dict(chunk=CHUNK_SIZE_NORMAL, batch=WRITE_BATCH_NORMAL, cap=BACKOFF_CAP_NORM)

# ================== CORE: STREAM + SAVE ==================
async def stream_and_save(message, request: web.Request,
                          *, prefer_dc: Optional[int] = None,
                          save_dir: Optional[str] = None) -> web.StreamResponse:
    global CURRENT_MAX_CHUNK  # <-- ensure global declared before any use

    # HEAD safety
    if request.method == "HEAD":
        return web.Response(
            status=200,
            headers={
                "Content-Type": "application/octet-stream",
                "Accept-Ranges": "bytes",
                "X-Downloader": "safe",
            },
        )

    save_dir = save_dir or SAVE_DIR
    os.makedirs(save_dir, exist_ok=True)

    # DC-aware client pick
    dc_hint = _dc_from_media(message)
    idx, client = await choose_best_client(prefer_dc or dc_hint)
    sem = get_sema(idx)
    if sem.locked():
        raise web.HTTPTooManyRequests(text="Server busy, try in 30–60s")

    await sem.acquire()
    _ensure_workloads()
    work_loads[idx] = work_loads.get(idx, 0) + 1

    # Producer shutdown switch
    stop_event = asyncio.Event()

    try:
        if not getattr(client, "_is_connected", False):
            try: await client.start()
            except Exception: pass

        # Build location & meta
        location = await _ensure_location(client, message)
        file_size, filename = _extract_meta(message)

        final_path = os.path.join(save_dir, filename)
        part_path  = final_path + ".part"

        key = key_for_message(message)
        await _init_state(key, {"size": file_size, "filename": filename})

        # Response headers (Content-Length → mobile UI shows total size)
        headers = {
            "Content-Type": "application/octet-stream",
            "Content-Disposition": f"attachment; filename*=UTF-8''{filename}",
            "Accept-Ranges": "bytes",
            "Connection": "keep-alive",
            "X-Downloader": "safe",
        }
        if file_size:
            headers["Content-Length"] = str(file_size)

        resp = web.StreamResponse(status=200, headers=headers)
        if file_size:
            resp.content_length = file_size
        await resp.prepare(request)

        # Optional resume on disk (server-side)
        offset = os.path.getsize(part_path) if os.path.exists(part_path) else 0
        f = open(part_path, "r+b" if offset else "wb")
        if offset: f.seek(offset)

        # Governor init
        gov = Governor()
        prof = gov.profile()
        chunk_size  = min(_round_4k(prof["chunk"]), CURRENT_MAX_CHUNK)
        write_batch = prof["batch"]
        backoff_cap = prof["cap"]

        # State
        st = _download_states[key]
        st["status"] = "running"; st["start"] = time.time()
        speed_hist: List[float] = []
        last_progress = time.time()
        consecutive_timeouts = 0
        empty_twice = 0
        retries = 0
        backoff = 1.0
        written_since_fsync = 0
        last_refetch = time.time()
        drain_acc = 0

        # Prefetch queue
        fetch_q: asyncio.Queue = asyncio.Queue(maxsize=max(1, PREFETCH))

        async def fetcher_lane(lane_idx: int):
            nonlocal location, file_size, chunk_size
            local_off = offset + lane_idx * chunk_size
            stride    = chunk_size * max(1, FETCHERS)
            while not stop_event.is_set():
                if file_size and local_off >= file_size:
                    await fetch_q.put(None); break

                remaining = (file_size - local_off) if file_size else chunk_size
                req = min(chunk_size, remaining, CURRENT_MAX_CHUNK)
                r = _round_4k(req)
                if r > remaining: r = remaining
                req = max(1, int(r))

                try:
                    data, location2 = await asyncio.wait_for(
                        _raw_getfile_chunk(client, location, local_off, req, message=message),
                        timeout=90
                    )
                    location = location2
                except asyncio.TimeoutError:
                    await fetch_q.put(("timeout", local_off)); continue
                except Exception as e:
                    await fetch_q.put(("error", e)); break

                if stop_event.is_set(): break
                if not data:
                    await fetch_q.put(("empty", local_off)); continue

                await fetch_q.put(("data", local_off, data))
                # spread lanes when size unknown
                local_off += len(data) if file_size else stride

        # spawn N lanes
        fetch_tasks = [asyncio.create_task(fetcher_lane(i)) for i in range(max(1, FETCHERS))]
        wb = bytearray()

        try:
            while True:
                # Stall guard
                if time.time() - last_progress > STALL_TIMEOUT:
                    retries += 1
                    await asyncio.sleep(min(backoff * 2, backoff_cap))
                    backoff = min(backoff * 2, backoff_cap)
                    last_progress = time.time()
                    if retries >= 4:
                        try: location = await _ensure_location(client, message)
                        except Exception: pass
                        if len(multi_clients) > 1:
                            idx = (idx + 1) % len(multi_clients)
                            client = multi_clients[idx]
                            try: await client.start()
                            except Exception: pass
                        retries = 0

                item = await fetch_q.get()
                if item is None:
                    break
                if isinstance(item, tuple):
                    tag = item[0]
                    if tag == "error":
                        raise item[1]
                    if tag == "timeout":
                        consecutive_timeouts += 1
                        retries += 1
                        await asyncio.sleep(min(backoff * 2, backoff_cap))
                        backoff = min(backoff * 2, backoff_cap)
                        if consecutive_timeouts >= 2:
                            if len(multi_clients) > 1:
                                idx = (idx + 1) % len(multi_clients)
                                client = multi_clients[idx]
                                try: await client.start()
                                except Exception: pass
                            try: location = await _ensure_location(client, message)
                            except Exception: pass
                            consecutive_timeouts = 0
                        continue
                    if tag == "empty":
                        empty_twice += 1
                        if empty_twice >= 2:
                            try: location = await _ensure_location(client, message)
                            except Exception: pass
                            empty_twice = 0
                        continue

                _, off0, chunk = item

                t0 = time.time()
                # stream to client
                try:
                    await resp.write(chunk)
                    drain_acc += len(chunk)
                    if drain_acc >= DRAIN_BYTES:
                        await resp.drain()
                        drain_acc = 0
                except (ConnectionResetError, ConnectionError, asyncio.CancelledError):
                    log.info("Client disconnected during safe stream; stopping writes.")
                    stop_event.set()
                    break

                # batched disk write
                wb += chunk
                if len(wb) >= write_batch or (file_size and off0 + len(chunk) >= file_size):
                    f.write(wb); f.flush()
                    written_since_fsync += len(wb)
                    wb.clear()
                    if written_since_fsync >= FSYNC_INTERVAL:
                        try: os.fsync(f.fileno())
                        except Exception: pass
                        written_since_fsync = 0

                # progress & stats
                offset = off0 + len(chunk)
                last_progress = time.time()
                retries = 0
                sp = (len(chunk)/1024/1024) / max(1e-3, time.time() - t0)
                speed_hist.append(sp)
                if len(speed_hist) > 20: speed_hist.pop(0)
                avg_speed = sum(speed_hist)/len(speed_hist)

                st["downloaded"] = offset
                st["size"] = file_size
                st["progress"] = (offset/file_size*100) if file_size else 0.0
                st["avg_speed"] = avg_speed
                st["last_update"] = time.time()

                # ETA calc
                eta_sec = None
                if file_size and avg_speed > 0:
                    remaining_bytes = max(0, file_size - offset)
                    eta_sec = remaining_bytes / (avg_speed * 1024 * 1024)
                st["eta_seconds"] = int(eta_sec) if eta_sec else None
                st["eta_human"] = (
                    f"{int(eta_sec//3600)}h {int((eta_sec%3600)//60)}m {int(eta_sec%60)}s"
                    if eta_sec else "calculating…"
                )

                # periodic refetch (keep file_reference fresh)
                if time.time() - last_refetch > 60:
                    try: location = await _ensure_location(client, message)
                    except Exception: pass
                    last_refetch = time.time()

                # RAM pressure guard
                if mem_pct() >= 80.0:
                    write_batch = max(WRITE_BATCH_SAFE, int(write_batch * 0.75))

                # governor adapt + enforce runtime cap + 4K align + tail safety
                waiters = (sem._value == 0)
                prof = gov.update(avg_speed, False, waiters)
                chunk_size  = max(CHUNK_SIZE_SAFE, min(CHUNK_SIZE_NORMAL, prof["chunk"]))
                write_batch = max(WRITE_BATCH_SAFE, min(WRITE_BATCH_NORMAL, prof["batch"]))
                backoff_cap = prof["cap"]
                chunk_size  = min(chunk_size, CURRENT_MAX_CHUNK)
                chunk_size  = _round_4k(chunk_size)
                if file_size:
                    remaining_hint = max(1, file_size - st.get("downloaded", 0))
                    if chunk_size > remaining_hint:
                        chunk_size = remaining_hint

        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
        except RPCError as e:
            s = str(e).upper()
            if "LIMIT_INVALID" in s:
                # DC refused >512KB → runtime cap drop
                if CURRENT_MAX_CHUNK > 512 * 1024:
                    CURRENT_MAX_CHUNK = 512 * 1024
                    log.warning("GetFile LIMIT_INVALID → runtime cap downgraded to 512KB")
                await asyncio.sleep(0.2)
            elif "FILE_REFERENCE" in s:
                try: location = await _ensure_location(client, message)
                except Exception: pass
            elif "MIGRATE" in s or "NETWORK_MIGRATE" in s or "PHONE_MIGRATE" in s:
                if len(multi_clients) > 1:
                    idx = (idx + 1) % len(multi_clients)
                    client = multi_clients[idx]
                    try: await client.start()
                    except Exception: pass
            else:
                log.warning(f"RPC error: {e}")
                await asyncio.sleep(jitter(1,1))
        except Exception as e:
            log.error(f"Download error: {e}")
            traceback.print_exc()
            raise
        finally:
            # stop producers cleanly
            try:
                stop_event.set()
                # cancel all lanes
                if 'fetch_tasks' in locals():
                    for t in fetch_tasks:
                        if not t.done():
                            t.cancel()
                    await asyncio.gather(*fetch_tasks, return_exceptions=True)
            except Exception: pass
            # flush to disk
            try:
                if wb:
                    f.write(wb); f.flush(); wb.clear()
                f.close()
            except Exception: pass

        # finalize file
        try: os.replace(part_path, final_path)
        except Exception:
            import shutil; shutil.move(part_path, final_path)

        st["status"] = "finished"
        st["end"] = time.time()
        st["progress"] = 100.0
        st["eta_seconds"] = 0
        st["eta_human"]  = "done"

        try:
            if drain_acc:
                await resp.drain()
            await resp.write_eof()
        except (ConnectionResetError, ConnectionError, asyncio.CancelledError):
            pass
        return resp

    finally:
        work_loads[idx] = max(0, work_loads.get(idx, 1) - 1)
        sem.release()

# ================== PUBLIC: /progress helper ==================
def get_state_snapshot(key: str) -> Dict[str, Any]:
    return _download_states.get(key, {})

# ================== CLI (optional: debug/benchmark) ==================
if __name__ == "__main__":
    import argparse
    import asyncio as _aio
    import math
    import sys

    parser = argparse.ArgumentParser(description="Safe downloader CLI (VPS-like)")
    parser.add_argument("--chat", type=int, required=True, help="Telegram chat id")
    parser.add_argument("--msg",  type=int, required=True, help="Message id with media")
    parser.add_argument("--out",  type=str, default=SAVE_DIR, help="Output folder")
    parser.add_argument("--name", type=str, default="", help="Override output filename (optional)")
    args = parser.parse_args()

    async def _run():
        global CURRENT_MAX_CHUNK  # <-- ensure global declared here too
        if not multi_clients:
            print("No pyrogram clients configured."); return
        c = multi_clients[0]
        await c.start()
        try:
            m = await c.get_messages(args.chat, args.msg)
            loc = await _ensure_location(c, m)
            fsz, sname = _extract_meta(m)
            if not loc:
                print("No media in message."); return

            fname = sanitize_filename(args.name or sname or f"file_{secrets.token_hex(4)}")
            out_dir = args.out or SAVE_DIR
            os.makedirs(out_dir, exist_ok=True)
            path = os.path.join(out_dir, fname)
            part = path + ".part"

            wrote = 0
            wb = bytearray()
            since_fsync = 0

            # start with 1MB (cap), downshift to 512KB on LIMIT_INVALID
            chunk = min(CHUNK_SIZE_NORMAL if not IS_PAAS else CHUNK_SIZE_SAFE, CURRENT_MAX_CHUNK)
            chunk = _round_4k(chunk)

            t0 = time.time()
            with open(part, "wb") as f:
                last_print = 0.0
                speed_hist = []

                while True:
                    if fsz and wrote >= fsz:
                        break

                    remaining = (fsz - wrote) if fsz else chunk
                    req = min(chunk, remaining, CURRENT_MAX_CHUNK)
                    r = _round_4k(req)
                    if r > remaining: r = remaining
                    req = max(1, int(r))

                    t_req = time.time()
                    try:
                        data, loc = await _raw_getfile_chunk(c, loc, wrote, req, message=m)
                    except RPCError as e:
                        s = str(e).upper()
                        if "LIMIT_INVALID" in s:
                            if CURRENT_MAX_CHUNK > 512 * 1024:
                                CURRENT_MAX_CHUNK = 512 * 1024
                                print("\nLIMIT_INVALID → cap=512KB")
                            continue
                        elif "FILE_REFERENCE" in s:
                            loc = await _ensure_location(c, m)
                            continue
                        else:
                            raise
                    except FileReferenceExpired:
                        loc = await _ensure_location(c, m)
                        continue

                    if not data:
                        break

                    # write buffer
                    wb += data
                    if len(wb) >= WRITE_BATCH_NORMAL:
                        f.write(wb); f.flush(); since_fsync += len(wb); wb.clear()
                        if since_fsync >= FSYNC_INTERVAL:
                            try: os.fsync(f.fileno())
                            except Exception: pass
                            since_fsync = 0

                    wrote += len(data)

                    # progress line
                    dt = time.time() - t_req
                    inst = (len(data)/1024/1024) / max(1e-3, dt)
                    speed_hist.append(inst)
                    if len(speed_hist) > 20: speed_hist.pop(0)
                    avg = sum(speed_hist)/len(speed_hist)

                    now = time.time()
                    if now - last_print >= 0.3:
                        last_print = now
                        done = wrote
                        total = fsz or 0
                        pct = (done/total*100) if total else 0.0
                        eta = None
                        if total and avg > 0:
                            eta = (total - done) / (avg * 1024 * 1024)

                        bar_len = 24
                        fill = int((pct/100.0) * bar_len) if total else int((done % (bar_len*1024*1024))/(1024*1024))
                        bar = "█"*fill + "░"*(bar_len-fill if bar_len-fill>0 else 0)

                        if total:
                            sys.stdout.write(
                                f"\r[{bar}] {pct:6.2f}%  "
                                f"{done/1024/1024:,.2f} / {total/1024/1024:,.2f} MB  "
                                f"avg {avg:5.2f} MB/s  "
                                f"ETA {int(eta//3600)}h {int((eta%3600)//60)}m {int(eta%60)}s" if eta else "\r"
                            )
                        else:
                            sys.stdout.write(
                                f"\r[{bar}]  {done/1024/1024:,.2f} MB  "
                                f"avg {avg:5.2f} MB/s"
                            )
                        sys.stdout.flush()

                if wb:
                    f.write(wb); f.flush(); wb.clear()

            try: os.replace(part, path)
            except Exception:
                import shutil; shutil.move(part, path)

            dt_all = time.time() - t0
            mb = wrote / 1024 / 1024
            sp = mb / max(1e-3, dt_all)
            print(f"\nSaved: {path}")
            print(f"Size : {mb:.2f} MB  |  Time: {dt_all:.2f}s  |  Avg: {sp:.2f} MB/s")

        finally:
            await c.stop()

    _aio.run(_run())
