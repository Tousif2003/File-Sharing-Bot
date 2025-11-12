# Thunder/server/stream_routes.py

import re
import secrets
import time
from urllib.parse import quote, unquote
import json
import asyncio
from aiohttp import web, ClientConnectionResetError 
from aiohttp.web_exceptions import HTTPInternalServerError, HTTPNotFound
import os

from Thunder import __version__, StartTime
from Thunder.bot import StreamBot, multi_clients, work_loads
from Thunder.server.exceptions import FileNotFound, InvalidHash
from Thunder.utils.custom_dl import ByteStreamer
from Thunder.utils.logger import logger
from Thunder.utils.render_template import render_page
from Thunder.utils.time_format import get_readable_time

# ✅ Safe Download functions & hybrid-lock support
from Thunder.utils.safe_download import (
    stream_and_save,
    get_state_snapshot,
    set_hybrid_lock,        # new
    clear_hybrid_lock,      # new
    is_hybrid_locked,       # new
    hybrid_lock_map,        # optional debug map
    is_persistent_locked,   # ✅ Mongo persistent hybrid lock (new)
    set_persistent_lock     # ✅ Mongo persistent hybrid lock (new)
)
from Thunder.utils import safe_download as _sd

routes = web.RouteTableDef()

SECURE_HASH_LENGTH = 6
CHUNK_SIZE = 1024 * 1024
MAX_CONCURRENT_PER_CLIENT = 8
RANGE_REGEX = re.compile(r"bytes=(?P<start>\d*)-(?P<end>\d*)")
PATTERN_HASH_FIRST = re.compile(
    rf"^([a-zA-Z0-9_-]{{{SECURE_HASH_LENGTH}}})(\d+)(?:/.*)?$")
PATTERN_ID_FIRST = re.compile(r"^(\d+)(?:/.*)?$")
VALID_HASH_REGEX = re.compile(r'^[a-zA-Z0-9_-]+$')

streamers = {}

async def ensure_client_started(cli):
    """Start only if not already connected; ignore 'already connected' errors."""
    try:
        if hasattr(cli, "is_connected"):
            if not cli.is_connected:
                await cli.start()
        else:
            await cli.start()
    except Exception as e:
        s = str(e).lower()
        if "already connected" in s or "already running" in s:
            pass
        else:
            raise

def choose_alt_client(curr_id: int):
    """
    Pick a different, least-loaded client for hybrid mode.
    Falls back to current client if no alternative exists.
    """
    try:
        candidates = [i for i in range(len(multi_clients)) if i != curr_id]
        if not candidates:
            return curr_id, multi_clients[curr_id]
        # prefer least loaded among the others
        best = min(candidates, key=lambda k: work_loads.get(k, 0))
        return best, multi_clients[best]
    except Exception:
        return curr_id, multi_clients[curr_id]

def select_optimal_client():
    """
    Least-loaded client choose kare. 
    Returns: (client_id, client_instance)
    """
    if not work_loads:
        return 0, multi_clients[0]
    cid = min(work_loads, key=lambda k: work_loads.get(k, 0))
    return cid, multi_clients[cid]

def parse_range_header(range_header: str, file_size: int):
    """
    RFC7233-ish parser. Returns (start, end).
    If no/invalid Range -> full file.
    Ensures bounds within [0, file_size-1].
    """
    if not range_header or not range_header.startswith("bytes=") or not file_size:
        return 0, max(0, file_size - 1)

    m = re.match(r"bytes=(\d+)-(\d+)?", range_header)
    if not m:
        return 0, max(0, file_size - 1)

    start = int(m.group(1))
    end = int(m.group(2)) if m.group(2) else file_size - 1

    if start < 0:
        start = 0
    if end >= file_size:
        end = file_size - 1
    if start > end:
        # invalid range → fall back to full file
        start, end = 0, file_size - 1
    return start, end

def parse_media_request(path, query):
    """
    Local fallback parser to extract message_id and secure_hash.
    Supports paths like: /<message_id>/<file_name>?hash=<secure_hash>
    """
    parts = path.split("/", 1)
    message_id = parts[0]
    secure_hash = query.get("hash") or query.get("h") or ""
    if not secure_hash and len(message_id) >= 6:
        secure_hash = message_id[:6]
    return message_id, secure_hash

@routes.get("/hybrid-test")
async def hybrid_test(request):
    logger.warning("🧪 HYBRID TEST: Triggering safe_download handshake check...")
    try:
        # Try to import the safe download handler
        from Thunder.utils.safe_download import stream_and_save

        if callable(stream_and_save):
            logger.info("✅ SafeDownload module imported successfully & ready!")
            return web.Response(text="✅ SafeDownload module imported successfully & ready!")
        else:
            logger.error("❌ SafeDownload found but not callable!")
            return web.Response(text="❌ SafeDownload found but not callable!")

    except Exception as e:
        logger.exception(f"❌ SafeDownload import failed: {e}")
        return web.Response(text=f"❌ SafeDownload import failed: {e}")

def get_streamer(client_id: int) -> ByteStreamer:
    if client_id not in streamers:
        streamers[client_id] = ByteStreamer(multi_clients[client_id])
    return streamers[client_id]


def parse_media_request(path: str, query: dict) -> tuple[int, str]:
    clean_path = unquote(path).strip('/')

    match = PATTERN_HASH_FIRST.match(clean_path)
    if match:
        try:
            message_id = int(match.group(2))
            secure_hash = match.group(1)
            if (len(secure_hash) == SECURE_HASH_LENGTH and
                    VALID_HASH_REGEX.match(secure_hash)):
                return message_id, secure_hash
        except ValueError as e:
            raise InvalidHash(f"Invalid message ID format in path: {e}") from e

    match = PATTERN_ID_FIRST.match(clean_path)
    if match:
        try:
            message_id = int(match.group(1))
            secure_hash = query.get("hash", "").strip()
            if (len(secure_hash) == SECURE_HASH_LENGTH and
                    VALID_HASH_REGEX.match(secure_hash)):
                return message_id, secure_hash
            else:
                raise InvalidHash("Invalid or missing hash in query parameter")
        except ValueError as e:
            raise InvalidHash(f"Invalid message ID format in path: {e}") from e

    raise InvalidHash("Invalid URL structure or missing hash")


def select_optimal_client() -> tuple[int, ByteStreamer]:
    if not work_loads:
        raise web.HTTPInternalServerError(
            text=("No available clients to handle the request. "
                  "Please try again later."))

    available_clients = [
        (cid, load) for cid, load in work_loads.items()
        if load < MAX_CONCURRENT_PER_CLIENT]

    if available_clients:
        client_id = min(available_clients, key=lambda x: x[1])[0]
    else:
        client_id = min(work_loads, key=work_loads.get)

    return client_id, get_streamer(client_id)


def parse_range_header(range_header: str, file_size: int) -> tuple[int, int]:
    if not range_header:
        return 0, file_size - 1

    match = RANGE_REGEX.match(range_header)
    if not match:
        raise web.HTTPBadRequest(text=f"Invalid range header: {range_header}")

    start_str = match.group("start")
    end_str = match.group("end")
    if start_str:
        start = int(start_str)
        end = int(end_str) if end_str else file_size - 1
    else:
        if not end_str:
            raise web.HTTPBadRequest(text=f"Invalid range header: {range_header}")
        suffix_len = int(end_str)
        if suffix_len <= 0:
            raise web.HTTPRequestRangeNotSatisfiable(headers={"Content-Range": f"bytes */{file_size}"})
        start = max(file_size - suffix_len, 0)
        end = file_size - 1

    if start < 0 or end >= file_size or start > end:
        raise web.HTTPRequestRangeNotSatisfiable(
            headers={"Content-Range": f"bytes */{file_size}"}
        )

    return start, end


@routes.get("/", allow_head=True)

async def root_redirect(request):
    raise web.HTTPFound("https://telegram.me/FilmyMod123")


@routes.get("/statxx", allow_head=True)
async def status_endpoint(request):
    uptime = time.time() - StartTime
    total_load = sum(work_loads.values())

    workload_distribution = {str(k): v for k, v in sorted(work_loads.items())}

    return web.Response(
        text=json.dumps({
            "server": {
                "status": "operational",
                "version": __version__,
                "uptime": get_readable_time(uptime)
            },
            "telegram_bot": {
                "username": f"@{StreamBot.username}",
                "active_clients": len(multi_clients)
            },
            "resources": {
                "total_workload": total_load,
                "workload_distribution": workload_distribution
            }
        }, indent=2),
        content_type='application/json'
    )

@routes.get(r"/watch/{path:.+}", allow_head=True)
async def media_preview(request: web.Request):
    try:
        path = request.match_info["path"]
        message_id, secure_hash = parse_media_request(path, request.query)

        rendered_page = await render_page(
            message_id, secure_hash, requested_action='stream')
        return web.Response(text=rendered_page, content_type='text/html')

    except (InvalidHash, FileNotFound) as e:
        logger.debug(
            f"Client error in preview: {type(e).__name__} - {e}",
            exc_info=True)
        raise web.HTTPNotFound(text="Resource not found") from e
    except Exception as e:

        error_id = secrets.token_hex(6)
        logger.error(f"Preview error {error_id}: {e}", exc_info=True)
        raise web.HTTPInternalServerError(
            text=f"Server error occurred: {error_id}") from e


@routes.get(r"/{path:.+}", allow_head=True)
async def media_delivery(request: web.Request):
    # small helper: start client safely (no crash if already running)
    async def ensure_client_started(cli):
        try:
            already = bool(getattr(cli, "is_connected", False) or getattr(cli, "_is_connected", False))
            if not already:
                await cli.start()
        except Exception as e:
            s = str(e).lower()
            if "already connected" in s or "already running" in s:
                pass
            else:
                raise

    # --- Hybrid lock (unique file-id) store (in-proc, per-app) ---
    import time
    _HYBRID_LOCKS = getattr(media_delivery, "_HYBRID_LOCKS", {})
    media_delivery._HYBRID_LOCKS = _HYBRID_LOCKS  # persist across calls in same process

    def _hybrid_lock_set(uid: str, ttl: int, file_name: str = ""):
        """
        Set lock for this unique_id only. Logs include filename & TTL hours.
        """
        now = time.time()
        _HYBRID_LOCKS[uid] = now + ttl
        # periodic cleanup
        if len(_HYBRID_LOCKS) % 64 == 0:
            expired = [k for k, v in _HYBRID_LOCKS.items() if v <= now]
            for k in expired:
                _HYBRID_LOCKS.pop(k, None)
        hrs = round(ttl / 3600, 2)
        fn = (file_name or "").strip()
        if fn:
            logger.info(f"🔒 [HybridLock] Set {uid[:10]}... for {hrs}h | file='{fn}'")
        else:
            logger.info(f"🔒 [HybridLock] Set {uid[:10]}... for {hrs}h")

    def _hybrid_lock_active(uid: str) -> bool:
        exp = _HYBRID_LOCKS.get(uid)
        if not exp:
            return False
        if exp > time.time():
            return True
        _HYBRID_LOCKS.pop(uid, None)
        return False

    try:
        path = request.match_info["path"]
        message_id, secure_hash = parse_media_request(path, request.query)

        # —— tunables via env ——
        SAFE_SWITCH_THRESHOLD_MBPS = float(os.getenv("SAFE_SWITCH_THRESHOLD_MBPS", "4"))
        SAFE_SWITCH_DURATION_SEC   = int(os.getenv("SAFE_SWITCH_DURATION_SEC", "4"))
        ASYNC_YIELD_INTERVAL_MB    = int(os.getenv("ASYNC_YIELD_INTERVAL_MB", "1"))
        USE_ALT_CLIENT             = os.getenv("SAFE_SWITCH_USE_ALT_CLIENT", "true").lower() == "true"
        STALL_DEADLINE_SEC         = int(os.getenv("STALL_DEADLINE_SEC", "15"))

        # ✅ Adaptive chunking (env-driven)
        AD_CHUNK_NORMAL       = int(os.getenv("CHUNK_SIZE_NORMAL",  str(1 * 1024 * 1024)))  # 1MB
        AD_CHUNK_FALLBACK     = int(os.getenv("CHUNK_SIZE_FALLBACK", str(768 * 1024)))      # 768KB
        AD_DOWNGRADE_TIMEOUTS = int(os.getenv("DOWNGRADE_TIMEOUTS", "1"))                   # timeouts to drop size
        AD_LOW_MBPS           = float(os.getenv("LOW_SPEED_MBPS_THRESHOLD", "2.5"))         # streak reset only
        AD_RECOVER_MBPS       = float(os.getenv("RECOVERY_MBPS_THRESHOLD", "5.5"))          # upscale threshold
        AD_GOOD_SEC           = int(os.getenv("GOOD_SECONDS_TO_UPSCALE", "6"))              # seconds to upscale

        # 🔒 Hybrid lock TTL (4 hours default)
        HYBRID_LOCK_TTL = int(os.getenv("HYBRID_LOCK_TTL", "14400"))

        # 🧠 Mid-stream switch knobs
        MIDSTREAM_MIN_MB = float(os.getenv("MIDSTREAM_MIN_MB", "8"))   # after these MB sent, allow midstream switch
        MIDSTREAM_SLOW_SEC = int(os.getenv("MIDSTREAM_SLOW_SEC", "4"))   # consecutive slow seconds to trigger

        def _ad_log(msg: str):
            logger.info(f"⚙️ [Adaptive] {msg}")

        # Choose least-loaded client
        client_id, streamer = select_optimal_client()
        # ensure workload accounting is safe
        work_loads.setdefault(client_id, 0)
        work_loads[client_id] += 1

        try:
            # Warm connection (best-effort)
            try:
                await streamer.get_me()
            except Exception:
                pass

            # File info + hash check
            file_info = await streamer.get_file_info(message_id)
            if not file_info.get("unique_id"):
                raise FileNotFound("File unique ID not found.")
            if file_info["unique_id"][:SECURE_HASH_LENGTH] != secure_hash:
                raise InvalidHash("Hash mismatch with file unique ID.")

            file_uid = str(file_info["unique_id"])      # for hybrid lock (per-file)
            file_size = int(file_info.get("file_size") or 0)
            if file_size <= 0:
                raise FileNotFound("File size unavailable or zero.")

            filename  = (file_info.get("file_name") or f"file_{secrets.token_hex(4)}").strip()
            encoded_filename = quote(filename)

            # ✅ Persistent Mongo check first (best-effort)
            try:
                # prefer top-level import; fallback to inline import if not present
                try:
                    locked = await is_persistent_locked(file_uid)
                except NameError:
                    from Thunder.utils.safe_download import is_persistent_locked as _sd_is_persistent_locked
                    locked = await _sd_is_persistent_locked(file_uid)
                if locked:
                    logger.info(f"🔒 Mongo persistent lock active for {file_uid[:10]}... → forcing SafeDownload path.")
                    try:
                        # robust chat_id resolution
                        raw_chat = getattr(StreamBot, "chat_id", None) or request.query.get("chat_id") or os.getenv("BIN_CHANNEL")
                        logger.info(f"DEBUG resolved chat_id raw={raw_chat!r}")

                        if not raw_chat:
                            raise FileNotFound("Chat ID unavailable for fallback.")

                        raw_chat_s = str(raw_chat).strip()
                        try:
                            chat_id = int(raw_chat_s.replace("@", ""))
                        except Exception:
                            chat_id = raw_chat_s  # username allowed

                        keys = list(multi_clients.keys())
                        try:
                            cur_ix = keys.index(client_id)
                        except ValueError:
                            cur_ix = 0
                        alt_id = keys[(cur_ix + 1) % len(keys)] if (USE_ALT_CLIENT and len(keys) > 1) else client_id
                        hybrid_cli = multi_clients[alt_id]

                        await ensure_client_started(hybrid_cli)
                        msg = await hybrid_cli.get_messages(chat_id, int(message_id))
                        if not msg:
                            raise FileNotFound(f"Failed to fetch message {message_id} from chat {chat_id}")

                        # refresh lock for this uid so whole download stays hybrid
                        _hybrid_lock_set(file_uid, HYBRID_LOCK_TTL, filename)
                        # twin-write to safe_download in-process map (best-effort)
                        try:
                            from Thunder.utils.safe_download import set_hybrid_lock as _sd_set_hybrid_lock
                            try:
                                _sd_set_hybrid_lock(file_uid, HYBRID_LOCK_TTL, filename)
                                logger.info("🔁 Also wrote hybrid lock to safe_download's map for cross-path visibility")
                            except Exception as e:
                                logger.warning(f"Could not write to safe_download's lock map: {e}")
                        except Exception:
                            logger.debug("safe_download.set_hybrid_lock not importable; skipped twin-write")

                        # ensure persistent lock stays refreshed (best-effort)
                        try:
                            try:
                                await set_persistent_lock(file_uid, ttl_seconds=HYBRID_LOCK_TTL, file_name=filename, by="system")
                            except NameError:
                                from Thunder.utils.safe_download import set_persistent_lock as _sd_set_persistent
                                await _sd_set_persistent(file_uid, ttl_seconds=HYBRID_LOCK_TTL, file_name=filename, by="system")
                            logger.info("🔁 Also refreshed persistent lock in Mongo.")
                        except Exception as e:
                            logger.debug(f"Could not refresh persistent lock in Mongo: {e}")

                        return await stream_and_save(msg, request)

                    except Exception as e:
                        logger.warning(
                            f"Mongo persistent fallback failed for uid={file_uid[:8]}... chat_id={request.query.get('chat_id')} "
                            f"message_id={message_id} error={e}"
                        )
                        # fallthrough to normal streaming
            except Exception:
                # if DB check errors, continue with normal flow (no crash)
                logger.debug("Persistent lock check error — continuing normal stream")

            # ✅ Hybrid lock: if already active for THIS uid, force safe download path now
            if _hybrid_lock_active(file_uid):
                logger.info(f"🔒 Hybrid lock active for {file_uid[:10]}... → forcing SafeDownload path.")
                try:
                    # robust chat_id resolution
                    raw_chat = getattr(StreamBot, "chat_id", None) or request.query.get("chat_id") or os.getenv("BIN_CHANNEL")
                    logger.info(f"DEBUG resolved chat_id raw={raw_chat!r}")

                    if not raw_chat:
                        raise FileNotFound("Chat ID unavailable for fallback.")

                    raw_chat_s = str(raw_chat).strip()
                    try:
                        chat_id = int(raw_chat_s.replace("@", ""))
                    except Exception:
                        chat_id = raw_chat_s  # username allowed

                    keys = list(multi_clients.keys())
                    try:
                        cur_ix = keys.index(client_id)
                    except ValueError:
                        cur_ix = 0
                    alt_id = keys[(cur_ix + 1) % len(keys)] if (USE_ALT_CLIENT and len(keys) > 1) else client_id
                    hybrid_cli = multi_clients[alt_id]

                    await ensure_client_started(hybrid_cli)
                    msg = await hybrid_cli.get_messages(chat_id, int(message_id))
                    if not msg:
                        raise FileNotFound(f"Failed to fetch message {message_id} from chat {chat_id}")

                    # refresh lock for this uid so whole download stays hybrid
                    _hybrid_lock_set(file_uid, HYBRID_LOCK_TTL, filename)
                    # also attempt to write the canonical lock in safe_download (best-effort)
                    try:
                        from Thunder.utils.safe_download import set_hybrid_lock as _sd_set_hybrid_lock
                        try:
                            _sd_set_hybrid_lock(file_uid, HYBRID_LOCK_TTL, filename)
                            logger.info("🔁 Also wrote hybrid lock to safe_download's map for cross-path visibility")
                        except Exception as e:
                            logger.warning(f"Could not write to safe_download's lock map: {e}")
                    except Exception:
                        logger.debug("safe_download.set_hybrid_lock not importable; skipped twin-write")

                    # also ensure persistent lock present (best-effort)
                    try:
                        try:
                            await set_persistent_lock(file_uid, ttl_seconds=HYBRID_LOCK_TTL, file_name=filename, by="system")
                        except NameError:
                            from Thunder.utils.safe_download import set_persistent_lock as _sd_set_persistent
                            await _sd_set_persistent(file_uid, ttl_seconds=HYBRID_LOCK_TTL, file_name=filename, by="system")
                        logger.info("🔁 Also wrote persistent lock to Mongo for cross-worker visibility.")
                    except Exception as e:
                        logger.warning(f"Mongo persistent lock write failed: {e}")

                    return await stream_and_save(msg, request)

                except Exception as e:
                    logger.warning(
                        f"Hybrid lock fallback failed for uid={file_uid[:8]}... chat_id={request.query.get('chat_id')} "
                        f"message_id={message_id} error={e}"
                    )
                    # fallthrough to normal streaming

            # ===== Range parsing =====
            range_header = request.headers.get("Range", "")
            start, end = parse_range_header(range_header, file_size)
            content_length = end - start + 1

            # Full-file → 200
            full_range = (start == 0 and end == file_size - 1)
            if full_range:
                range_header = ""

            # Headers
            status = 200 if not range_header else 206
            headers = {
                "Content-Type": "application/octet-stream",
                "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}",
                "Accept-Ranges": "bytes",
                "Cache-Control": "public, max-age=31536000, immutable",
                "Connection": "keep-alive",
                "X-Content-Type-Options": "nosniff",
                "Referrer-Policy": "strict-origin-when-cross-origin",
                "X-Accel-Buffering": "no",
                "Content-Length": str(content_length),
            }
            if status == 206:
                headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

            # HEAD
            if request.method == "HEAD":
                return web.Response(status=status, headers=headers)

            # Prepare stream
            response = web.StreamResponse(status=status, headers=headers)
            await response.prepare(request)

            # Streaming + adaptive state
            _ad_cur_chunk   = AD_CHUNK_NORMAL
            _ad_timeouts    = 0
            _ad_good_streak = 0

            CLIENT_CHUNK_SIZE = _ad_cur_chunk
            write_buffer = bytearray()
            bytes_sent = 0
            bytes_to_skip = start % CLIENT_CHUNK_SIZE
            dc_retries = 0

            # Speed monitor
            last_check = time.time()
            last_bytes = 0
            slow_count = 0

            # Stall watchdog
            last_progress_ts = time.time()

            # midstream threshold (bytes)
            _midstream_min_bytes = int(MIDSTREAM_MIN_MB * 1024 * 1024)

            while True:
                try:
                    async for chunk in streamer.stream_file(
                        message_id,
                        offset=start + bytes_sent,
                        limit=content_length - bytes_sent
                    ):
                        # align
                        if bytes_to_skip:
                            if len(chunk) <= bytes_to_skip:
                                bytes_to_skip -= len(chunk)
                                continue
                            chunk = chunk[bytes_to_skip:]
                            bytes_to_skip = 0

                        # bound to requested range
                        remaining = content_length - bytes_sent
                        if len(chunk) > remaining:
                            chunk = chunk[:remaining]

                        if chunk:
                            last_progress_ts = time.time()

                        write_buffer.extend(chunk)
                        if len(write_buffer) >= CLIENT_CHUNK_SIZE:
                            try:
                                await response.write(write_buffer)
                                await response.drain()
                                write_buffer = bytearray()

                                if bytes_sent and bytes_sent % (ASYNC_YIELD_INTERVAL_MB * CLIENT_CHUNK_SIZE) == 0:
                                    await asyncio.sleep(0)
                            except (ConnectionResetError, ClientConnectionResetError, ConnectionError):
                                logger.warning("⚠️ Client disconnected mid-stream.")
                                break
                            except BufferError:
                                logger.warning("⚠️ Buffer conflict detected — recreating buffer.")
                                write_buffer = bytearray()

                        bytes_sent += len(chunk)

                        # ---- speed / adaptive / hybrid triggers ----
                        now = time.time()
                        if now - last_check >= 1:
                            elapsed = now - last_check
                            downloaded = bytes_sent - last_bytes
                            speed_MBps = (downloaded / (1024 * 1024)) / max(0.001, elapsed)

                            # adaptive streaks
                            if speed_MBps < AD_LOW_MBPS:
                                _ad_good_streak = 0
                            else:
                                _ad_good_streak += 1

                            # restore to normal chunk
                            if (_ad_cur_chunk == AD_CHUNK_FALLBACK
                                and _ad_good_streak >= AD_GOOD_SEC
                                and speed_MBps >= AD_RECOVER_MBPS):
                                _ad_cur_chunk = AD_CHUNK_NORMAL
                                CLIENT_CHUNK_SIZE = _ad_cur_chunk
                                _ad_log(f"Chunk size restored → {AD_CHUNK_NORMAL // 1024} KB "
                                        f"(stable {speed_MBps:.2f} MB/s for {AD_GOOD_SEC}s)")
                                _ad_good_streak = 0

                            # pre-send windowing
                            if speed_MBps < SAFE_SWITCH_THRESHOLD_MBPS:
                                slow_count += 1
                            else:
                                slow_count = 0

                            logger.info(f"⚙️ Stream speed: {speed_MBps:.2f} MB/s | SlowCount={slow_count}")

                            # PRE-SEND hybrid (old behavior)
                            if slow_count >= SAFE_SWITCH_DURATION_SEC and bytes_sent == 0 and len(write_buffer) == 0:
                                logger.warning(
                                    f"⚡ Speed too low ({speed_MBps:.2f} MB/s) — switching to SafeDownload hybrid mode (pre-send)."
                                )
                                try:
                                    # robust chat_id resolution
                                    raw_chat = getattr(StreamBot, "chat_id", None) or request.query.get("chat_id") or os.getenv("BIN_CHANNEL")
                                    logger.info(f"DEBUG resolved chat_id raw={raw_chat!r}")

                                    if not raw_chat:
                                        raise FileNotFound("Chat ID unavailable for fallback.")

                                    raw_chat_s = str(raw_chat).strip()
                                    try:
                                        chat_id = int(raw_chat_s.replace("@", ""))
                                    except Exception:
                                        chat_id = raw_chat_s  # username allowed

                                    keys = list(multi_clients.keys())
                                    try:
                                        cur_ix = keys.index(client_id)
                                    except ValueError:
                                        cur_ix = 0
                                    alt_id = keys[(cur_ix + 1) % len(keys)] if (USE_ALT_CLIENT and len(keys) > 1) else client_id
                                    hybrid_cli = multi_clients[alt_id]
                                    if alt_id != client_id:
                                        logger.info(f"🔁 Hybrid using alternate client ID {alt_id}.")

                                    await ensure_client_started(hybrid_cli)
                                    msg = await hybrid_cli.get_messages(chat_id, int(message_id))
                                    if not msg:
                                        raise FileNotFound(f"Failed to fetch message {message_id} from chat {chat_id}")

                                    _hybrid_lock_set(file_uid, HYBRID_LOCK_TTL, filename)
                                    # twin-write to canonical map if possible
                                    try:
                                        from Thunder.utils.safe_download import set_hybrid_lock as _sd_set_hybrid_lock
                                        try:
                                            _sd_set_hybrid_lock(file_uid, HYBRID_LOCK_TTL, filename)
                                            logger.info("🔁 Also wrote hybrid lock to safe_download's map for cross-path visibility")
                                        except Exception as e:
                                            logger.warning(f"Could not write to safe_download's lock map: {e}")
                                    except Exception:
                                        logger.debug("safe_download.set_hybrid_lock not importable; skipped twin-write")

                                    # also persist to Mongo (best-effort)
                                    try:
                                        try:
                                            await set_persistent_lock(file_uid, ttl_seconds=HYBRID_LOCK_TTL, file_name=filename, by="system")
                                        except NameError:
                                            from Thunder.utils.safe_download import set_persistent_lock as _sd_set_persistent
                                            await _sd_set_persistent(file_uid, ttl_seconds=HYBRID_LOCK_TTL, file_name=filename, by="system")
                                        logger.info("🔁 Also wrote persistent lock to Mongo for cross-worker visibility.")
                                    except Exception as e:
                                        logger.warning(f"Mongo persistent lock write failed: {e}")

                                    return await stream_and_save(msg, request)

                                except Exception as e:
                                    logger.error(f"Hybrid switch failed (continuing normal stream): {e}")

                            # MID-STREAM hybrid (new behavior)
                            if (bytes_sent >= _midstream_min_bytes) and (slow_count >= MIDSTREAM_SLOW_SEC):
                                logger.warning(
                                    f"⚡ Midstream slow ({speed_MBps:.2f} MB/s for {slow_count}s, "
                                    f"sent ~{bytes_sent/1_048_576:.1f}MB) → setting hybrid lock & closing for resume."
                                )
                                _hybrid_lock_set(file_uid, HYBRID_LOCK_TTL, filename)
                                # also attempt twin-write to canonical safe_download map (best-effort)
                                try:
                                    from Thunder.utils.safe_download import set_hybrid_lock as _sd_set_hybrid_lock
                                    try:
                                        _sd_set_hybrid_lock(file_uid, HYBRID_LOCK_TTL, filename)
                                        logger.info("🔁 Also wrote hybrid lock to safe_download's map for cross-path visibility")
                                    except Exception as e:
                                        logger.warning(f"Could not write to safe_download's lock map: {e}")
                                except Exception:
                                    logger.debug("safe_download.set_hybrid_lock not importable; skipped twin-write")

                                # also persist to Mongo (best-effort)
                                try:
                                    try:
                                        await set_persistent_lock(file_uid, ttl_seconds=HYBRID_LOCK_TTL, file_name=filename, by="system")
                                    except NameError:
                                        from Thunder.utils.safe_download import set_persistent_lock as _sd_set_persistent
                                        await _sd_set_persistent(file_uid, ttl_seconds=HYBRID_LOCK_TTL, file_name=filename, by="system")
                                    logger.info("🔁 Also wrote persistent lock to Mongo for cross-worker visibility.")
                                except Exception as e:
                                    logger.warning(f"Mongo persistent lock write failed: {e}")

                                # flush and close current response
                                try:
                                    if write_buffer:
                                        await response.write(write_buffer)
                                        await response.drain()
                                except Exception:
                                    pass
                                try:
                                    await response.write_eof()
                                except Exception:
                                    pass

                                # return a tiny 206 so client/proxy surely does Range resume
                                return web.Response(status=206, text="Hybrid switch triggered; please resume download.")

                            # STALL WATCHDOG
                            if (now - last_progress_ts) > STALL_DEADLINE_SEC and bytes_sent < content_length:
                                logger.warning(
                                    f"⏳ No progress for {now - last_progress_ts:.1f}s "
                                    f"(sent {bytes_sent}/{content_length}). Closing to trigger client resume."
                                )
                                try:
                                    if write_buffer:
                                        await response.write(write_buffer)
                                        await response.drain()
                                except Exception:
                                    pass
                                try:
                                    await response.write_eof()
                                except Exception:
                                    pass
                                return web.Response(status=206, text="Stall detected; please resume download.")

                            last_check = now
                            last_bytes = bytes_sent

                        if bytes_sent >= content_length:
                            break

                    break  # success; leave retry loop

                except Exception as e:
                    # Adaptive: downgrade on TIMEOUTs
                    if "TIMEOUT" in str(e).upper():
                        _ad_timeouts += 1
                        if _ad_timeouts >= AD_DOWNGRADE_TIMEOUTS and _ad_cur_chunk != AD_CHUNK_FALLBACK:
                            _ad_cur_chunk = AD_CHUNK_FALLBACK
                            CLIENT_CHUNK_SIZE = _ad_cur_chunk
                            _ad_log(f"Chunk size downgraded → {AD_CHUNK_FALLBACK // 1024} KB (timeouts hit: {AD_DOWNGRADE_TIMEOUTS})")
                            _ad_timeouts = 0
                        await asyncio.sleep(0.5)
                        continue

                    # DC migration hints
                    if any(dc in str(e) for dc in ["PHONE_MIGRATE", "NETWORK_MIGRATE", "USER_MIGRATE", "FILE_MIGRATE"]):
                        dc_retries += 1
                        if dc_retries > 3:
                            raise
                        logger.warning(f"🌐 DC mismatch detected — reconnecting attempt {dc_retries}")
                        await asyncio.sleep(1.5)
                        continue
                    else:
                        raise

            # Final flush
            if write_buffer:
                try:
                    await response.write(write_buffer)
                    await response.drain()
                except (ConnectionResetError, ClientConnectionResetError, ConnectionError):
                    logger.warning("⚠️ Client disconnected during final flush.")

            try:
                await response.write_eof()
            except (ConnectionResetError, ClientConnectionResetError, ConnectionError):
                logger.info("Client closed connection before EOF; ignoring.")

            return response  # ✅ success path

        except (FileNotFound, InvalidHash):
            raise
        except Exception as e:
            error_id = secrets.token_hex(6)
            logger.error(f"Stream error {error_id}: {e}", exc_info=True)
            raise web.HTTPInternalServerError(text=f"Streaming error: {error_id}") from e
        finally:
            work_loads[client_id] = max(0, work_loads.get(client_id, 0) - 1)

    except (InvalidHash, FileNotFound):
        raise web.HTTPNotFound(text="Resource not found")
    except Exception as e:
        error_id = secrets.token_hex(6)
        logger.error(f"Server error {error_id}: {e}", exc_info=True)
        raise web.HTTPInternalServerError(text=f"Unexpected server error: {error_id}") from e
# ============================================================
# 🧩 Hybrid Lock Control & Debug Routes (for testing)
# ============================================================
@routes.get("/force-lock")
async def force_hybrid_lock(request: web.Request):
    """
    Manually create a hybrid lock for testing.
    Usage:
      /force-lock?id=<file_unique_id>[&name=OptionalFileName][&ttl=<seconds>]
    Notes:
      - If 'name' given, it's shown in logs only (not stored).
      - TTL uses env HYBRID_LOCK_TTL (default 4h) unless ?ttl is provided.
    """
    import os, time
    from Thunder.utils.logger import logger

    uid  = (request.query.get("id") or "").strip()
    name = (request.query.get("name") or "").strip()
    ttl_q = request.query.get("ttl")
    try:
        ttl = int(ttl_q) if ttl_q else int(os.getenv("HYBRID_LOCK_TTL", "14400"))  # 4h default
    except Exception:
        ttl = 14400

    if not uid:
        return web.Response(text="⚠️ Please supply ?id=<file_unique_id>")

    # Prefer helper functions if present, else fallback to map
    try:
        from Thunder.utils.safe_download import set_hybrid_lock
        set_hybrid_lock(uid, ttl)
        hrs = round(ttl / 3600, 2)
        if name:
            logger.info(f"🔒 [ForceLock] Manually added lock for {uid[:10]}... ({hrs}h) | file='{name}'")
        else:
            logger.info(f"🔒 [ForceLock] Manually added lock for {uid[:10]}... ({hrs}h)")
    except Exception:
        # Fallback: direct map write
        try:
            from Thunder.utils.safe_download import hybrid_lock_map
            hybrid_lock_map[uid] = time.time() + ttl
            hrs = round(ttl / 3600, 2)
            if name:
                logger.info(f"🔒 [ForceLock] Manually added lock for {uid[:10]}... ({hrs}h) | file='{name}'")
            else:
                logger.info(f"🔒 [ForceLock] Manually added lock for {uid[:10]}... ({hrs}h)")
        except Exception as e:
            return web.Response(text=f"❌ Failed to force-lock: {e}")

    disp = f"{uid[:10]}..."
    suffix = f" | file='{name}'" if name else ""
    return web.Response(text=f"✅ Forced hybrid lock added for: {disp} (expires in {round(ttl/3600,2)}h){suffix}")


@routes.get("/unlock-lock")
async def unlock_hybrid_lock(request: web.Request):
    """
    Manually remove a hybrid lock by UID.
    Usage:
      /unlock-lock?id=<file_unique_id>
    """
    from Thunder.utils.logger import logger

    uid = (request.query.get("id") or "").strip()
    if not uid:
        return web.Response(text="⚠️ Please specify ?id=<file_unique_id> to unlock.")

    # Prefer helper if present
    try:
        from Thunder.utils.safe_download import clear_hybrid_lock
        removed = clear_hybrid_lock(uid)
    except Exception:
        # Fallback to map
        try:
            import time as _t
            from Thunder.utils.safe_download import hybrid_lock_map
            exp = hybrid_lock_map.pop(uid, None)
            removed = exp is not None and exp > _t.time()
        except Exception as e:
            return web.Response(text=f"❌ Failed to unlock: {e}")

    if removed:
        logger.info(f"🔓 [ForceUnlock] Lock removed for {uid[:10]}...")
        return web.Response(text=f"✅ Lock removed for {uid[:10]}...")
    else:
        return web.Response(text=f"❌ No active lock found for {uid[:10]}.")


@routes.get("/hybrid-locks")
async def hybrid_locks_debug(request: web.Request):
    """
    View currently active hybrid locks and expiry info.
    Usage:
      /hybrid-locks
    """
    import time
    from Thunder.utils.logger import logger

    # Always fallback-safe
    try:
        from Thunder.utils.safe_download import hybrid_lock_map
    except Exception as e:
        return web.Response(text=f"❌ Cannot access lock map: {e}")

    if not hybrid_lock_map:
        return web.Response(text="❌ No active hybrid locks.")

    now = time.time()
    rows = []
    for uid, exp in list(hybrid_lock_map.items()):
        left_h = max(0.0, round((exp - now) / 3600, 2))
        rows.append(f"{uid[:10]}... → expires in {left_h}h")

    logger.info("🧠 Hybrid lock status route called")
    return web.Response(text="\n".join(rows))

