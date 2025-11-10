# Thunder/utils/safe_download.py
# VPS-like downloader: DC-aware, single-lane raw chunking (upload.GetFile),
# proper HTTP Range/HEAD, forced download headers, stall watchdog,
# and now ADAPTIVE CHUNKING to survive TG -500 TIMEOUT & slow paths.

import os
import asyncio
import logging
import time
from typing import Tuple, Optional, Dict

from aiohttp import web
from pyrogram.errors import FileReferenceExpired, FileMigrate, FloodWait
from pyrogram.raw.functions.upload import GetFile
from pyrogram.raw.types import (
    InputDocumentFileLocation,
    InputPhotoFileLocation,
)

logger = logging.getLogger("ThunderBot")

# =========================
# Tunables (env overrides)
# =========================
# Base (normal) and fallback chunk sizes
CHUNK_SIZE_NORMAL    = int(os.getenv("CHUNK_SIZE_NORMAL",  str(1024 * 1024)))   # 1 MB
CHUNK_SIZE_FALLBACK  = int(os.getenv("CHUNK_SIZE_FALLBACK", str(768 * 1024)))   # 768 KB
DOWNGRADE_TIMEOUTS   = int(os.getenv("DOWNGRADE_TIMEOUTS", "1"))                # timeouts before downgrade

# Adaptive speed thresholds (MB/s)
LOW_SPEED_MBPS_THRESHOLD  = float(os.getenv("LOW_SPEED_MBPS_THRESHOLD", "2.5")) # below this → streak reset
RECOVERY_MBPS_THRESHOLD   = float(os.getenv("RECOVERY_MBPS_THRESHOLD", "5.5"))  # stable above → restore
GOOD_SECONDS_TO_UPSCALE   = int(os.getenv("GOOD_SECONDS_TO_UPSCALE", "6"))      # seconds needed to restore

# Always force download content-type (avoid inline quirks)
FORCE_OCTET_STREAM = os.getenv("FORCE_OCTET_STREAM", "true").lower() == "true"

# If no progress for this many seconds, close connection — browser will retry via Range
STALL_DEADLINE_SEC = int(os.getenv("STALL_DEADLINE_SEC", "15"))

# Write/drain timeouts so we don’t hang forever on slow/closed clients
WRITE_TIMEOUT_SEC = int(os.getenv("WRITE_TIMEOUT_SEC", "15"))
EOF_TIMEOUT_SEC   = int(os.getenv("EOF_TIMEOUT_SEC", "8"))


# Default TTL = 12 hours (configurable via ENV)
HYBRID_LOCK_TTL = int(os.getenv("HYBRID_LOCK_TTL", "14400"))  # 4h default
hybrid_lock_map: Dict[str, float] = {}  # {file_unique_id: expiry_epoch}

def _hybrid_lock_cleanup() -> None:
    """Remove expired locks."""
    now = time.time()
    expired = [k for k, v in hybrid_lock_map.items() if v <= now]
    for k in expired:
        hybrid_lock_map.pop(k, None)

def set_hybrid_lock(uid: str, ttl: int | None = None) -> None:
    """Activate hybrid mode lock for a file UID."""
    if not uid:
        return
    _hybrid_lock_cleanup()
    expiry = time.time() + int(ttl or HYBRID_LOCK_TTL)
    hybrid_lock_map[uid] = expiry
    hours = (int(ttl or HYBRID_LOCK_TTL)) // 3600
    try:
        logger.info(f"🔒 [HybridLock] Set {uid[:10]}... for {hours}h")
    except Exception:
        pass

def clear_hybrid_lock(uid: str) -> bool:
    """Remove hybrid mode lock manually."""
    _hybrid_lock_cleanup()
    existed = uid in hybrid_lock_map
    hybrid_lock_map.pop(uid, None)
    if existed:
        try:
            logger.info(f"🔓 [HybridLock] Cleared {uid[:10]}...")
        except Exception:
            pass
    return existed

def is_hybrid_locked(uid: str) -> bool:
    """Return True if UID currently locked."""
    _hybrid_lock_cleanup()
    exp = hybrid_lock_map.get(uid)
    return bool(exp and exp > time.time())


# =========================
# Helpers
# =========================
def _pick_media(msg):
    """
    Return the first present media object and a nice filename.
    Supports: document, video, audio, voice, photo (original).
    """
    media = (
        getattr(msg, "document", None)
        or getattr(msg, "video", None)
        or getattr(msg, "audio", None)
        or getattr(msg, "voice", None)
        or getattr(msg, "photo", None)
    )
    if media is None:
        return None, None

    name = getattr(media, "file_name", None)
    if not name:
        base = "file"
        ext = ""
        if hasattr(media, "mime_type") and media.mime_type:
            mt = media.mime_type.lower()
            if "mp4" in mt:
                ext = ".mp4"
            elif "webm" in mt:
                ext = ".webm"
            elif "x-matroska" in mt or "mkv" in mt:
                ext = ".mkv"
            elif "mpeg" in mt or "mp3" in mt:
                ext = ".mp3"
            elif "ogg" in mt:
                ext = ".ogg"
            elif "pdf" in mt:
                ext = ".pdf"
        name = f"{base}_{msg.id}{ext}"
    return media, name


def _build_location_and_size(media):
    """
    Build raw input file location for upload.GetFile and return (location, size).
    Works for document/video/audio/voice and photo.
    """
    if hasattr(media, "file_reference") and hasattr(media, "id") and hasattr(media, "access_hash"):
        # Document-like (document/video/audio/voice)
        try:
            loc = InputDocumentFileLocation(
                id=media.id,
                access_hash=media.access_hash,
                file_reference=media.file_reference,
                thumb_size=""
            )
            size = getattr(media, "file_size", None)
            return loc, int(size) if size is not None else None
        except Exception:
            pass

        # Photo
        try:
            loc = InputPhotoFileLocation(
                id=media.id,
                access_hash=media.access_hash,
                file_reference=media.file_reference,
                thumb_size=""
            )
            size = getattr(media, "file_size", None)
            return loc, int(size) if size is not None else None
        except Exception:
            pass

    return None, None


def _parse_http_range(range_header: str, total_size: int) -> Tuple[int, int]:
    """
    Parse HTTP Range header and return (start, end), clamped to file bounds.
    Supports: bytes=start-end, bytes=start-, bytes=-suffixLen
    """
    if not range_header or "bytes=" not in range_header:
        return 0, max(0, total_size - 1)

    try:
        spec = range_header.split("=", 1)[1].strip()
        if "-" not in spec:
            return 0, total_size - 1

        start_s, end_s = spec.split("-", 1)
        if start_s and end_s:
            start = int(start_s); end = int(end_s)
        elif start_s and not end_s:
            start = int(start_s); end = total_size - 1
        elif not start_s and end_s:
            suffix = int(end_s)
            if suffix <= 0:
                return 0, total_size - 1
            start = max(0, total_size - suffix); end = total_size - 1
        else:
            return 0, total_size - 1

        if start < 0: start = 0
        if end >= total_size: end = total_size - 1
        if start > end: return 0, total_size - 1
        return start, end
    except Exception:
        return 0, max(0, total_size - 1)


async def _refresh_message(msg):
    """Re-fetch the same message to refresh file_reference when it expires."""
    try:
        cli = msg._client
        chat_id = msg.chat.id if msg.chat else None
        if cli and chat_id:
            return await cli.get_messages(chat_id, msg.id)
    except Exception as e:
        logger.warning(f"Failed to refresh message: {e}")
    return msg


async def _write_with_timeout(resp: web.StreamResponse, data: bytes) -> None:
    await asyncio.wait_for(resp.write(data), timeout=WRITE_TIMEOUT_SEC)
    await asyncio.wait_for(resp.drain(), timeout=WRITE_TIMEOUT_SEC)


# =========================
# Public API
# =========================
async def stream_and_save(msg, request: web.Request):
    """
    Stream Telegram file to HTTP client with proper resume + full playback.
    - 206 only when Range is present (resume-friendly).
    - Correct Content-Length for the requested window.
    - Single-lane ordered chunks via upload.GetFile (player-friendly).
    - Auto refresh file_reference, auto-handle FileMigrate/DC hops.
    - Stall watchdog to avoid 99% stuck for flaky clients.
    - NEW: Adaptive chunk size (1MB → 512KB on timeouts/slow; restore on stable fast).
    """
    media, file_name = _pick_media(msg)
    if not media:
        raise web.HTTPNotFound(text="No downloadable media in message")

    location, total_size = _build_location_and_size(media)
    if not location or not total_size:
        raise web.HTTPNotFound(text="Unable to resolve file location/size")

    # Parse Range
    range_header = request.headers.get("Range", "")
    start, end = _parse_http_range(range_header, total_size)
    full_range = (start == 0 and end == total_size - 1)
    if full_range:
        range_header = ""  # normalize

    content_length = (end - start + 1)
    status = 206 if range_header else 200

    # Content-Type (forced to download)
    mime_type = (
        "application/octet-stream"
        if FORCE_OCTET_STREAM
        else getattr(media, "mime_type", None) or "application/octet-stream"
    )

    headers = {
        "Content-Type": mime_type,
        "Content-Disposition": f"attachment; filename*=UTF-8''{file_name}",
        "Accept-Ranges": "bytes",
        "Cache-Control": "public, max-age=31536000, immutable",
        "Connection": "keep-alive",
        "Content-Length": str(content_length),
    }
    if status == 206:
        headers["Content-Range"] = f"bytes {start}-{end}/{total_size}"

    # HEAD → only headers
    if request.method == "HEAD":
        return web.Response(status=status, headers=headers)

    response = web.StreamResponse(status=status, headers=headers)
    await response.prepare(request)

    cli = msg._client  # pyrogram Client
    offset = start
    bytes_sent = 0
    last_log_t = time.time()
    last_progress_ts = time.time()
    last_progress_bytes = 0

    # Adaptive state
    cur_chunk_size = CHUNK_SIZE_NORMAL
    timeout_count = 0
    good_speed_streak = 0

    # for quick aborts if the client leaves
    transport = request.transport

    def _ad_log(msg: str):
        logger.info(f"⚙️ [Adaptive] {msg}")

    logger.info(
        f"▶ Safe stream: {file_name} | {start}-{end}/{total_size} | status={status} | "
        f"chunk={cur_chunk_size//1024}KB"
    )

    try:
        # measure speed window
        speed_last_check_t = time.time()
        speed_last_bytes = 0

        while offset <= end:
            # client gone? abort quickly
            if transport is None or transport.is_closing():
                logger.warning("🔌 Client transport closed — aborting safe stream.")
                break

            limit = min(cur_chunk_size, end - offset + 1)
            try:
                # raw upload.GetFile
                res = await cli.invoke(GetFile(location=location, offset=offset, limit=limit))
                chunk: Optional[bytes] = res.bytes
                # success → reset timeout_count
                # (we still might downgrade via speed logic below)
            except FileReferenceExpired:
                logger.warning("⚠️ file_reference expired — refreshing message…")
                msg = await _refresh_message(msg)
                media, _ = _pick_media(msg)
                location, _ = _build_location_and_size(media)
                # retry same offset
                continue
            except FileMigrate as e:
                logger.warning(f"🌐 File migrated to DC{getattr(e, 'new_dc', '?')}; retrying…")
                await asyncio.sleep(0.5)
                continue
            except FloodWait as e:
                logger.warning(f"⏳ Flood wait {e.value}s during GetFile")
                await asyncio.sleep(e.value)
                continue
            except Exception as e:
                # handle generic TIMEOUT → count, and consider downgrade
                if "TIMEOUT" in str(e).upper():
                    timeout_count += 1
                    logger.warning(f"⏱️ GetFile timeout ({timeout_count}/{DOWNGRADE_TIMEOUTS}) at offset {offset}")
                    if timeout_count >= DOWNGRADE_TIMEOUTS and cur_chunk_size != CHUNK_SIZE_FALLBACK:
                        cur_chunk_size = CHUNK_SIZE_FALLBACK
                        _ad_log(f"Chunk size downgraded → {cur_chunk_size//1024} KB (due to repeated TIMEOUTs)")
                        timeout_count = 0
                    await asyncio.sleep(0.35)
                    continue
                # other errors → raise
                raise

            if not chunk:
                # Defensive: no data returned—brief sleep and retry once
                await asyncio.sleep(0.15)
                continue

            # write
            try:
                await _write_with_timeout(response, chunk)
            except asyncio.TimeoutError:
                logger.warning("⏱️ write/drain timeout — retrying once")
                try:
                    await response.write(chunk)
                    await response.drain()
                except (ConnectionResetError, asyncio.CancelledError):
                    logger.warning("⚠️ Client disconnected mid-stream (after timeout).")
                    break

            # progress
            b = len(chunk)
            bytes_sent += b
            offset += b
            now = time.time()

            # stall watchdog
            last_progress_ts = now

            # periodic buffer health drain (every ~4MB sent)
            if bytes_sent - last_progress_bytes >= (4 * 1024 * 1024):
                try:
                    await asyncio.wait_for(response.drain(), timeout=WRITE_TIMEOUT_SEC)
                except asyncio.TimeoutError:
                    logger.warning("⏱️ drain timeout at checkpoint — continuing")
                last_progress_bytes = bytes_sent

            # optional progress log
            if now - last_log_t >= 3:
                sent_mb = bytes_sent / (1024 * 1024)
                logger.info(f"⏩ Sent {sent_mb:.2f} MB of {file_name} (chunk={cur_chunk_size//1024}KB)")
                last_log_t = now

            # ---- Adaptive speed window (per ~1s) ----
            if now - speed_last_check_t >= 1:
                elapsed = max(0.001, now - speed_last_check_t)
                delta = bytes_sent - speed_last_bytes
                speed_MBps = (delta / (1024 * 1024)) / elapsed

                # good/low speed tracking
                if speed_MBps < LOW_SPEED_MBPS_THRESHOLD:
                    good_speed_streak = 0
                else:
                    good_speed_streak += 1

                # if we are on fallback and speed is stable high for long enough → restore to normal
                if (cur_chunk_size == CHUNK_SIZE_FALLBACK
                        and good_speed_streak >= GOOD_SECONDS_TO_UPSCALE
                        and speed_MBps >= RECOVERY_MBPS_THRESHOLD):
                    cur_chunk_size = CHUNK_SIZE_NORMAL
                    _ad_log(
                        f"Chunk size restored → {cur_chunk_size//1024} KB "
                        f"(stable {speed_MBps:.2f} MB/s for {GOOD_SECONDS_TO_UPSCALE}s)"
                    )
                    good_speed_streak = 0

                # reset window
                speed_last_check_t = now
                speed_last_bytes = bytes_sent

            # global stall cutoff (for flaky clients that pause near 99%)
            if STALL_DEADLINE_SEC > 0 and (now - last_progress_ts) > STALL_DEADLINE_SEC and offset <= end:
                logger.warning(
                    f"⏳ No progress for {now - last_progress_ts:.1f}s "
                    f"(sent {bytes_sent}/{content_length}). Closing to trigger client resume."
                )
                break

        # Finalize
        try:
            await asyncio.wait_for(response.write_eof(), timeout=EOF_TIMEOUT_SEC)
        except asyncio.TimeoutError:
            logger.info("EOF write timed out; closing connection.")
        except ConnectionResetError:
            pass

        logger.info(f"✅ Completed (or cleanly aborted) {file_name}: {bytes_sent} bytes sent")
        return response

    except asyncio.CancelledError:
        logger.warning("⚠️ Client cancelled the download.")
        raise
    except ConnectionResetError:
        logger.warning("⚠️ Client disconnected mid-stream.")
        try:
            await response.write_eof()
        except Exception:
            pass
        return response
    except Exception as e:
        logger.error(f"❌ SafeDownload error: {e}")
        raise web.HTTPInternalServerError(text=f"SafeDownload error: {e}") from e


def get_state_snapshot():
    """Lightweight status for debug endpoints."""
    return {
        "mode": "safe_download",
        "chunk_size_normal": CHUNK_SIZE_NORMAL,
        "chunk_size_fallback": CHUNK_SIZE_FALLBACK,
        "forced_octet_stream": FORCE_OCTET_STREAM,
        "stall_deadline_sec": STALL_DEADLINE_SEC,
        "write_timeout_sec": WRITE_TIMEOUT_SEC,
        "eof_timeout_sec": EOF_TIMEOUT_SEC,
        "low_speed_mbps": LOW_SPEED_MBPS_THRESHOLD,
        "recovery_mbps": RECOVERY_MBPS_THRESHOLD,
        "good_seconds_to_upscale": GOOD_SECONDS_TO_UPSCALE,
        "time": int(time.time()),
    }


def recheck_clients_ready(tag: str = "manual"):
    """Log again whether SafeDownload + Pyrogram clients are fully ready."""
    try:
        ok_handler = callable(globals().get("stream_and_save"))
        try:
            from Thunder.bot import multi_clients
            if isinstance(multi_clients, dict):
                mc_count = len(multi_clients)
            elif isinstance(multi_clients, (list, tuple)):
                mc_count = len(multi_clients)
            else:
                mc_count = 0
        except Exception as e:
            mc_count = 0
            logger.warning(f"⚠️ SafeDownload recheck ({tag}): multi_clients import failed: {e}")

        try:
            import pyrogram
            pver = getattr(pyrogram, "__version__", "unknown")
        except Exception:
            pver = "unknown"

        if ok_handler and mc_count > 0:
            logger.info(f"✅ SafeDownload ready ({tag}): handler=OK | clients={mc_count} | pyrogram={pver}")
        else:
            logger.warning(
                f"⚠️ SafeDownload partial init ({tag}): handler={'OK' if ok_handler else 'MISSING'} | "
                f"clients={mc_count} | pyrogram={pver}"
            )
    except Exception as e:
        logger.exception(f"SafeDownload recheck failed ({tag}): {e}")


# --- SafeDownload boot self-check (prints on import) ---
def _integration_boot_log():
    try:
        details = []
        ok_handler = callable(globals().get("stream_and_save"))
        details.append(f"handler={'OK' if ok_handler else 'MISSING'}")
        try:
            from Thunder.bot import multi_clients
            mc_count = 0
            if isinstance(multi_clients, dict):
                mc_count = len(multi_clients)
            elif isinstance(multi_clients, (list, tuple)):
                mc_count = len(multi_clients)
            details.append(f"clients={mc_count}")
            mc_ok = mc_count > 0
        except Exception as e:
            mc_ok = False
            details.append(f"clients=ERR:{e.__class__.__name__}")

        try:
            import pyrogram
            details.append(f"pyrogram={getattr(pyrogram, '__version__', 'unknown')}")
        except Exception:
            details.append("pyrogram=unknown")

        if ok_handler and mc_ok:
            logger.info("✅ SafeDownload ready: " + " | ".join(details))
        else:
            logger.warning("⚠️ SafeDownload partial init: " + " | ".join(details))
    except Exception as e:
        logger.exception(f"SafeDownload boot log failed: {e}")


# --- Auto-run on import (can be disabled via env SAFE_DL_BOOT_LOG=false) ---
import os as _os
import time as _time
import threading as _threading

if _os.getenv("SAFE_DL_BOOT_LOG", "true").lower() == "true":
    try:
        _integration_boot_log()
    except Exception as e:
        logger.warning(f"⚠️ SafeDownload initial log failed: {e}")

    def _delayed_check(tag, delay):
        def _check():
            try:
                recheck_clients_ready(tag)
            except Exception as e:
                logger.warning(f"⚠️ {tag} check failed: {e}")
        t = _threading.Timer(delay, _check)
        t.daemon = True
        t.start()

    # short + long checks
    _delayed_check("fallback-10s", 10)
    _delayed_check("fallback-25s", 25)

    # polling loop (once-only)
    def _poll_for_clients():
        deadline = _time.time() + 25
        while _time.time() < deadline:
            try:
                from Thunder.bot import multi_clients
                if isinstance(multi_clients, (dict, list, tuple)) and len(multi_clients) > 0:
                    recheck_clients_ready("poll-ready")
                    return
            except Exception:
                pass
            _time.sleep(2)
        recheck_clients_ready("poll-timeout")

    _threading.Thread(target=_poll_for_clients, daemon=True).start()
    logger.info("🧩 SafeDownload auto-check: scheduled fallback & polling verification")
