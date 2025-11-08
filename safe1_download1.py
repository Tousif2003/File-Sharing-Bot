# Thunder/utils/safe_download.py
# VPS-like downloader: DC-aware, single-lane raw chunking (upload.GetFile),
# proper HTTP Range/HEAD, forced download headers, and gentle stall handling.
# Includes boot-time self-check log to confirm wiring with stream_routes.

import os
import asyncio
import logging
import time
from typing import Tuple, Optional

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
# 1 MB chunks are safe for most mobile networks and players
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE_NORMAL", str(1024 * 1024)))

# Always force download content-type (prevents inline “streaming” quirks)
FORCE_OCTET_STREAM = os.getenv("FORCE_OCTET_STREAM", "true").lower() == "true"

# If no progress for this many seconds, close connection — browser will retry via Range
STALL_DEADLINE_SEC = int(os.getenv("STALL_DEADLINE_SEC", "15"))

# Write/drain timeouts so we don’t hang forever on slow/closed clients
WRITE_TIMEOUT_SEC = int(os.getenv("WRITE_TIMEOUT_SEC", "15"))
EOF_TIMEOUT_SEC   = int(os.getenv("EOF_TIMEOUT_SEC", "8"))


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

    # File name
    name = getattr(media, "file_name", None)
    if not name:
        # voice / photo don't have file_name; fallback
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
    # Document-like (document/video/audio/voice)
    if hasattr(media, "file_reference") and hasattr(media, "id") and hasattr(media, "access_hash"):
        loc = InputDocumentFileLocation(
            id=media.id,
            access_hash=media.access_hash,
            file_reference=media.file_reference,
            thumb_size=""  # original
        )
        size = getattr(media, "file_size", None)
        return loc, int(size) if size is not None else None

    # Photo
    if hasattr(media, "file_reference") and hasattr(media, "id") and hasattr(media, "access_hash"):
        loc = InputPhotoFileLocation(
            id=media.id,
            access_hash=media.access_hash,
            file_reference=media.file_reference,
            thumb_size=""  # original
        )
        size = getattr(media, "file_size", None)
        return loc, int(size) if size is not None else None

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
            start = int(start_s)
            end = int(end_s)
        elif start_s and not end_s:
            # bytes=start-
            start = int(start_s)
            end = total_size - 1
        elif not start_s and end_s:
            # bytes=-N (last N bytes)
            suffix = int(end_s)
            if suffix <= 0:
                return 0, total_size - 1
            start = max(0, total_size - suffix)
            end = total_size - 1
        else:
            return 0, total_size - 1

        if start < 0:
            start = 0
        if end >= total_size:
            end = total_size - 1
        if start > end:
            return 0, total_size - 1
        return start, end

    except Exception:
        return 0, max(0, total_size - 1)


async def _refresh_message(msg):
    """
    Re-fetch the same message to refresh file_reference when it expires.
    """
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
    - Gentle stall watchdog to avoid 99% stuck for flaky clients.
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
        # Normalize to 200 path (some clients send Range: bytes=0- by default)
        range_header = ""

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

    # for quick aborts if the client leaves
    transport = request.transport

    logger.info(f"▶ Safe stream: {file_name} | {start}-{end}/{total_size} | status={status}")

    try:
        while offset <= end:
            # client gone? abort quickly
            if transport is None or transport.is_closing():
                logger.warning("🔌 Client transport closed — aborting safe stream.")
                break

            limit = min(CHUNK_SIZE, end - offset + 1)
            try:
                # raw upload.GetFile
                res = await cli.invoke(GetFile(location=location, offset=offset, limit=limit))
                chunk: Optional[bytes] = res.bytes
            except FileReferenceExpired:
                logger.warning("⚠️ file_reference expired — refreshing message…")
                msg = await _refresh_message(msg)
                media, _ = _pick_media(msg)
                location, _ = _build_location_and_size(media)
                # retry same offset
                continue
            except FileMigrate as e:
                # Let Pyrogram handle reconnection internally; a small pause then retry
                logger.warning(f"🌐 File migrated to DC{getattr(e, 'new_dc', '?')}; retrying…")
                await asyncio.sleep(0.5)
                continue
            except FloodWait as e:
                logger.warning(f"⏳ Flood wait {e.value}s during GetFile")
                await asyncio.sleep(e.value)
                continue

            if not chunk:
                # Defensive: no data returned—brief sleep and retry once
                await asyncio.sleep(0.15)
                continue

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

            bytes_sent += len(chunk)
            offset += len(chunk)
            last_progress_ts = time.time()

            # periodic drain checkpoint for buffer health
            if bytes_sent - last_progress_bytes >= (4 * 1024 * 1024):  # ~4MB intervals
                try:
                    await asyncio.wait_for(response.drain(), timeout=WRITE_TIMEOUT_SEC)
                except asyncio.TimeoutError:
                    logger.warning("⏱️ drain timeout at checkpoint — continuing")
                last_progress_bytes = bytes_sent

            # optional logging
            now = time.time()
            if now - last_log_t >= 3:
                sent_mb = bytes_sent / (1024 * 1024)
                logger.info(f"⏩ Sent {sent_mb:.2f} MB of {file_name}")
                last_log_t = now

            # Stall watchdog: if no progress for too long, close so browser retries via Range
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
        # best-effort close
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
        "chunk_size": CHUNK_SIZE,
        "forced_octet_stream": FORCE_OCTET_STREAM,
        "stall_deadline_sec": STALL_DEADLINE_SEC,
        "write_timeout_sec": WRITE_TIMEOUT_SEC,
        "eof_timeout_sec": EOF_TIMEOUT_SEC,
        "time": int(time.time()),
    }

def recheck_clients_ready(tag: str = "manual"):
    """Log again whether SafeDownload + Pyrogram clients are fully ready."""
    try:
        # 1) handler present?
        ok_handler = callable(globals().get("stream_and_save"))

        # 2) client pool size
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

        # 3) pyrogram version (optional)
        try:
            import pyrogram
            pver = getattr(pyrogram, "__version__", "unknown")
        except Exception:
            pver = "unknown"

        # 4) final log
        if ok_handler and mc_count > 0:
            logger.info(f"✅ SafeDownload ready ({tag}): handler=OK | clients={mc_count} | pyrogram={pver}")
        else:
            logger.warning(f"⚠️ SafeDownload partial init ({tag}): handler={'OK' if ok_handler else 'MISSING'} | "
                           f"clients={mc_count} | pyrogram={pver}")
    except Exception as e:
        logger.exception(f"SafeDownload recheck failed ({tag}): {e}")


# --- SafeDownload boot self-check (prints on import) ---
def _integration_boot_log():
    try:
        details = []

        # 1) stream_and_save callable?
        ok_handler = callable(globals().get("stream_and_save"))
        details.append(f"handler={'OK' if ok_handler else 'MISSING'}")

        # 2) multi_clients status (Pyrogram pool)
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

        # 3) Pyrogram version (just for visibility)
        try:
            import pyrogram
            details.append(f"pyrogram={getattr(pyrogram, '__version__', 'unknown')}")
        except Exception:
            details.append("pyrogram=unknown")

        # 4) Final log line
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

    # helper recheck
    def _delayed_check(tag, delay):
        def _check():
            try:
                recheck_clients_ready(tag)
            except Exception as e:
                logger.warning(f"⚠️ {tag} check failed: {e}")
        t = _threading.Timer(delay, _check)
        t.daemon = True
        t.start()

    # 1️⃣ schedule one short and one long check
    _delayed_check("fallback-10s", 10)
    _delayed_check("fallback-25s", 25)

    # 2️⃣ polling loop (once-only)
    def _poll_for_clients():
        deadline = _time.time() + 25
        while _time.time() < deadline:
            try:
                from Thunder.bot import multi_clients
                if isinstance(multi_clients, (dict, list, tuple)):
                    if len(multi_clients) > 0:
                        recheck_clients_ready("poll-ready")
                        return
            except Exception:
                pass
            _time.sleep(2)
        recheck_clients_ready("poll-timeout")

    _threading.Thread(target=_poll_for_clients, daemon=True).start()
    logger.info("🧩 SafeDownload auto-check: scheduled fallback & polling verification")
