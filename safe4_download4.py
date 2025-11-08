# Thunder/utils/safe_download.py
# VPS-like downloader: DC-aware, parallel raw chunking, stream+save (batched),

import os
import asyncio
import logging
import time
from typing import Tuple

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
        # try to construct a reasonable name
        base = "file"
        ext = ""
        # Quick extension hints
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
    """
    # For document/video/audio/voice → they all are "document-like" in Pyrogram raw
    if hasattr(media, "file_reference") and hasattr(media, "id") and hasattr(media, "access_hash"):
        loc = InputDocumentFileLocation(
            id=media.id,
            access_hash=media.access_hash,
            file_reference=media.file_reference,
            thumb_size=""  # original
        )
        size = getattr(media, "file_size", None)
        if size is None and hasattr(media, "file_size"):
            size = media.file_size
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
    """
    if not range_header or "bytes=" not in range_header:
        return 0, max(0, total_size - 1)

    try:
        spec = range_header.split("=", 1)[1].strip()
        start_s, end_s = spec.split("-", 1)
        start = int(start_s) if start_s else 0
        end = int(end_s) if end_s else total_size - 1
        if start < 0:
            start = 0
        if end >= total_size:
            end = total_size - 1
        if start > end:
            # invalid range → full file
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


# =========================
# Public API
# =========================
async def stream_and_save(msg, request: web.Request):
    """
    Stream Telegram file to HTTP client with proper resume + full playback.
    - Sends 206 only when Range is present.
    - Sends correct Content-Length for the requested window.
    - Streams in-order (single lane) using upload.GetFile.
    - Auto-refreshes expired file_reference.
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
        # Normalize to 200 path (better for some clients that send Range: bytes=0-)
        range_header = ""

    content_length = (end - start + 1)
    status = 206 if range_header else 200

    # Content-Type (forced to download)
    mime_type = "application/octet-stream" if FORCE_OCTET_STREAM else getattr(media, "mime_type", None) or "application/octet-stream"

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
    last_progress_bytes = 0

    logger.info(f"▶ Safe stream: {file_name} | {start}-{end}/{total_size} | status={status}")

    try:
        while offset <= end:
            limit = min(CHUNK_SIZE, end - offset + 1)
            try:
                # raw upload.GetFile
                res = await cli.invoke(GetFile(location=location, offset=offset, limit=limit))
                chunk = res.bytes
            except FileReferenceExpired:
                logger.warning("⚠️ file_reference expired — refreshing message…")
                msg = await _refresh_message(msg)
                media, _ = _pick_media(msg)
                location, _ = _build_location_and_size(media)
                # retry same offset with renewed location
                continue
            except FileMigrate as e:
                logger.warning(f"🌐 File migrated to DC{e.new_dc}; reconnect handled by Pyrogram.")
                # Let Pyrogram handle reconnection internally; just retry
                await asyncio.sleep(0.5)
                continue
            except FloodWait as e:
                logger.warning(f"⏳ Flood wait {e.value}s during GetFile")
                await asyncio.sleep(e.value)
                continue

            if not chunk:
                # Protective break to avoid infinite loop
                break

            await response.write(chunk)
            bytes_sent += len(chunk)
            offset += len(chunk)

            # periodic drain to keep buffers healthy
            if bytes_sent - last_progress_bytes >= (4 * 1024 * 1024):  # ~4MB intervals
                await response.drain()
                last_progress_bytes = bytes_sent

            # optional logging
            now = time.time()
            if now - last_log_t >= 3:
                sent_mb = bytes_sent / (1024 * 1024)
                logger.info(f"⏩ Sent {sent_mb:.2f} MB of {file_name}")
                last_log_t = now

        # Finalize
        try:
            await response.write_eof()
        except ConnectionResetError:
            pass

        logger.info(f"✅ Completed {file_name}: {bytes_sent} bytes sent")
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
    """
    Lightweight status for debug endpoints.
    """
    return {
        "mode": "safe_download",
        "chunk_size": CHUNK_SIZE,
        "forced_octet_stream": FORCE_OCTET_STREAM,
        "time": int(time.time()),
    }
