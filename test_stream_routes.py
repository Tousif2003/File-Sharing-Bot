# stream_routes.py
import os
import re
import json
import asyncio
import time
import math
import traceback
import inspect
import mimetypes
from aiohttp import web
from functools import wraps
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote, unquote
from contextlib import asynccontextmanager

from Thunder import StartTime, __version__
from Thunder.bot import multi_clients, StreamBot, work_loads
from Thunder.server.exceptions import FileNotFound, InvalidHash
from Thunder.utils.custom_dl import ByteStreamer
from Thunder.utils.logger import logger
from Thunder.utils.render_template import render_page
from Thunder.utils.time_format import get_readable_time
from Thunder.vars import Var

# Import hybrid downloader (safe_download.py)
try:
    from Thunder.server.safe_download import download_file, stream_and_download
except Exception:
    try:
        from .safe_download import download_file, stream_and_download
    except Exception as e:
        download_file = None
        stream_and_download = None
        logger.warning(f"safe_download import failed: {e}")
from Thunder.server.helpers import (
    sanitize_filename,
    json_error,
    parse_media_request,
    optimal_client_selection,
    track_workload,
    get_cached_streamer,
    SECURE_HASH_LENGTH,  # ✅ Add this line
)

routes = web.RouteTableDef()

# Configuration constants.
SECURE_HASH_LENGTH = 6
CHUNK_SIZE = int(getattr(Var, "CHUNK_SIZE", 1 * 1024 * 1024))
MAX_CLIENTS = int(getattr(Var, "MAX_CLIENTS", 50))
THREADPOOL_MAX_WORKERS = int(getattr(Var, "THREADPOOL_MAX_WORKERS", 12))

PATTERN_HASH_FIRST = re.compile(rf"^([a-zA-Z0-9_-]{{{SECURE_HASH_LENGTH}}})(\d+)(?:/.*)?$")
PATTERN_ID_FIRST = re.compile(r"^(\d+)(?:/.*)?$")
RANGE_REGEX = re.compile(r"bytes=(?P<start>\d*)-(?P<end>\d*)")

class_cache = LRUCache(maxsize=getattr(Var, "CACHE_SIZE", 128))
cache_lock = asyncio.Lock()

executor = ThreadPoolExecutor(max_workers=THREADPOOL_MAX_WORKERS)

def json_error(status, message):
    return json.dumps({"error": message})

def exception_handler(func):
    @wraps(func)
    async def wrapper(request):
        try:
            return await func(request)
        except InvalidHash as e:
            logger.debug(f"InvalidHash exception: {e}")
            raise web.HTTPForbidden(
                text=json_error(403, "Invalid security credentials"),
                content_type="application/json"
            )
        except FileNotFound as e:
            error_html = """
                <html>
                <head><title>Link Expired</title></head>
                <body style="background-color:#121212; color:#ff4d4d; text-align:center; font-family:Arial, sans-serif; padding-top:100px;">
                    <h1 style="font-size:2em;">🚫 ᴛʜɪꜱ ʟɪɴᴋ ʜᴀꜱ ᴇxᴘɪʀᴇᴅ ᴏʀ ᴛʜᴇ ꜰɪʟᴇ ᴡᴀꜱ ʀᴇᴍᴏᴠᴇᴅ</h1>
                    <h2 style="color:#5fffd4; margin-top:20px;">⏳ ᴘʟᴇᴀꜱᴇ ꜱᴇᴀʀᴄʜ ᴀɢᴀɪɴ ᴀɴᴅ ɢᴇɴᴇʀᴀᴛᴇ ᴀ ɴᴇᴡ ʟɪɴᴋ ꜰʀᴏᴍ ᴛʜᴇ ʙᴏᴛ.</h2>
                </body>
                </html>
            """
            raise web.HTTPNotFound(text=error_html, content_type="text/html")
        except (ClientConnectionError, asyncio.CancelledError):
            return web.Response(status=499)
        except web.HTTPException:
            raise
        except Exception as e:
            error_id = secrets.token_hex(6)
            logger.error(f"Unhandled exception (ID: {error_id}): {str(e)}")
            logger.error(f"Stack trace for error {error_id}:\n{traceback.format_exc()}")
            raise web.HTTPInternalServerError(
                text=json_error(500, f"Internal server error (Reference ID: {error_id})"),
                content_type="application/json"
            )
    return wrapper

def parse_media_request(path: str, query: dict) -> tuple[int, str]:
    clean_path = unquote(path).strip('/')
    match = PATTERN_HASH_FIRST.match(clean_path)
    if match:
        try:
            message_id = int(match.group(2))
            secure_hash = match.group(1)
            if not re.match(r'^[a-zA-Z0-9_-]+$', secure_hash):
                raise InvalidHash("Security token contains invalid characters")
            return message_id, secure_hash
        except ValueError:
            raise InvalidHash("Invalid message ID format")
    match = PATTERN_ID_FIRST.match(clean_path)
    if match:
        try:
            message_id = int(match.group(1))
            secure_hash = query.get("hash", "").strip()
            if len(secure_hash) != SECURE_HASH_LENGTH:
                raise InvalidHash("Security token length mismatch")
            if not re.match(r'^[a-zA-Z0-9_-]+$', secure_hash):
                raise InvalidHash("Security token contains invalid characters")
            return message_id, secure_hash
        except ValueError:
            raise InvalidHash("Invalid message ID format")
    raise InvalidHash("Invalid URL structure")

def optimal_client_selection():
    if not work_loads:
        raise web.HTTPInternalServerError(
            text=json_error(500, "No available clients"),
            content_type="application/json"
        )
    client_id, _ = min(work_loads.items(), key=lambda item: item[1])
    return client_id, multi_clients[client_id]

async def get_cached_streamer(client) -> ByteStreamer:
    streamer = class_cache.get(client)
    if streamer is None:
        async with cache_lock:
            streamer = class_cache.get(client)
            if streamer is None:
                streamer = ByteStreamer(client)
                class_cache[client] = streamer
    return streamer

def sanitize_filename(filename: str) -> str:
    return re.sub(r'[\n\r";]', '', filename or "")

@asynccontextmanager
async def track_workload(client_id):
    work_loads[client_id] += 1
    try:
        yield
    finally:
        work_loads[client_id] -= 1

@routes.get("/", allow_head=True)
@exception_handler
async def root_redirect(request):
    raise web.HTTPFound("https://telegram.me/FilmyMod123")

@routes.get("/statxx", allow_head=True)
async def status_endpoint(request):
    uptime_seconds = time.time() - StartTime
    uptime_formatted = get_readable_time(uptime_seconds)

    sorted_workloads = dict(sorted(
        {f"client_{k}": v for k, v in work_loads.items()}.items(),
        key=lambda item: item[1]
    ))

    total_load = sum(work_loads.values())
    if total_load < MAX_CLIENTS * 0.5:
        status_level = "optimal"
    elif total_load < MAX_CLIENTS * 0.8:
        status_level = "high"
    else:
        status_level = "critical"

    status_data = {
        "server": {
            "status": "operational",
            "status_level": status_level,
            "version": __version__,
            "uptime": uptime_formatted,
            "uptime_seconds": int(uptime_seconds),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
        },
        "telegram_bot": {
            "username": f"@{StreamBot.username}",
            "active_clients": len(multi_clients)
        },
        "resources": {
            "total_workload": total_load,
            "max_clients": MAX_CLIENTS,
            "workload_distribution": sorted_workloads,
            "cache": {
                "current": len(class_cache),
                "maximum": getattr(Var, "CACHE_SIZE", 128),
                "utilization_percent": round((len(class_cache) / getattr(Var, "CACHE_SIZE", 128)) * 100, 2)
            }
        },
        "system": {
            "chunk_size": f"{CHUNK_SIZE / (1024 * 1024):.1f} MB",
            "thread_pool_workers": THREADPOOL_MAX_WORKERS
        }
    }

    return web.json_response(status_data, dumps=lambda obj: json.dumps(obj, indent=2, sort_keys=False))

@routes.get(r"/watch/{path:.+}", allow_head=True)
@exception_handler
async def media_preview(request: web.Request):
    path = request.match_info["path"]
    message_id, secure_hash = parse_media_request(path, request.query)
    rendered_page = await render_page(message_id, secure_hash, requested_action='stream')
    return web.Response(
        text=rendered_page,
        content_type='text/html',
        headers={
            "Cache-Control": "no-cache, must-revalidate",
            "Content-Security-Policy": "default-src 'self' https:; script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://cdnjs.cloudflare.com https://cdn.plyr.io; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://cdn.jsdelivr.net https://cdnjs.cloudflare.com https://cdn.plyr.io; img-src 'self' data: https:; font-src 'self' https://fonts.gstatic.com https://cdnjs.cloudflare.com; connect-src 'self' https:; media-src 'self' https:;",
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "SAMEORIGIN",
            "X-XSS-Protection": "1; mode=block",
            "Referrer-Policy": "strict-origin-when-cross-origin"
        }
    )

@routes.get(r"/{path:.+}", allow_head=True)
@exception_handler
async def media_delivery(request: web.Request):
    client_id, client = optimal_client_selection()
    async with track_workload(client_id):
        path = request.match_info["path"]
        message_id, secure_hash = parse_media_request(path, request.query)
        return await handle_media_stream(request, message_id, secure_hash, client_id, client)

async def handle_media_stream(request, message_id, secure_hash, client_id, client):
    """Thunder-style optimized streaming with fixed 1 MB chunks."""
    streamer = await get_cached_streamer(client)
    file_meta = await streamer.get_file_properties(message_id)

    # Security check
    if file_meta.unique_id[:SECURE_HASH_LENGTH] != secure_hash:
        raise InvalidHash("Security token mismatch")

    file_size = file_meta.file_size
    range_header = request.headers.get("Range", "")

    # Parse HTTP Range
    if range_header:
        range_match = RANGE_REGEX.fullmatch(range_header)
        if not range_match:
            raise web.HTTPBadRequest(
                text=json_error(400, "Malformed range header"),
                content_type="application/json"
            )
        start = int(range_match.group("start") or 0)
        end = int(range_match.group("end") or file_size - 1)
    else:
        start, end = 0, file_size - 1

    # Validate range
    if start < 0 or end >= file_size or start > end:
        raise web.HTTPRequestRangeNotSatisfiable(headers={"Content-Range": f"bytes */{file_size}"})

    # Chunk math
    offset = start - (start % CHUNK_SIZE)
    first_chunk_cut = start - offset
    last_chunk_cut = (end % CHUNK_SIZE) + 1 if end < file_size - 1 else None
    total_chunks = ((end - offset) // CHUNK_SIZE) + 1

    # Always force download
    original_filename = unquote(file_meta.file_name) if file_meta.file_name else f"file_{secrets.token_hex(4)}"
    safe_filename = sanitize_filename(original_filename)
    encoded_filename = quote(safe_filename)

    headers = {
        "Content-Type": "application/octet-stream",
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Content-Length": str(end - start + 1),
        "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}",
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
        "X-Content-Type-Options": "nosniff",
    }

    # Pick generator
    if hasattr(streamer, 'async_yield_file'):
        stream_generator = streamer.async_yield_file(
            file_meta, client_id, offset, first_chunk_cut, last_chunk_cut,
            total_chunks, CHUNK_SIZE
        )
    else:
        stream_generator = streamer.yield_file(
            file_meta, client_id, offset, first_chunk_cut, last_chunk_cut,
            total_chunks, CHUNK_SIZE
        )

    # Normalize generator
    if inspect.isgenerator(stream_generator):
        stream_generator = async_gen_wrapper(stream_generator)
    elif not inspect.isasyncgen(stream_generator):
        raise TypeError("Streamer did not return a valid generator")

    # Proper streaming response
    resp = web.StreamResponse(status=206 if range_header else 200, headers=headers)
    await resp.prepare(request)

    async for chunk in stream_generator:
        if not chunk:
            continue
        await resp.write(chunk)
        await asyncio.sleep(0)  # improve stability

    await resp.write_eof()
    return resp

async def async_gen_wrapper(sync_gen):
    """Wrap a sync generator into an async generator using executor."""
    loop = asyncio.get_running_loop()
    try:
        while True:
            try:
                chunk = await loop.run_in_executor(executor, next, sync_gen)
                yield chunk
            except StopIteration:
                break
    finally:
        try:
            sync_gen.close()
        except Exception:
            pass

# ==============================
# 🚀 Optimized Mobile Download Code (Hybrid)
# ==============================
# Only the /download route is implemented to use safe_download.download_file for resume + fast download.
# Rest of the file left intact so your streaming code remains unchanged.

# ==============================
# 🔹 DOWNLOAD ROUTE — FULL RESUME + DC-AWARE + PROGRESSIVE STREAM
# ==============================
@routes.get(r"/download/{path:.+}", allow_head=True)
@exception_handler
async def mobile_download(request: web.Request):
    """
    Download route integrated with safe_download:
      - Resumable (.part) + HTTP Range support
      - DC-aware download via download_file()
      - Progressive streaming while download runs (if stream_and_download available -> preferred)
      - Falls back to background download + streaming from .part
    """
    path = request.match_info["path"]
    message_id, secure_hash = parse_media_request(path, request.query)

    # Choose best client
    client_id, client = optimal_client_selection()
    async with track_workload(client_id):
        # Resolve message object
        chat_id = getattr(Var, "DEFAULT_CHAT_ID", None)
        message = await client.get_messages(chat_id, message_id)
        if not message:
            raise FileNotFound("Message not found")

        # Quick verify via file unique id if metadata accessible
        try:
            streamer = await get_cached_streamer(client)
            file_meta = await streamer.get_file_properties(message_id)
            if file_meta.unique_id[:SECURE_HASH_LENGTH] != secure_hash:
                raise InvalidHash("Invalid security hash")
            file_size = file_meta.file_size
            suggested_name = file_meta.file_name or ""
        except InvalidHash:
            raise
        except Exception:
            # If metadata lookup fails, try to proceed using message attributes
            file_size = getattr(getattr(message, "document", None) or getattr(message, "video", None) or getattr(message, "audio", None) or getattr(message, "photo", None), "file_size", 0)
            suggested_name = getattr(getattr(message, "document", None) or getattr(message, "video", None) or getattr(message, "audio", None) or getattr(message, "photo", None), "file_name", "") or ""

        # Filenames and paths
        filename = sanitize_filename(suggested_name or f"file_{message_id}.bin")
        # Use Var.DOWNLOAD_DIR or fallback to /tmp
        base_dir = getattr(Var, "DOWNLOAD_DIR", None) or getattr(Var, "SAVE_DIR", None) or "/tmp"
        os.makedirs(base_dir, exist_ok=True)
        final_path = os.path.join(base_dir, filename)
        part_path = final_path + ".part"

        # Determine requested Range from client
        range_header = request.headers.get("Range", "")
        req_start = 0
        req_end = None
        if range_header:
            try:
                # support forms: bytes=START- or bytes=START-END
                m = re.search(r"bytes=(\d*)-(\d*)", range_header)
                if m:
                    s_str, e_str = m.groups()
                    req_start = int(s_str) if s_str else 0
                    req_end = int(e_str) if e_str else None
            except Exception:
                raise web.HTTPBadRequest(text=json_error(400, "Malformed Range header"), content_type="application/json")

        # If file already fully present and valid -> serve file with normal Range handling
        if os.path.exists(final_path) and file_size and os.path.getsize(final_path) >= file_size:
            # Serve final file (use same streaming helper below)
            serve_from = final_path
            download_completed = True
        else:
            serve_from = part_path
            download_completed = False

        # If safe downloader exposes stream_and_download, prefer it (it streams while saving)
        can_use_stream_and_download = False
        try:
            from Thunder.server.safe_download import stream_and_download as _sd_stream_and_download
            if callable(_sd_stream_and_download):
                can_use_stream_and_download = True
        except Exception:
            _sd_stream_and_download = None

        # If stream_and_download exists and client requested whole file (or partial), call it directly.
        # It is expected to handle .part, resume and progressive streaming itself.
        if _sd_stream_and_download is not None:
            try:
                # prefer server-side streaming helper which already supports Range/resume
                return await _sd_stream_and_download(message, request)
            except Exception as e:
                logger.warning(f"stream_and_download failed/fellthrough: {e} (falling back to background download)")

        # Otherwise: start background download_file (which will maintain .part and finalize)
        # Ensure download_file import available
        try:
            from Thunder.server.safe_download import download_file as _sd_download_file
        except Exception:
            _sd_download_file = None

        if _sd_download_file is None:
            logger.error("safe_download.download_file not available (import error)")
            raise web.HTTPInternalServerError(text=json_error(500, "Server missing download helper"))

        # Start background download (if final not already present)
        download_task = None
        if not download_completed:
            # launch background downloader (it will resume if .part present)
            download_task = asyncio.create_task(_sd_download_file(message))

        # Now determine final byte-range to send to the client
        # We'll stream from part_path (which grows) until requested bytes fulfilled or file completes.
        # If req_end is None -> send until EOF (or until file_size) — if file_size known, we use that.
        total_size = file_size if file_size else None
        # If both unknown, we'll stream until download finishes.
        # Compute content-range values for headers:
        start = req_start or 0
        # if client requested a specific end, use it; else aim to total_size-1 if known; else unknown (stream until done)
        end = None
        if req_end is not None:
            end = req_end
        elif total_size is not None:
            end = total_size - 1

        # Validate requested start/end when total_size known
        if total_size is not None:
            if start < 0 or start >= total_size:
                raise web.HTTPRequestRangeNotSatisfiable(headers={"Content-Range": f"bytes */{total_size}"})
            if end is None:
                end = total_size - 1
            elif end >= total_size:
                end = total_size - 1
            if start > end:
                raise web.HTTPRequestRangeNotSatisfiable(headers={"Content-Range": f"bytes */{total_size}"})

        # Prepare response headers
        # Use Content-Range only when we know total_size or client requested Range
        status = 206 if range_header else 200
        headers = {
            "Content-Type": "application/octet-stream",
            "Accept-Ranges": "bytes",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename)}",
            "X-Content-Type-Options": "nosniff",
        }
        # If end known and total_size known, set Content-Range and Content-Length
        if end is not None and total_size is not None:
            headers["Content-Range"] = f"bytes {start}-{end}/{total_size}"
            headers["Content-Length"] = str(end - start + 1)
        elif total_size is not None:
            # no client Range, but size known
            headers["Content-Length"] = str(total_size - start)

        # Streaming generator which reads the part file as it grows and yields chunks
        async def stream_part_file(path: str, start_byte: int, end_byte: int | None, dl_task: asyncio.Task | None, chunk_size: int = 512 * 1024):
            """
            Reads from `path` starting at start_byte, yields bytes as they arrive.
            - If end_byte is not None, stop after sending that byte.
            - If dl_task is running, waits for more bytes to appear until end_byte reached or dl_task finished.
            - If dl_task finished and final file exists, continue reading remaining bytes and finish.
            """
            sent = 0
            pos = start_byte
            # Wait loop: if file doesn't exist yet, wait a bit for downloader to create .part
            wait_loops = 0
            while not os.path.exists(path):
                # If download_task finished and file never created -> nothing to stream
                if dl_task and dl_task.done():
                    # If background download succeeded and created final file at different location, break
                    break
                await asyncio.sleep(0.1)
                wait_loops += 1
                if wait_loops > 600:  # ~60s of waiting, something's wrong
                    logger.warning(f"Waited too long for part file {path}")
                    break

            # Open in binary read mode and seek to start_byte when available
            try:
                with open(path, "rb") as rf:
                    rf.seek(pos)
                    while True:
                        # compute how many bytes we are allowed to send in this iteration
                        if end_byte is not None:
                            to_send_remaining = (end_byte - pos + 1)
                            if to_send_remaining <= 0:
                                break
                            to_read = min(chunk_size, to_send_remaining)
                        else:
                            to_read = chunk_size

                        chunk = rf.read(to_read)
                        if chunk:
                            pos += len(chunk)
                            yield chunk
                            # if we've reached end_byte, stop
                            if end_byte is not None and pos > end_byte:
                                break
                            # yield control
                            await asyncio.sleep(0)
                            continue

                        # no data read (EOF). If download still running, wait for more data
                        if dl_task and not dl_task.done():
                            # small sleep waiting for new bytes
                            await asyncio.sleep(0.2)
                            continue
                        else:
                            # If download finished (or no download task), check if final file exists (rename may have happened)
                            # If final file exists and larger, continue reading it; else break
                            if os.path.exists(final_path):
                                # switch to final_path if different from path
                                if final_path != path:
                                    # close current handle and reopen final_path
                                    try:
                                        with open(final_path, "rb") as rf2:
                                            rf2.seek(pos)
                                            while True:
                                                if end_byte is not None:
                                                    to_send_remaining = (end_byte - pos + 1)
                                                    if to_send_remaining <= 0:
                                                        return
                                                    to_read2 = min(chunk_size, to_send_remaining)
                                                else:
                                                    to_read2 = chunk_size
                                                data2 = rf2.read(to_read2)
                                                if not data2:
                                                    break
                                                pos += len(data2)
                                                yield data2
                                                await asyncio.sleep(0)
                                            return
                                    except Exception:
                                        return
                                else:
                                    # part file is final file (already renamed)
                                    await asyncio.sleep(0.1)
                                    continue
                            # Download finished or not producing more data -> break
                            break
            except FileNotFoundError:
                # Nothing present to stream
                return
            except Exception as e:
                logger.error(f"Error streaming part file: {e}")
                return

        # Prepare StreamResponse
        resp = web.StreamResponse(status=status, headers=headers)
        await resp.prepare(request)

        # If file already complete and present -> stream it and return
        if download_completed:
            try:
                # serve existing final file with range semantics
                async for chunk in stream_file_range(final_path, start, end):
                    await resp.write(chunk)
                await resp.write_eof()
            except Exception as e:
                logger.error(f"Error streaming final file: {e}")
                try:
                    await resp.write_eof()
                except Exception:
                    pass
            return resp

        # Otherwise stream from .part as it grows while background download task runs
        try:
            # If client requested a subset (end specified) we pass that; else pass None to stream until download completes
            async for chunk in stream_part_file(part_path, start, end, download_task):
                # client might have disconnected
                if request.transport is None or request.transport.is_closing():
                    logger.info("Client connection closed while streaming download")
                    break
                if chunk:
                    await resp.write(chunk)
            # If background download not done yet but client requested until EOF, wait until download finishes then send remaining
            if download_task is not None:
                # Wait a short while for download to finish if we haven't reached end
                if not download_task.done():
                    # Let the download continue but yield some time for it to finish producing remainder.
                    # We wait up to a short timeout; if it finishes, the loop above will pick the new bytes
                    await asyncio.sleep(0.2)

                # If download finished successfully, and final file exists, stream any remaining requested bytes
                if download_task.done():
                    exc = download_task.exception()
                    if exc:
                        logger.warning(f"Background download failed: {exc}")
                    else:
                        # stream leftover bytes from final_path if needed
                        if os.path.exists(final_path):
                            final_size_now = os.path.getsize(final_path)
                            # compute any remaining bytes after what we already sent
                            already_sent = start + (int(headers.get("Content-Length", "0")) - (end - start + 1) if ("Content-Length" in headers and end is not None) else 0)
                            # safer approach: stream from current file offset until requested end
                            # We'll simply ensure any remaining bytes upto end (if end known) get sent
                            # To keep it simple, re-open and stream any bytes beyond what was already read
                            # NOTE: stream_part_file already attempted to finish reading final file; this is just safety.
                            async for extra in stream_part_file(final_path, start, end, None):
                                if not extra:
                                    break
                                await resp.write(extra)
            try:
                await resp.write_eof()
            except Exception:
                pass
        except ConnectionResetError:
            logger.info("Client disconnected during download stream")
        except Exception as e:
            logger.error(f"Unexpected error in download route streaming: {e}")
            try:
                await resp.write_eof()
            except Exception:
                pass

        return resp


# === Helper: simple range-file streamer used when final file already exists ===
async def stream_file_range(path: str, start_byte: int = 0, end_byte: int | None = None, chunk_size: int = 512 * 1024):
    """
    Async generator that streams a finished file from start_byte to end_byte (inclusive).
    """
    try:
        with open(path, "rb") as rf:
            rf.seek(start_byte)
            remaining = None if end_byte is None else (end_byte - start_byte + 1)
            while True:
                to_read = chunk_size if remaining is None else min(chunk_size, remaining)
                data = rf.read(to_read)
                if not data:
                    break
                yield data
                if remaining is not None:
                    remaining -= len(data)
                    if remaining <= 0:
                        break
                await asyncio.sleep(0)
    except Exception as e:
        logger.error(f"stream_file_range error: {e}")
        return


