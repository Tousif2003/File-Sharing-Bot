# ==========================================
# 🔹 stream_routes.py — Import Section (Resume Supported)
# ==========================================
import inspect
import secrets
import aiofiles
import os
import re
import json
import asyncio
import time
import traceback
import mimetypes
from aiohttp import web
from aiohttp.client_exceptions import ClientConnectionError
from functools import wraps, lru_cache
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote, unquote
from contextlib import asynccontextmanager
from cachetools import LRUCache  # ✅ Caching for ByteStreamer

# ==========================================
# 🔹 Thunder core imports
# ==========================================
from Thunder import StartTime, __version__
from Thunder.bot import multi_clients, StreamBot, work_loads
from Thunder.server.exceptions import FileNotFound, InvalidHash
from Thunder.utils.custom_dl import ByteStreamer
from Thunder.utils.logger import logger
from Thunder.utils.render_template import render_page
from Thunder.utils.time_format import get_readable_time
from Thunder.vars import Var  # ✅ Global config (DOWNLOAD_DIR, etc.)
routes = web.RouteTableDef()
# ==========================================
# 🔹 Hybrid Downloader Import (safe_download.py)
# ==========================================
try:
    from Thunder.server.safe_download import download_file, stream_and_download
except Exception:
    try:
        from .safe_download import download_file, stream_and_download
    except Exception as e:
        download_file = None
        stream_and_download = None
        logger.warning(f"⚠️ safe_download import failed: {e}")

# ==========================================
# 🔹 Global Download Directory (for resume system)
# ==========================================
SAVE_DIR = Var.DOWNLOAD_DIR  # ✅ Same path as in safe_download.py
# Do NOT create directory again, it's already created in vars.py

# ==========================================
# 🔹 Basic Config & Constants
# ==========================================
SECURE_HASH_LENGTH = 6
CHUNK_SIZE = int(getattr(Var, "CHUNK_SIZE", 1 * 1024 * 1024))
MAX_CLIENTS = int(getattr(Var, "MAX_CLIENTS", 50))
THREADPOOL_MAX_WORKERS = int(getattr(Var, "THREADPOOL_MAX_WORKERS", 12))

PATTERN_HASH_FIRST = re.compile(rf"^([a-zA-Z0-9_-]{{{SECURE_HASH_LENGTH}}})(\d+)(?:/.*)?$")
PATTERN_ID_FIRST = re.compile(r"^(\d+)(?:/.*)?$")
RANGE_REGEX = re.compile(r"bytes=(?P<start>\d*)-(?P<end>\d*)")

# ==========================================
# 🔹 Cache and Executor Setup
# ==========================================
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
            #logger.debug(f"FileNotFound exception: {e}") 
            error_html = """
                <html>
                <head><title>Link Expired</title></head>
                <body style="background-color:#121212; color:#ff4d4d; text-align:center; font-family:Arial, sans-serif; padding-top:100px;">
                    <h1 style="font-size:2em;">🚫 ᴛʜɪꜱ ʟɪɴᴋ ʜᴀꜱ ᴇxᴘɪʀᴇᴅ ᴏʀ ᴛʜᴇ ꜰɪʟᴇ ᴡᴀꜱ ʀᴇᴍᴏᴠᴇᴅ</h1>
                    <h2 style="color:#5fffd4; margin-top:20px;">⏳ ᴘʟᴇᴀꜱᴇ ꜱᴇᴀʀᴄʜ ᴀɢᴀɪɴ ᴀɴᴅ ɢᴇɴᴇʀᴀᴛᴇ ᴀ ɴᴇᴡ ʟɪɴᴋ ꜰʀᴏᴍ ᴛʜᴇ ʙᴏᴛ.</h2>
                </body>
                </html>
            """
            raise web.HTTPNotFound(
                text=error_html,
                content_type="text/html"
            )
            #raise web.HTTPNotFound(
               # text=json_error(404, "File not found"),
                #content_type="application/json"
           # )
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
    return re.sub(r'[\n\r";]', '', filename)

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
                "maximum": Var.CACHE_SIZE,
                "utilization_percent": round((len(class_cache) / Var.CACHE_SIZE) * 100, 2)
            }
        },
        "system": {
            "chunk_size": f"{CHUNK_SIZE / (1024 * 1024):.1f} MB",
            "thread_pool_workers": THREADPOOL_MAX_WORKERS
        }
    }
    
    return web.json_response(
        status_data,
        dumps=lambda obj: json.dumps(obj, indent=2, sort_keys=False)
    )

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
        raise web.HTTPRequestRangeNotSatisfiable(
            headers={"Content-Range": f"bytes */{file_size}"}
        )

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

@routes.get(r"/download/{path:.+}", allow_head=True)
@exception_handler
async def mobile_download(request: web.Request):
    """
    Resumable download endpoint (uses Thunder.server.safe_download.download_file).
    This handler will:
     - Resolve message id + secure hash
     - Choose optimal client
     - Fetch message object
     - Call download_file(message) which will write .part and finalize
     - Serve final file with Range support (resume)
    """
    path = request.match_info["path"]
    message_id, secure_hash = parse_media_request(path, request.query)

    # pick least-loaded client
    client_id, client = optimal_client_selection()
    async with track_workload(client_id):
        # fetch message
        chat_id = getattr(Var, "DEFAULT_CHAT_ID", None)
        message = await client.get_messages(chat_id, message_id)
        if not message:
            raise FileNotFound("Message not found")

        # prefer streaming helper if available (streams while downloading)
        try:
            from Thunder.server.safe_download import stream_and_download as sd_stream
        except Exception:
            sd_stream = None

        if sd_stream:
            try:
                return await sd_stream(message, request, client=client)
            except Exception as e:
                logger.warning("stream_and_download failed/fellthrough: %s", e)

        # fallback: perform background download (resumable) then serve
        try:
            from Thunder.server.safe_download import download_file as sd_download
        except Exception:
            sd_download = None

        if sd_download is None:
            logger.error("safe_download.download_file not available")
            raise web.HTTPInternalServerError(text=json_error(500, "Server download helper missing"))

        # Start background download task (it will resume if .part exists)
        # Note: download_file requires a pyrogram client bound to message (message._client) or client param.
        download_task = asyncio.create_task(sd_download(message, client=client, save_dir=getattr(Var, "DOWNLOAD_DIR", None)))

        # Wait for download_task to finish if the file is small? No — we stream final file after the task finishes.
        # Here we will await the download to complete, then stream file with Range support (simpler and reliable).
        try:
            downloaded_path = await download_task
        except Exception as e:
            logger.error("Background download failed: %s", e)
            raise web.HTTPInternalServerError(text=json_error(500, "Download failed"))

        if not os.path.exists(downloaded_path):
            raise web.HTTPNotFound(text=json_error(404, "File missing"))

        file_size = os.path.getsize(downloaded_path)
        range_header = request.headers.get("Range", "")
        start = 0
        end = file_size - 1
        status = 200

        if range_header:
            try:
                m = re.search(r"bytes=(\d*)-(\d*)", range_header)
                if m:
                    s_str, e_str = m.groups()
                    start = int(s_str) if s_str else 0
                    end = int(e_str) if e_str else file_size - 1
                    end = min(end, file_size - 1)
                    if start > end:
                        raise ValueError("Invalid range")
                    status = 206
            except Exception:
                raise web.HTTPBadRequest(text=json_error(400, "Malformed Range header"), content_type="application/json")

        # validate
        if start < 0 or start >= file_size or end >= file_size or start > end:
            raise web.HTTPRequestRangeNotSatisfiable(headers={"Content-Range": f"bytes */{file_size}"})

        # headers
        filename = os.path.basename(downloaded_path)
        safe_name = sanitize_filename(filename)
        encoded_name = quote(safe_name)
        headers = {
            "Content-Type": "application/octet-stream",
            "Accept-Ranges": "bytes",
            "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_name}",
            "Cache-Control": "no-cache",
            "X-Content-Type-Options": "nosniff",
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Content-Length": str(end - start + 1),
        }

        # stream the file (efficient)
        async def file_stream(path, s, e, chunk=1024 * 1024):
            with open(path, "rb") as rf:
                rf.seek(s)
                remaining = e - s + 1
                while remaining > 0:
                    to_read = min(chunk, remaining)
                    data = rf.read(to_read)
                    if not data:
                        break
                    remaining -= len(data)
                    yield data
                    await asyncio.sleep(0)

        resp = web.StreamResponse(status=status, headers=headers)
        await resp.prepare(request)
        try:
            async for chunk in file_stream(downloaded_path, start, end):
                await resp.write(chunk)
            await resp.write_eof()
        except ConnectionResetError:
            logger.info("Client disconnected during download")
        except Exception as e:
            logger.error("Error while streaming downloaded file: %s", e)
            try:
                await resp.write_eof()
            except Exception:
                pass
        return resp






