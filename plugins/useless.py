import asyncio
import inspect
import mimetypes
import re
import secrets
import time
import json
import traceback
import math
from concurrent.futures import ThreadPoolExecutor
from aiohttp import web
from aiohttp.client_exceptions import ClientConnectionError
from cachetools import LRUCache
from functools import wraps
from urllib.parse import unquote, quote
from contextlib import asynccontextmanager

from Thunder import StartTime, __version__
from Thunder.bot import multi_clients, StreamBot, work_loads
from Thunder.server.exceptions import FileNotFound, InvalidHash
from Thunder.utils.custom_dl import ByteStreamer
from Thunder.utils.logger import logger
from Thunder.utils.render_template import render_page
from Thunder.utils.time_format import get_readable_time
from Thunder.vars import Var

routes = web.RouteTableDef()

# Configuration constants.
SECURE_HASH_LENGTH = 6
CHUNK_SIZE = 1 * 1024 * 1024
MAX_CLIENTS = 50
THREADPOOL_MAX_WORKERS = 12

# Updated regex: Correctly capture start and end values with named groups.
PATTERN_HASH_FIRST = re.compile(rf"^([a-zA-Z0-9_-]{{{SECURE_HASH_LENGTH}}})(\d+)(?:/.*)?$")
PATTERN_ID_FIRST = re.compile(r"^(\d+)(?:/.*)?$")
RANGE_REGEX = re.compile(r"bytes=(?P<start>\d*)-(?P<end>\d*)")

# Global LRU cache for ByteStreamer instances.
class_cache = LRUCache(maxsize=Var.CACHE_SIZE)
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
    path = request.match_info["path"]
    message_id, secure_hash = parse_media_request(path, request.query)
    return await optimized_mobile_stream(request, message_id, secure_hash)


async def optimized_mobile_stream(request: web.Request, msg_id: int, secure_hash: str):
    """Hybrid mobile download: Old speed + New safety"""
    client_id, client = optimal_client_selection()
    async with track_workload(client_id):
        streamer = await get_cached_streamer(client)
        file_meta = await streamer.get_file_properties(msg_id)

        # ✅ Security check
        if file_meta.unique_id[:SECURE_HASH_LENGTH] != secure_hash:
            raise InvalidHash("Invalid security hash")

        file_size = file_meta.file_size
        range_header = request.headers.get("Range", "")

        # ✅ Lightweight Range parsing (old style)
        if range_header:
            try:
                from_bytes, until_bytes = range_header.replace("bytes=", "").split("-")
                from_bytes = int(from_bytes) if from_bytes else 0
                until_bytes = int(until_bytes) if until_bytes else file_size - 1
            except Exception:
                raise web.HTTPBadRequest(
                    text=json_error(400, "Malformed range header"),
                    content_type="application/json"
                )
        else:
            from_bytes, until_bytes = 0, file_size - 1

        # ✅ Range validation (from new code)
        if until_bytes >= file_size or from_bytes < 0 or until_bytes < from_bytes:
            raise web.HTTPRequestRangeNotSatisfiable(
                headers={"Content-Range": f"bytes */{file_size}"}
            )

        # ✅ Chunk math (old-style fast calc)
        chunk_size = CHUNK_SIZE
        offset = from_bytes - (from_bytes % chunk_size)
        req_length = until_bytes - from_bytes + 1
        total_chunks = math.ceil(req_length / chunk_size)

        # ✅ Direct generator (no async wrapper unless needed)
        if hasattr(streamer, "async_yield_file"):
            stream_generator = streamer.async_yield_file(
                file_meta, client_id, offset, 0, None, total_chunks, chunk_size
            )
        else:
            stream_generator = streamer.yield_file(
                file_meta, client_id, offset, 0, None, total_chunks, chunk_size
            )

        # ✅ Safer filename handling
        filename = sanitize_filename(file_meta.file_name or f"{secrets.token_hex(2)}.bin")
        encoded_filename = quote(filename)

        headers = {
            "Content-Type": "application/octet-stream",
            "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
            "Content-Length": str(req_length),
            "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}",
            "Accept-Ranges": "bytes",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Content-Type-Options": "nosniff",
        }

        resp = web.StreamResponse(status=206 if range_header else 200, headers=headers)
        await resp.prepare(request)

        async for chunk in stream_generator:
            if chunk:
                await resp.write(chunk)
                await asyncio.sleep(0)  # keep event loop responsive

        await resp.write_eof()
        return resp




