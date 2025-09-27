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

from Thunder import __version__, StartTime
from Thunder.bot import multi_clients, StreamBot, work_loads
from Thunder.server.exceptions import FileNotFound, InvalidHash
from Thunder.utils.custom_dl import ByteStreamer
from Thunder.utils.logger import logger
from Thunder.utils.render_template import render_page
from Thunder.utils.time_format import get_readable_time
from Thunder.vars import Var

routes = web.RouteTableDef()

# ==============================
# Configuration constants
# ==============================

SECURE_HASH_LENGTH = 6
CHUNK_SIZE = 1 * 1024 * 1024   # 1 MB fixed chunks
MAX_CLIENTS = 50
THREADPOOL_MAX_WORKERS = 12
RANGE_REGEX = re.compile(r"bytes=(?P<start>\d*)-(?P<end>\d*)")
PATTERN_HASH_FIRST = re.compile(rf"^([a-zA-Z0-9_-]{{{SECURE_HASH_LENGTH}}})(\d+)(?:/.*)?$")
PATTERN_ID_FIRST = re.compile(r"^(\d+)(?:/.*)?$")

# ==============================
# Global cache
# ==============================

class_cache = LRUCache(maxsize=Var.CACHE_SIZE)
cache_lock = asyncio.Lock()
executor = ThreadPoolExecutor(max_workers=THREADPOOL_MAX_WORKERS)

# ==============================
# Helpers
# ==============================

def json_error(status, message):
    return json.dumps({"error": message})

def exception_handler(func):
    @wraps(func)
    async def wrapper(request):
        try:
            return await func(request)
        except InvalidHash:
            raise web.HTTPForbidden(
                text=json_error(403, "Invalid security credentials"),
                content_type="application/json"
            )
        except FileNotFound:
            raise web.HTTPNotFound(
                text=json_error(404, "File not found"),
                content_type="application/json"
            )
        except (ClientConnectionError, asyncio.CancelledError):
            return web.Response(status=499)
        except web.HTTPException:
            raise
        except Exception as e:
            error_id = secrets.token_hex(6)
            logger.error(f"Unhandled exception {error_id}: {str(e)}")
            logger.error(traceback.format_exc())
            raise web.HTTPInternalServerError(
                text=json_error(500, f"Internal server error (ID: {error_id})"),
                content_type="application/json"
            )
    return wrapper

def parse_media_request(path: str, query: dict) -> tuple[int, str]:
    clean_path = unquote(path).strip('/')
    match = PATTERN_HASH_FIRST.match(clean_path)
    if match:
        message_id = int(match.group(2))
        secure_hash = match.group(1)
        return message_id, secure_hash
    match = PATTERN_ID_FIRST.match(clean_path)
    if match:
        message_id = int(match.group(1))
        secure_hash = query.get("hash", "").strip()
        return message_id, secure_hash
    raise InvalidHash("Invalid URL structure")

def optimal_client_selection():
    if not work_loads:
        raise web.HTTPInternalServerError(text="No available clients")
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

# ==============================
# Routes
# ==============================

@routes.get("/", allow_head=True)
@exception_handler
async def root_redirect(request):
    raise web.HTTPFound("https://telegram.me/FilmyMod123")

@routes.get("/statxx", allow_head=True)
async def status_endpoint(request):
    uptime = time.time() - StartTime
    total_load = sum(work_loads.values())
    return web.json_response({
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
            "workload_distribution": dict(work_loads)
        }
    })

@routes.get(r"/watch/{path:.+}", allow_head=True)
@exception_handler
async def media_preview(request: web.Request):
    path = request.match_info["path"]
    message_id, secure_hash = parse_media_request(path, request.query)
    rendered_page = await render_page(message_id, secure_hash, requested_action='stream')
    return web.Response(text=rendered_page, content_type="text/html")

@routes.get(r"/{path:.+}", allow_head=True)
@exception_handler
async def media_delivery(request: web.Request):
    client_id, client = optimal_client_selection()
    async with track_workload(client_id):
        path = request.match_info["path"]
        message_id, secure_hash = parse_media_request(path, request.query)
        return await handle_media_stream(request, message_id, secure_hash, client_id, client)

# ==============================
# Streaming handler (fixed)
# ==============================

async def handle_media_stream(request, message_id, secure_hash, client_id, client):
    streamer = await get_cached_streamer(client)
    file_meta = await streamer.get_file_properties(message_id)

    if file_meta.unique_id[:SECURE_HASH_LENGTH] != secure_hash:
        raise InvalidHash("Security token mismatch")

    file_size = file_meta.file_size
    range_header = request.headers.get("Range", "")

    if range_header:
        match = RANGE_REGEX.fullmatch(range_header)
        if not match:
            raise web.HTTPBadRequest(text="Malformed Range header")
        start = int(match.group("start") or 0)
        end = int(match.group("end") or file_size - 1)
    else:
        start, end = 0, file_size - 1

    if start < 0 or end >= file_size or start > end:
        raise web.HTTPRequestRangeNotSatisfiable(
            headers={"Content-Range": f"bytes */{file_size}"}
        )

    headers = {
        "Content-Type": "application/octet-stream",
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Content-Length": str(end - start + 1),
        "Content-Disposition": f"attachment; filename*=UTF-8''{quote(file_meta.file_name)}",
        "Accept-Ranges": "bytes"
    }

    stream_generator = streamer.yield_file(
        file_meta, client_id, start, 0, None,
        math.ceil((end - start + 1) / CHUNK_SIZE), CHUNK_SIZE
    )

    resp = web.StreamResponse(status=206 if range_header else 200, headers=headers)
    await resp.prepare(request)

    async for chunk in stream_generator:
        if chunk:
            await resp.write(chunk)
            await asyncio.sleep(0)

    await resp.write_eof()
    return resp

# ==============================
# Mobile optimized download (fixed)
# ==============================

@routes.get(r"/download/{path:.+}", allow_head=True)
@exception_handler
async def mobile_download(request: web.Request):
    path = request.match_info["path"]
    message_id, secure_hash = parse_media_request(path, request.query)
    return await mobile_media_streamer(request, message_id, secure_hash)

async def mobile_media_streamer(request: web.Request, msg_id: int, secure_hash: str):
    range_header = request.headers.get("Range", "")
    client_id, client = optimal_client_selection()
    streamer = await get_cached_streamer(client)
    file_id = await streamer.get_file_properties(msg_id)

    if file_id.unique_id[:SECURE_HASH_LENGTH] != secure_hash:
        raise InvalidHash("Invalid security hash")

    file_size = file_id.file_size
    if range_header:
        from_bytes, until_bytes = range_header.replace("bytes=", "").split("-")
        from_bytes = int(from_bytes)
        until_bytes = int(until_bytes) if until_bytes else file_size - 1
    else:
        from_bytes, until_bytes = 0, file_size - 1

    if until_bytes > file_size or from_bytes < 0 or until_bytes < from_bytes:
        return web.Response(
            status=416,
            body="416: Range not satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    chunk_size = 1024 * 1024
    offset = from_bytes - (from_bytes % chunk_size)
    req_length = until_bytes - from_bytes + 1

    stream_generator = streamer.yield_file(
        file_id, client_id, offset, 0, None,
        math.ceil(req_length / chunk_size), chunk_size
    )

    filename = file_id.file_name or f"{secrets.token_hex(2)}.bin"

    headers = {
        "Content-Type": "application/octet-stream",
        "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
        "Content-Length": str(req_length),
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Accept-Ranges": "bytes",
    }

    resp = web.StreamResponse(status=206 if range_header else 200, headers=headers)
    await resp.prepare(request)

    async for chunk in stream_generator:
        if chunk:
            await resp.write(chunk)
            await asyncio.sleep(0)

    await resp.write_eof()
    return resp
