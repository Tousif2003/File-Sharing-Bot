# Thunder/server/stream_routes.py

import re
import secrets
import time
from urllib.parse import quote, unquote
import json
import asyncio
from aiohttp import web, ClientConnectionResetError 
from aiohttp.web_exceptions import HTTPInternalServerError, HTTPNotFound

from Thunder import __version__, StartTime
from Thunder.bot import StreamBot, multi_clients, work_loads
from Thunder.server.exceptions import FileNotFound, InvalidHash
from Thunder.utils.custom_dl import ByteStreamer
from Thunder.utils.logger import logger
from Thunder.utils.render_template import render_page
from Thunder.utils.time_format import get_readable_time

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
    try:
        path = request.match_info["path"]
        message_id, secure_hash = parse_media_request(path, request.query)

        # ✅ Choose best Telegram client dynamically
        client_id, streamer = select_optimal_client()
        work_loads[client_id] += 1

        try:
            # 🔥 Warm Telegram connection (avoid cold-start lag)
            try:
                await streamer.get_me()
            except Exception:
                pass

            # 🎯 Fetch file info
            file_info = await streamer.get_file_info(message_id)
            if not file_info.get("unique_id"):
                raise FileNotFound("File unique ID not found.")
            if file_info["unique_id"][:SECURE_HASH_LENGTH] != secure_hash:
                raise InvalidHash("Hash mismatch with file unique ID.")

            file_size = file_info.get("file_size", 0)
            if not file_size:
                raise FileNotFound("File size unavailable or zero.")

            # 🎯 Handle Range header
            range_header = request.headers.get("Range", "")
            start, end = parse_range_header(range_header, file_size)
            content_length = end - start + 1
            full_range = (start == 0 and end == file_size - 1)

            filename = file_info.get("file_name") or f"file_{secrets.token_hex(4)}"
            encoded_filename = quote(filename)

            # ✅ Streaming headers (force download)
            headers = {
                "Content-Type": "application/octet-stream",  # always binary
                "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}",
                "Accept-Ranges": "bytes",
                "Content-Range": f"bytes {start}-{end}/{file_size}",
                "Cache-Control": "public, max-age=31536000",
                "Connection": "keep-alive",
                "X-Content-Type-Options": "nosniff",
                "Referrer-Policy": "strict-origin-when-cross-origin",
                "X-Accel-Buffering": "no",
                "Content-Length": str(content_length),
            }
            if not full_range:
                headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

            # ✅ For HEAD request (metadata only)
            if request.method == "HEAD":
                work_loads[client_id] -= 1
                return web.Response(status=206 if not full_range else 200, headers=headers)

            # ✅ Prepare streaming response
            response = web.StreamResponse(status=206 if not full_range else 200, headers=headers)
            await response.prepare(request)

            # ⚙️ Chunk + buffering setup
            CLIENT_CHUNK_SIZE = 1 * 1024 * 1024  # 1 MB chunks for stability
            write_buffer = bytearray()
            bytes_sent = 0
            bytes_to_skip = start % CLIENT_CHUNK_SIZE

            # 🚀 Stream from Telegram
            async for chunk in streamer.stream_file(message_id, offset=start, limit=content_length):
                if bytes_to_skip:
                    if len(chunk) <= bytes_to_skip:
                        bytes_to_skip -= len(chunk)
                        continue
                    chunk = chunk[bytes_to_skip:]
                    bytes_to_skip = 0

                remaining = content_length - bytes_sent
                if len(chunk) > remaining:
                    chunk = chunk[:remaining]

                # ✅ Buffered writing
                write_buffer.extend(chunk)
                if len(write_buffer) >= CLIENT_CHUNK_SIZE:
                    try:
                        await response.write(write_buffer)
                        await response.drain()
                        write_buffer = bytearray()
                        # Slight yield every ~3MB to keep async smooth
                        if bytes_sent % (3 * CLIENT_CHUNK_SIZE) == 0:
                            await asyncio.sleep(0)
                    except (ConnectionResetError, ClientConnectionResetError, ConnectionError):
                        logger.warning("⚠️ Client disconnected mid-stream.")
                        break
                    except BufferError:
                        logger.warning("⚠️ Buffer conflict detected — recreating buffer.")
                        write_buffer = bytearray()

                bytes_sent += len(chunk)
                if bytes_sent >= content_length:
                    break

            # ✅ Final flush
            if write_buffer:
                try:
                    await response.write(write_buffer)
                    await response.drain()
                except (ConnectionResetError, ClientConnectionResetError, ConnectionError):
                    logger.warning("⚠️ Client disconnected during final flush.")

            await response.write_eof()

        except (FileNotFound, InvalidHash):
            raise

        except Exception as e:
            error_id = secrets.token_hex(6)
            logger.error(f"Stream error {error_id}: {e}", exc_info=True)
            raise web.HTTPInternalServerError(text=f"Streaming error: {error_id}") from e

        finally:
            work_loads[client_id] -= 1

        return response

    except (InvalidHash, FileNotFound):
        raise web.HTTPNotFound(text="Resource not found")

    except Exception as e:
        error_id = secrets.token_hex(6)
        logger.error(f"Server error {error_id}: {e}", exc_info=True)
        raise web.HTTPInternalServerError(text=f"Unexpected server error: {error_id}") from e






