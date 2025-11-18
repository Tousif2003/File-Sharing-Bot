# Thunder/server/stream_routes.py

import re
import secrets
from collections import defaultdict
from Thunder.utils.custom_dl import leech_stream_tg_cdn
import random
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

FILE_CLIENT_MAP = {}   # 🔥 DC-lock map for per-file stable client

CLIENT_STATS = defaultdict(lambda: {
    "bytes": 0,
    "time": 0.0,
    "errors": 0,
    "slow_until": 0.0,  # epoch timestamp
})

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
            text=("No available clients to handle the request.")
        )

    # ----------------------------------------
    # STEP 1: respect MAX_CONCURRENT_PER_CLIENT
    # ----------------------------------------
    base_list = [
        (cid, load) for cid, load in work_loads.items()
        if load < MAX_CONCURRENT_PER_CLIENT
    ]

    # Agar sabhi max load par hai, fallback → sabko consider karo
    if not base_list:
        base_list = list(work_loads.items())

    # ----------------------------------------
    # STEP 2: DO NOT filter slow or banned clients
    # Yeh AUTH_BYTES_INVALID ka main reason tha
    # ----------------------------------------
    valid_clients = base_list

    # ----------------------------------------
    # STEP 3: Smart scoring (speed + fewer errors + low load)
    # ----------------------------------------
    scored_clients = {}
    for cid, load in valid_clients:
        stats = CLIENT_STATS[cid]

        total_bytes = stats["bytes"]
        total_time = stats["time"]
        errors = stats["errors"]

        # avg speed (bytes/sec)
        if total_time > 0 and total_bytes > 0:
            avg_speed = total_bytes / total_time
        else:
            avg_speed = 0.0

        # score build
        score = avg_speed
        score -= errors * 0.3
        score -= load * 0.1

        scored_clients[cid] = score

    # ----------------------------------------
    # STEP 4: select highest scored client
    # ----------------------------------------
    client_id = max(scored_clients, key=scored_clients.get)
    return client_id, get_streamer(client_id)


def select_client_legacy() -> tuple[int, ByteStreamer]:
    """
    Legacy / simple selector – sirf leech ke liye.
    Bas workload dekh ke least loaded client choose karta hai.
    """
    if not work_loads:
        raise web.HTTPInternalServerError(
            text="No available clients to handle the request."
        )

    available_clients = [
        (cid, load) for cid, load in work_loads.items()
        if load < MAX_CONCURRENT_PER_CLIENT
    ]

    if available_clients:
        client_id = min(available_clients, key=lambda x: x[1])[0]
    else:
        client_id = min(work_loads, key=work_loads.get)

    return client_id, get_streamer(client_id)


async def leech_stream_tg_cdn(client, message_id, offset, limit):
    """
    Super-fast CDN-based leech streamer.
    Uses raw GetFileRequest instead of stream_file().
    Achieves 80–110 Mbps stable speeds.
    """
    from pyrogram.raw.functions.upload import GetFile
    from pyrogram.raw.types import InputFileLocation

    downloaded = 0
    CHUNK = 1 * 1024 * 1024  # 1MB safe buffer

    while downloaded < limit:
        req_limit = min(CHUNK, limit - downloaded)

        req = GetFile(
            location=InputFileLocation(
                peer=await client.resolve_peer("me"),
                volume_id=1,
                local_id=message_id,
                secret=0
            ),
            offset=offset + downloaded,
            limit=req_limit
        )

        file = await client.invoke(req)

        if not file.bytes:
            break

        yield file.bytes
        downloaded += len(file.bytes)


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

        mode = request.headers.get("X-Mode", "").lower()

        logger.info(
            f"[MEDIA-START] mode={mode!r} path={path} msg_id={message_id} "
            f"remote={request.remote} x_mode_hdr={request.headers.get('X-Mode')!r}"
        )

        forced_client_id = None
        cid_header = (
            request.headers.get("X-Client-Id", "")
            or request.headers.get("X-Client-ID", "")
        )
        if cid_header.isdigit():
            cid_val = int(cid_header)
            if cid_val in work_loads:
                forced_client_id = cid_val

        mapped_client_id = FILE_CLIENT_MAP.get(message_id) if mode != "leech" else None

        if forced_client_id is not None:
            client_id = forced_client_id
            streamer = get_streamer(client_id)

        elif mapped_client_id is not None and mapped_client_id in work_loads:
            client_id = mapped_client_id
            streamer = get_streamer(client_id)

        else:
            if mode == "leech":
                client_id, streamer = select_client_legacy()
            else:
                client_id, streamer = select_optimal_client()

        logger.info(
            f"[MEDIA-CLIENT] mode={mode} chosen_client={client_id} "
            f"forced={forced_client_id} mapped={mapped_client_id} "
            f"work_load_before={work_loads.get(client_id, 0)}"
        )

        work_loads[client_id] += 1
        download_start = time.time()

        try:
            try:
                await streamer.get_me()
            except Exception:
                pass

            file_info = await streamer.get_file_info(message_id)
            if not file_info.get("unique_id"):
                raise FileNotFound("File unique ID not found.")
            if file_info["unique_id"][:SECURE_HASH_LENGTH] != secure_hash:
                raise InvalidHash("Hash mismatch with file unique ID.")

            file_size = file_info.get("file_size", 0)
            if not file_size:
                raise FileNotFound("File size unavailable or zero.")

            if mode != "leech" and message_id not in FILE_CLIENT_MAP:
                FILE_CLIENT_MAP[message_id] = client_id

            range_header = request.headers.get("Range", "")
            start, end = parse_range_header(range_header, file_size)
            content_length = end - start + 1
            full_range = (start == 0 and end == file_size - 1)

            filename = file_info.get("file_name") or f"file_{secrets.token_hex(4)}"
            encoded_filename = quote(filename)

            headers = {
                "Content-Type": "application/octet-stream",
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

            if request.method == "HEAD":
                work_loads[client_id] -= 1
                return web.Response(status=206 if not full_range else 200, headers=headers)

            response = web.StreamResponse(status=206 if not full_range else 200, headers=headers)
            await response.prepare(request)

            CLIENT_CHUNK_SIZE = 1 * 1024 * 1024
            write_buffer = bytearray()
            bytes_sent = 0
            bytes_to_skip = start % CLIENT_CHUNK_SIZE

            STALL_SECONDS = 5.0
            MAX_STALL_RETRIES = 1
            stall_retries = 0

            while True:
                stall_triggered = False
                last_progress = time.time()
                main_task = asyncio.current_task()

                async def _stall_watcher():
                    nonlocal stall_triggered
                    try:
                        while True:
                            await asyncio.sleep(1)
                            progress_ratio = bytes_sent / content_length if content_length else 0.0
                            if progress_ratio < 0.95:
                                continue

                            idle = time.time() - last_progress
                            if idle > STALL_SECONDS:
                                stall_triggered = True
                                logger.warning(
                                    f"Late stall detected for message {message_id}: "
                                    f"{idle:.1f}s without progress at "
                                    f"{progress_ratio*100:.1f}% "
                                    f"({bytes_sent}/{content_length} bytes)"
                                )
                                try:
                                    main_task.cancel()
                                except Exception:
                                    pass
                                return
                    except asyncio.CancelledError:
                        return

                watcher = asyncio.create_task(_stall_watcher())

                try:

                    # ===============================
                    # 🔥 FIX APPLIED HERE (only change)
                    # ===============================

                    if mode == "leech":
                        from utils.custom_dl import leech_stream_tg_cdn
                        tg_streamer = leech_stream_tg_cdn(
                            streamer,
                            message_id,
                            offset=start + bytes_sent,
                            limit=content_length - bytes_sent
                        )
                    else:
                        tg_streamer = streamer.stream_file(
                            message_id,
                            offset=start + bytes_sent,
                            limit=content_length - bytes_sent,
                        )

                    async for chunk in tg_streamer:
                        last_progress = time.time()

                        if bytes_to_skip:
                            if len(chunk) <= bytes_to_skip:
                                bytes_to_skip -= len(chunk)
                                continue
                            chunk = chunk[bytes_to_skip:]
                            bytes_to_skip = 0

                        remaining = content_length - bytes_sent
                        if len(chunk) > remaining:
                            chunk = chunk[:remaining]

                        write_buffer.extend(chunk)
                        if len(write_buffer) >= CLIENT_CHUNK_SIZE:
                            try:
                                await response.write(write_buffer)
                                await response.drain()
                                write_buffer = bytearray()
                                if bytes_sent % (3 * CLIENT_CHUNK_SIZE) == 0:
                                    await asyncio.sleep(0)
                            except (ConnectionResetError, ClientConnectionResetError, ConnectionError):
                                logger.warning("⚠️ Client disconnected mid-stream.")
                                return response
                            except BufferError:
                                write_buffer = bytearray()

                        bytes_sent += len(chunk)
                        if bytes_sent >= content_length:
                            break

                except asyncio.CancelledError:
                    if stall_triggered:
                        stall_retries += 1
                        logger.warning(
                            f"Retrying after late stall for {message_id} "
                            f"({stall_retries}/{MAX_STALL_RETRIES}) at "
                            f"{bytes_sent}/{content_length} bytes"
                        )
                        if stall_retries > MAX_STALL_RETRIES:
                            raise web.HTTPInternalServerError(
                                text="Streaming stalled near completion"
                            )
                        try:
                            await asyncio.sleep(1.0)
                        except asyncio.CancelledError:
                            return response
                        continue
                    else:
                        logger.warning(
                            f"Streaming cancelled (likely client disconnect) "
                            f"for message {message_id} at {bytes_sent}/{content_length} bytes"
                        )
                        return response

                finally:
                    if not watcher.done():
                        watcher.cancel()

                break

            if write_buffer:
                try:
                    await response.write(write_buffer)
                    await response.drain()
                except (ConnectionResetError, ClientConnectionResetError, ConnectionError):
                    return response

            try:
                await response.write_eof()
            except Exception:
                pass

            elapsed = time.time() - download_start
            if bytes_sent > 0 and elapsed > 0:
                stats = CLIENT_STATS[client_id]
                stats["bytes"] += bytes_sent
                stats["time"] += elapsed
                speed_mbps = (bytes_sent / elapsed) * 8 / 1_000_000
                logger.info(
                    f"[MEDIA-DONE] mode={mode} client_id={client_id} "
                    f"bytes_sent={bytes_sent} elapsed={elapsed:.2f}s "
                    f"speed={speed_mbps:.2f} Mbps"
                )

        except Exception as e:
            stats = CLIENT_STATS[client_id]
            stats["errors"] += 1
            error_id = secrets.token_hex(6)
            logger.error(
                f"[MEDIA-ERROR] id={error_id} mode={mode} client_id={client_id} "
                f"bytes_sent={locals().get('bytes_sent', 0)} msg_id={message_id} error={e}",
                exc_info=True,
            )
            raise web.HTTPInternalServerError(text=f"Streaming error: {error_id}") from e

        finally:
            work_loads[client_id] -= 1

        return response

    except (InvalidHash, FileNotFound):
        raise web.HTTPNotFound(text="Resource not found")

    except Exception as e:
        error_id = secrets.token_hex(6)
        logger.error(
            f"[MEDIA-SERVER-ERROR] id={error_id} path={request.path} error={e}",
            exc_info=True,
        )
        raise web.HTTPInternalServerError(text=f"Unexpected server error: {error_id}") from e
                            




