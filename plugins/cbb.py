async def handle_media_stream(request, message_id, secure_hash, client_id, client):
    """Optimized streaming with fixed 1 MB chunks, stable for Heroku/Koyeb."""
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
        # tiny sleep improves stability on Heroku/Koyeb
        await asyncio.sleep(0)

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
