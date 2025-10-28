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


