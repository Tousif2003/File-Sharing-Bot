@routes.get(r"/{path:.+}", allow_head=True)
async def media_delivery(request: web.Request):
    try:
        path = request.match_info["path"]
        message_id, secure_hash = parse_media_request(path, request.query)

        # —— tunables via env ——
        SAFE_SWITCH_THRESHOLD_MBPS = float(os.getenv("SAFE_SWITCH_THRESHOLD_MBPS", "5"))
        SAFE_SWITCH_DURATION_SEC   = int(os.getenv("SAFE_SWITCH_DURATION_SEC", "3"))
        ASYNC_YIELD_INTERVAL_MB    = int(os.getenv("ASYNC_YIELD_INTERVAL_MB", "3"))
        USE_ALT_CLIENT             = os.getenv("SAFE_SWITCH_USE_ALT_CLIENT", "true").lower() == "true"

        # Choose least-loaded client
        client_id, streamer = select_optimal_client()
        work_loads[client_id] += 1

        try:
            # Warm connection (best-effort)
            try:
                await streamer.get_me()
            except Exception:
                pass

            # File info + hash check
            file_info = await streamer.get_file_info(message_id)
            if not file_info.get("unique_id"):
                raise FileNotFound("File unique ID not found.")
            if file_info["unique_id"][:SECURE_HASH_LENGTH] != secure_hash:
                raise InvalidHash("Hash mismatch with file unique ID.")

            file_size = file_info.get("file_size", 0)
            if not file_size:
                raise FileNotFound("File size unavailable or zero.")

            # ===== Resume logic (Range parsing) =====
            range_header = request.headers.get("Range", "")
            start, end = parse_range_header(range_header, file_size)
            content_length = end - start + 1

            # If the requested range is actually the whole file, normalize to 200
            full_range = (start == 0 and end == file_size - 1)
            if full_range:
                range_header = ""

            filename  = file_info.get("file_name") or f"file_{secrets.token_hex(4)}"
            mime_type = file_info.get("mime_type") or "application/octet-stream"
            encoded_filename = quote(filename)

            # Build headers + status
            status = 200 if not range_header else 206
            headers = {
                "Content-Type": mime_type,
                "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}",
                "Accept-Ranges": "bytes",
                "Cache-Control": "public, max-age=31536000, immutable",
                "Connection": "keep-alive",
                "X-Content-Type-Options": "nosniff",
                "Referrer-Policy": "strict-origin-when-cross-origin",
                "X-Accel-Buffering": "no",
                "Content-Length": str(content_length),
            }
            if status == 206:
                headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

            # HEAD -> headers only
            if request.method == "HEAD":
                return web.Response(status=status, headers=headers)

            # Prepare response
            response = web.StreamResponse(status=status, headers=headers)
            await response.prepare(request)

            # Streaming setup
            CLIENT_CHUNK_SIZE = 1 * 1024 * 1024  # 1 MB
            write_buffer = bytearray()
            bytes_sent = 0
            bytes_to_skip = start % CLIENT_CHUNK_SIZE
            dc_retries = 0

            # Speed monitor (for hybrid switch)
            last_check = time.time()
            last_bytes = 0
            slow_count = 0

            while True:
                try:
                    # IMPORTANT: start + bytes_sent, limit = remaining in requested window
                    async for chunk in streamer.stream_file(
                        message_id,
                        offset=start + bytes_sent,
                        limit=content_length - bytes_sent
                    ):
                        # Align to our client chunk boundary if needed
                        if bytes_to_skip:
                            if len(chunk) <= bytes_to_skip:
                                bytes_to_skip -= len(chunk)
                                continue
                            chunk = chunk[bytes_to_skip:]
                            bytes_to_skip = 0

                        # Do not exceed requested range
                        remaining = content_length - bytes_sent
                        if len(chunk) > remaining:
                            chunk = chunk[:remaining]

                        write_buffer.extend(chunk)
                        if len(write_buffer) >= CLIENT_CHUNK_SIZE:
                            try:
                                await response.write(write_buffer)
                                await response.drain()
                                write_buffer = bytearray()

                                # Periodic yield for smoother IO
                                if bytes_sent and bytes_sent % (ASYNC_YIELD_INTERVAL_MB * CLIENT_CHUNK_SIZE) == 0:
                                    await asyncio.sleep(0)
                            except (ConnectionResetError, ClientConnectionResetError, ConnectionError):
                                logger.warning("⚠️ Client disconnected mid-stream.")
                                break
                            except BufferError:
                                logger.warning("⚠️ Buffer conflict detected — recreating buffer.")
                                write_buffer = bytearray()

                        bytes_sent += len(chunk)

                        # ---- Speed monitor (MB/s) for hybrid fallback ----
                        now = time.time()
                        if now - last_check >= 1:
                            elapsed = now - last_check
                            downloaded = bytes_sent - last_bytes
                            speed_MBps = (downloaded / (1024 * 1024)) / max(0.001, elapsed)

                            if speed_MBps < SAFE_SWITCH_THRESHOLD_MBPS:
                                slow_count += 1
                            else:
                                slow_count = 0

                            logger.info(f"⚙️ Stream speed: {speed_MBps:.2f} MB/s | SlowCount={slow_count}")

                            if slow_count >= SAFE_SWITCH_DURATION_SEC:
                                logger.warning(
                                    f"⚡ Speed too low ({speed_MBps:.2f} MB/s) — switching to SafeDownload hybrid mode."
                                )
                                try:
                                    chat_id = (
                                        getattr(StreamBot, "chat_id", None)
                                        or request.query.get("chat_id")
                                        or os.getenv("BIN_CHANNEL")
                                    )
                                    if not chat_id:
                                        raise FileNotFound("Chat ID unavailable for fallback.")
                                    chat_id = int(str(chat_id).replace("@", "").strip())

                                    # Prefer alternate client for hybrid if available
                                    hybrid_cli = None
                                    if USE_ALT_CLIENT and len(multi_clients) > 1:
                                        alt_id = (client_id + 1) % len(multi_clients)
                                        hybrid_cli = multi_clients[alt_id]
                                        logger.info(f"🔁 Hybrid using alternate client ID {alt_id}.")
                                    else:
                                        hybrid_cli = multi_clients[client_id]

                                    # ✅ start safely (no crash if already connected)
                                    await ensure_client_started(hybrid_cli)

                                    msg = await hybrid_cli.get_messages(chat_id, int(message_id))
                                    # handoff to safe downloader (fresh stream)
                                    return await stream_and_save(msg, request)
                                except Exception as e:
                                    logger.error(f"Hybrid switch failed: {e}")
                                    break

                            last_check = now
                            last_bytes = bytes_sent

                        if bytes_sent >= content_length:
                            break

                    break  # success; leave retry loop

                except Exception as e:
                    # Retry on DC migration hints
                    if any(dc in str(e) for dc in ["PHONE_MIGRATE", "NETWORK_MIGRATE", "USER_MIGRATE"]):
                        dc_retries += 1
                        if dc_retries > 3:
                            raise
                        logger.warning(f"🌐 DC mismatch detected — reconnecting attempt {dc_retries}")
                        await asyncio.sleep(1.5)
                        continue
                    else:
                        raise

            # Final flush
            if write_buffer:
                try:
                    await response.write(write_buffer)
                    await response.drain()
                except (ConnectionResetError, ClientConnectionResetError, ConnectionError):
                    logger.warning("⚠️ Client disconnected during final flush.")

            try:
                await response.write_eof()
            except (ConnectionResetError, ClientConnectionResetError, ConnectionError):
                logger.info("Client closed connection before EOF; ignoring.")

        except (FileNotFound, InvalidHash):
            raise
        except Exception as e:
            error_id = secrets.token_hex(6)
            logger.error(f"Stream error {error_id}: {e}", exc_info=True)
            raise web.HTTPInternalServerError(text=f"Streaming error: {error_id}") from e
        finally:
            work_loads[client_id] = max(0, work_loads.get(client_id, 1) - 1)

        return response

    except (InvalidHash, FileNotFound):
        raise web.HTTPNotFound(text="Resource not found")
    except Exception as e:
        error_id = secrets.token_hex(6)
        logger.error(f"Server error {error_id}: {e}", exc_info=True)
        raise web.HTTPInternalServerError(text=f"Unexpected server error: {error_id}") from e




























