# Thunder/utils/custom_dl.py

import asyncio
from typing import Any, AsyncGenerator, Dict

from pyrogram import Client
from pyrogram.errors import FloodWait
from pyrogram.types import Message

from Thunder.server.exceptions import FileNotFound
from Thunder.utils.logger import logger
from Thunder.vars import Var


class ByteStreamer:
    __slots__ = ('client', 'chat_id')

    def __init__(self, client: Client) -> None:
        self.client = client
        self.chat_id = int(Var.BIN_CHANNEL)

    async def get_message(self, message_id: int) -> Message:
        while True:
            try:
                message = await self.client.get_messages(self.chat_id, message_id)
                break
            except FloodWait as e:
                logger.debug(f"FloodWait: get_message, sleep {e.value}s")
                await asyncio.sleep(e.value)
            except Exception as e:
                logger.debug(f"Error fetching message {message_id}: {e}", exc_info=True)
                raise FileNotFound(f"Message {message_id} not found") from e

        if not message or not message.media:
            raise FileNotFound(f"Message {message_id} not found")
        return message

    async def stream_file(
        self,
        message_id: int,
        offset: int = 0,
        limit: int = 0
    ) -> AsyncGenerator[bytes, None]:
        """
        RAM-based streaming with proper resume support:
        - HTTP Range ka offset BYTES me aata hai
        - yahan usko 1MB chunk-offset + in-chunk skip me convert karte hain
        - limit (bytes) ka bhi dhyaan rakha jata hai
        """
        message = await self.get_message(message_id)

        CHUNK = 1024 * 1024  # 1 MB chunks, tumhara old style
        byte_offset = max(0, int(offset))

        # kitne bytes bhejne hain? 0 => unlimited
        remaining = int(limit) if (limit and limit > 0) else None

        # byte offset -> chunk index + chunk ke andar ka start position
        chunk_offset = byte_offset // CHUNK
        start_in_chunk = byte_offset % CHUNK

        # stream_media ka "limit" = kitne chunks
        chunk_limit = (
            (remaining + CHUNK - 1) // CHUNK
            if remaining is not None
            else 0  # 0 == unlimited chunks
        )

        while True:
            try:
                async for raw in self.client.stream_media(
                    message,
                    offset=chunk_offset,
                    limit=chunk_limit,
                ):
                    if not raw:
                        continue

                    chunk = raw

                    # agar offset chunk ke beech me hai to pehle ke bytes skip karo
                    if start_in_chunk:
                        if len(chunk) <= start_in_chunk:
                            # abhi bhi skip baaki hai, agla chunk lo
                            start_in_chunk -= len(chunk)
                            continue
                        chunk = chunk[start_in_chunk:]
                        start_in_chunk = 0

                    # yahan se chunk final hai, ab remaining ke hisaab se trim karo
                    if remaining is not None:
                        if remaining <= 0:
                            return
                        if len(chunk) > remaining:
                            # sirf jitna bacha hai utna hi bhejo
                            yield chunk[:remaining]
                            return
                        else:
                            yield chunk
                            remaining -= len(chunk)
                    else:
                        # unlimited mode
                        yield chunk

                # stream_media normally khatam ho gaya
                break

            except FloodWait as e:
                logger.debug(f"FloodWait: stream_file, sleep {e.value}s")
                await asyncio.sleep(e.value)
                # loop continue karega, wahi chunk_offset/start_in_chunk/remaining state se

    def get_file_info_sync(self, message: Message) -> Dict[str, Any]:
        media = message.document or message.video or message.audio or message.photo
        if not media:
            return {"message_id": message.id, "error": "No media"}
        return {
            "message_id": message.id,
            "file_size": getattr(media, 'file_size', 0) or 0,
            "file_name": getattr(media, 'file_name', None),
            "mime_type": getattr(media, 'mime_type', None),
            "unique_id": getattr(media, 'file_unique_id', None),
            "media_type": type(media).__name__.lower()
        }

    async def get_file_info(self, message_id: int) -> Dict[str, Any]:
        try:
            message = await self.get_message(message_id)
            return self.get_file_info_sync(message)
        except Exception as e:
            logger.debug(f"Error getting file info for {message_id}: {e}", exc_info=True)
            return {"message_id": message_id, "error": str(e)}


# -----------------------------------------------------
# LEECH MODE SUPER-FAST CDN STREAM  (1 MB stable chunks)
# -----------------------------------------------------
async def leech_stream_tg_cdn(client, message_id: int, offset: int, limit: int):
    """
    Ultra-fast Telegram CDN leech streaming.
    Uses raw GetFile requests (1MB chunks) for stable high speed.
    RAM-based, no disk usage.
    """
    from pyrogram.raw.functions.upload import GetFile
    from pyrogram.raw.types import InputDocumentFileLocation

    CHUNK = 1024 * 1024  # 1MB chunk
    downloaded = 0

    file = await client.get_messages("me", message_id)
    if not file or not file.document:
        return

    file_ref = file.document.file_reference
    access_hash = file.document.access_hash
    file_id = file.document.id

    while downloaded < limit:
        req_limit = min(CHUNK, limit - downloaded)

        req = GetFile(
            location=InputDocumentFileLocation(
                id=file_id,
                access_hash=access_hash,
                file_reference=file_ref
            ),
            offset=offset + downloaded,
            limit=req_limit
        )

        part = await client.invoke(req)
        if not part.bytes:
            break

        yield part.bytes
        downloaded += len(part.bytes)
