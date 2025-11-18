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

    async def stream_file(self, message_id: int, offset: int = 0, limit: int = 0) -> AsyncGenerator[bytes, None]:
        message = await self.get_message(message_id)
        
        chunk_offset = offset // (1024 * 1024)
        chunk_limit = (limit + (1024 * 1024) - 1) // (1024 * 1024) if limit > 0 else 0

        while True:
            try:
                async for chunk in self.client.stream_media(message, offset=chunk_offset, limit=chunk_limit):
                    yield chunk
                break
            except FloodWait as e:
                logger.debug(f"FloodWait: stream_file, sleep {e.value}s")
                await asyncio.sleep(e.value)

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
    Uses raw GetFile requests (1MB chunks) for stable 80–110 Mbps speed.
    """

    from pyrogram.raw.functions.upload import GetFile
    from pyrogram.raw.types import InputDocumentFileLocation

    CHUNK = 1024 * 1024  # 1MB chunk
    downloaded = 0

    # File metadata (needed for DC-auth)
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
