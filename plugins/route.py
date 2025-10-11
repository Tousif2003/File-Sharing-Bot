# Thunder/bot/plugins/stream.py

import time
import asyncio
import random
import uuid
from urllib.parse import quote
from typing import Optional, Tuple, Dict, Union, List, Set
from datetime import datetime, timedelta
from pyrogram import Client, filters, enums
from pyrogram.errors import (
    FloodWait,
    RPCError,
    MediaEmpty,
    FileReferenceExpired,
    FileReferenceInvalid,
    MessageNotModified
)
from pyrogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    CallbackQuery,
    LinkPreviewOptions
)
from pyrogram.enums import ChatMemberStatus
from Thunder.bot import StreamBot
from Thunder.utils.database import db
from Thunder.utils.messages import *
from Thunder.utils.file_properties import get_hash, get_media_file_size, get_name
from Thunder.utils.human_readable import humanbytes
from Thunder.utils.logger import logger
from Thunder.vars import Var
from Thunder.utils.decorators import check_banned, require_token, shorten_link
from Thunder.utils.force_channel import force_channel_check
from Thunder.utils.shortener import shorten
from Thunder.utils.bot_utils import (
    notify_owner,
    handle_user_error,
    log_new_user,
    generate_media_links,
    send_links_to_user,
    check_admin_privileges
)
from motor.motor_asyncio import AsyncIOMotorClient

client = AsyncIOMotorClient(Var.DATABASE_URL)
db_ = client["autodelete"]
collection = db_["auto_delete"]
scheduled_deletes = set()
DELETE_AFTER = 5 * 60  # 5 minutes in seconds


async def save_file(msg):
    data = {
        "_id": msg.id,                  # unique id for easy lookup
        "message_id": msg.id,           # same as msg.id
        "chat_id": msg.chat.id,
        "time": datetime.utcnow().isoformat()
    }
    await collection.insert_one(data)


async def delete_single_file(bot, msg_id, cache_key):
    """Delete a single file by its _id (msg.id)""" 
    await asyncio.sleep(DELETE_AFTER) 
    await CACHE.delete(cache_key)
    # Fetch doc from DB
    doc = await collection.find_one({"_id": msg_id.id})
    if not doc:
        print(f"⚠️ File with id {msg_id.id} not found in DB")
        return

    # Delete message from Telegram
    try:
        await bot.delete_messages(doc["chat_id"], doc["message_id"])
    except Exception as e:
        print(f"[DeleteSingle] Failed to delete message {doc['message_id']}: {e}")

    # Remove from DB
    await collection.delete_one({"_id": msg_id.id})
    print(f"✅ Single file deleted: {msg_id.id}")
    



# Time in seconds after which message should be deleted

async def check_and_delete(bot):
    """Call this once on bot start/restart"""
    now = datetime.utcnow()
    
    async for doc in collection.find({}):
        msg_id = doc["_id"]
        if msg_id in scheduled_deletes:
            continue  # Already scheduled

        msg_time = datetime.fromisoformat(doc["time"])
        elapsed = (now - msg_time).total_seconds()
        
        if elapsed >= DELETE_AFTER:
            # Time over → delete immediately
            try:
                await bot.delete_messages(doc["chat_id"], doc["message_id"])
            except Exception as e:
                print(f"[AutoDelete] Immediate delete failed ({doc['message_id']}): {e}")
            await collection.delete_one({"_id": msg_id})
            print(f"[AutoDelete] Deleted immediately {doc['message_id']}")
        else:
            # Schedule delete after remaining time
            remaining = DELETE_AFTER - elapsed
            scheduled_deletes.add(msg_id)
            asyncio.create_task(schedule_delete(bot, doc, remaining))

async def schedule_delete(bot, doc, delay):
    """Schedules delete after remaining time"""
    await asyncio.sleep(delay)
    try:
        await bot.delete_messages(doc["chat_id"], doc["message_id"])
    except Exception as e:
        print(f"[AutoDelete] Scheduled delete failed ({doc['message_id']}): {e}")
    await collection.delete_one({"_id": doc["_id"]})
    scheduled_deletes.discard(doc["_id"])
    print(f"[AutoDelete] Scheduled delete done {doc['message_id']}")




class LRUCache:
    def __init__(self, max_size=1000, ttl=86400):
        self.cache = {}
        self.max_size = max_size
        self.ttl = ttl
        self.access_order = []
        self._lock = asyncio.Lock()

    async def get(self, key):
        async with self._lock:
            if key not in self.cache:
                return None
            item = self.cache[key]
            if time.time() - item['timestamp'] > self.ttl:
                del self.cache[key]
                self.access_order.remove(key)
                return None
            self.access_order.remove(key)
            self.access_order.append(key)
            return item

    async def set(self, key, value):
        async with self._lock:
            if key in self.cache:
                self.access_order.remove(key)
            elif len(self.cache) >= self.max_size:
                lru_key = self.access_order.pop(0)
                del self.cache[lru_key]
            self.cache[key] = value
            self.access_order.append(key)

    async def delete(self, key):
        async with self._lock:
            if key in self.cache:
                del self.cache[key]
                self.access_order.remove(key)

    async def clean_expired(self):
        count = 0
        current_time = time.time()
        async with self._lock:
            for key in list(self.cache.keys()):
                if current_time - self.cache[key]['timestamp'] > self.ttl:
                    del self.cache[key]
                    self.access_order.remove(key)
                    count += 1
        return count

CACHE = LRUCache(max_size=getattr(Var, "CACHE_SIZE", 1000), ttl=86400)

class RateLimiter:
    def __init__(self, max_calls, time_period):
        self.max_calls = max_calls
        self.time_period = time_period
        self.calls = {}
        self._lock = asyncio.Lock()

    async def is_rate_limited(self, user_id):
        async with self._lock:
            now = time.time()
            if user_id not in self.calls:
                self.calls[user_id] = []
            self.calls[user_id] = [
                ts for ts in self.calls[user_id]
                if now - ts <= self.time_period
            ]
            if len(self.calls[user_id]) >= self.max_calls:
                return True
            self.calls[user_id].append(now)
            return False

    async def get_reset_time(self, user_id):
        async with self._lock:
            if user_id not in self.calls or not self.calls[user_id]:
                return 0
            now = time.time()
            oldest_call = min(self.calls[user_id])
            return max(0, self.time_period - (now - oldest_call))

rate_limiter = RateLimiter(max_calls=20, time_period=60)

async def handle_flood_wait(e):
    wait_time = e.value
    logger.warning(f"FloodWait encountered. Sleeping for {wait_time} seconds.")
    jitter = random.uniform(0, 0.1 * wait_time)
    await asyncio.sleep(wait_time + jitter + 1)

def get_file_unique_id(media_message):
    media_types = [
        'document', 'video', 'audio', 'photo', 'animation',
        'voice', 'video_note', 'sticker'
    ]
    for media_type in media_types:
        media = getattr(media_message, media_type, None)
        if media:
            return media.file_unique_id
    return None

async def forward_media(media_message, cache_key):
    for retry in range(3):
        try:
            result = await media_message.copy(chat_id=Var.BIN_CHANNEL)
            await asyncio.sleep(0.2) 
            await save_file(result)  # Save in DB for auto-delete
            # Schedule delete in background 
            asyncio.create_task(delete_single_file(StreamBot, result, cache_key))
            return result
        except Exception:
            try:
                result = await media_message.forward(chat_id=Var.BIN_CHANNEL)
                await asyncio.sleep(0.2) 
                await save_file(result)  # Save in DB for auto-delete
                # Schedule delete in background 
                asyncio.create_task(delete_single_file(StreamBot, result, cache_key))                 
                return result
            except FloodWait as flood_error:
                if retry < 2:
                    await handle_flood_wait(flood_error)
                else:
                    raise
            except Exception as forward_error:
                if retry == 2:
                    logger.error(f"Error forwarding media: {forward_error}")
                    raise
                await asyncio.sleep(0.5)
    raise Exception("Failed to forward media after multiple attempts")

async def log_request(log_msg, user, stream_link, online_link):
    try:
        if getattr(user, 'title', None):
            source_info = f"{user.title} (Chat/Channel)"
        else:
            first = getattr(user, 'first_name', '') or ''
            last = getattr(user, 'last_name', '') or ''
            display_name = f"{first} {last}".strip() or "Unknown"
            source_info = f"{display_name}"

        id_ = user.id

        buttons = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("📺 Stream", url=stream_link),
                    InlineKeyboardButton("⬇️ Download", url=online_link)
                ]
            ]
        )

        await log_msg.reply_text(
            MSG_NEW_FILE_REQUEST.format(
                source_info=source_info,
                id_=id_,
                online_link=online_link,
                stream_link=stream_link
            ),
            reply_markup=buttons,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            quote=True
        )

        await asyncio.sleep(0.3)
    except Exception as e:
        print(f"Error in log_request: {e}")
        pass

async def process_media_message(client, command_message, media_message, notify=True, shortener=True):
    retries = 0
    max_retries = 3
    cache_key = get_file_unique_id(media_message)
    if cache_key is None:
        if notify:
            await command_message.reply_text(MSG_ERROR_FILE_ID_EXTRACT, quote=True)
        return None
    cached_data = await CACHE.get(cache_key)
    if cached_data:
        if notify:
            await send_links_to_user(
                client,
                command_message,
                cached_data['media_name'],
                cached_data['media_size'],
                cached_data['stream_link'],
                cached_data['online_link']
            )
        return cached_data['online_link']
    while retries < max_retries:
        try:
            log_msg = await forward_media(media_message, cache_key)
            stream_link, online_link, media_name, media_size = await generate_media_links(log_msg, shortener=shortener)
            await CACHE.set(cache_key, {
                'media_name': media_name,
                'media_size': media_size,
                'stream_link': stream_link,
                'online_link': online_link,
                'message_id': log_msg.id,
                'timestamp': time.time()
            })
            if notify:
                await send_links_to_user(
                    client,
                    command_message,
                    media_name,
                    media_size,
                    stream_link,
                    online_link
                )
            await log_request(log_msg, command_message.from_user, stream_link, online_link)
            return online_link
        except FloodWait as e:
            await handle_flood_wait(e)
            retries += 1
            continue
        except (FileReferenceExpired, FileReferenceInvalid):
            retries += 1
            await asyncio.sleep(0.3)
            continue
        except MediaEmpty:
            if notify:
                await command_message.reply_text(MSG_MEDIA_ERROR, quote=True)
            return None
        except Exception as e:
            logger.error(f"Error processing media: {e}")
            if retries < max_retries - 1:
                retries += 1
                await asyncio.sleep(0.3)
                continue
            if notify:
                await handle_user_error(
                    command_message,
                    MSG_ERROR_PROCESSING_MEDIA
                )
            await notify_owner(
                client,
                MSG_CRITICAL_ERROR.format(error=e, error_id=uuid.uuid4().hex[:8])
            )
            return None
    return None

async def retry_failed_media(client, command_message, media_messages, status_msg=None, shortener=True):
    results = []
    for i, msg in enumerate(media_messages):
        try:
            result = await process_media_message(client, command_message, msg, notify=False, shortener=shortener)
            if result:
                results.append(result)
            if status_msg and i % 2 == 0:
                try:
                    await status_msg.edit(f"🔄 **Retrying Failed Files:** {len(results)}/{len(media_messages)} processed")
                except MessageNotModified:
                    pass
        except Exception as e:
            logger.error(f"Error retrying media: {e}")
    return results

async def process_multiple_messages(client, command_message, reply_msg, num_files, status_msg, shortener=True):
    chat_id = command_message.chat.id
    start_message_id = reply_msg.id
    end_message_id = start_message_id + num_files - 1
    message_ids = list(range(start_message_id, end_message_id + 1))
    last_status_text = ""
    try:
        batch_size = 5
        processed_count = 0
        failed_count = 0
        download_links = []
        failed_messages = []

        async def process_single_message(msg):
            try:
                return await process_media_message(
                    client,
                    command_message,
                    msg,
                    notify=False
                )
            except FloodWait as e:
                await handle_flood_wait(e)
                return await process_media_message(client, command_message, msg, notify=False)
            except Exception as e:
                logger.error(f"Message {msg.id} error: {e}")
                return None

        for i in range(0, len(message_ids), batch_size):
            batch_ids = message_ids[i:i+batch_size]
            new_status_text = MSG_PROCESSING_BATCH.format(
                batch_number=(i//batch_size)+1,
                total_batches=(len(message_ids)+batch_size-1)//batch_size,
                file_count=len(batch_ids)
            )
            if new_status_text != last_status_text:
                try:
                    await status_msg.edit(new_status_text)
                    last_status_text = new_status_text
                except MessageNotModified:
                    pass
            await asyncio.sleep(1.0)
            messages = []
            for retry in range(3):
                try:
                    messages = await client.get_messages(
                        chat_id=chat_id,
                        message_ids=batch_ids
                    )
                    break
                except FloodWait as e:
                    await handle_flood_wait(e)
                except Exception as e:
                    if retry == 2:
                        logger.error(f"Failed to get batch {i//batch_size+1}: {e}")
                    await asyncio.sleep(0.5)
            for msg in messages:
                if msg and msg.media:
                    try:
                        result = await process_media_message(
                            client, command_message, msg, notify=False, shortener=shortener
                        )
                        if result:
                            download_links.append(result)
                            processed_count += 1
                        else:
                            failed_count += 1
                            failed_messages.append(msg)
                    except Exception as e:
                        failed_count += 1
                        failed_messages.append(msg)
                        logger.error(f"Failed to process {msg.id}: {e}")
                await asyncio.sleep(1.0)
            if processed_count % 5 == 0 or processed_count + failed_count == len(message_ids):
                new_status_text = MSG_PROCESSING_STATUS.format(
                    processed=processed_count,
                    total=num_files,
                    failed=failed_count
                )
                if new_status_text != last_status_text:
                    try:
                        await status_msg.edit(new_status_text)
                        last_status_text = new_status_text
                    except MessageNotModified:
                        pass

        if failed_messages and len(failed_messages) < num_files / 2:
            new_status_text = MSG_RETRYING_FILES.format(count=len(failed_messages))
            if new_status_text != last_status_text:
                try:
                    await status_msg.edit(new_status_text)
                    last_status_text = new_status_text
                except MessageNotModified:
                    pass
            retry_results = await retry_failed_media(
                client, command_message, failed_messages, status_msg, shortener
            )
            if retry_results:
                download_links.extend(retry_results)
                processed_count += len(retry_results)
                failed_count -= len(retry_results)

        def chunk_list(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        for chunk in chunk_list(download_links, 20):
            links_text = "\n".join(chunk)
            group_message_content = MSG_BATCH_LINKS_READY.format(count=len(chunk)) + f"\n\n`{links_text}`"
            dm_prefix = MSG_DM_BATCH_PREFIX.format(chat_title=command_message.chat.title)
            dm_message_text = f"{dm_prefix}\n{group_message_content}"
            try:
                await command_message.reply_text(
                    group_message_content,
                    quote=True,
                    link_preview_options=LinkPreviewOptions(is_disabled=True),
                    parse_mode=enums.ParseMode.MARKDOWN
                )
            except Exception as e:
                logger.error(f"Error sending batch links to original chat {command_message.chat.id}: {e}")
            if command_message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
                try:
                    await client.send_message(
                        chat_id=command_message.from_user.id,
                        text=dm_message_text,
                        link_preview_options=LinkPreviewOptions(is_disabled=True),
                        parse_mode=enums.ParseMode.MARKDOWN
                    )
                except Exception as e:
                    logger.error(f"Error sending batch links to user DM {command_message.from_user.id}: {e}")
                    await command_message.reply_text(
                        MSG_ERROR_DM_FAILED,
                        quote=True
                    )
            if len(chunk) > 10:
                await asyncio.sleep(0.5)

        new_status_text = MSG_PROCESSING_RESULT.format(
            processed=processed_count,
            total=num_files,
            failed=failed_count
        )
        if new_status_text != last_status_text:
            try:
                await status_msg.edit(new_status_text)
                last_status_text = new_status_text
            except MessageNotModified:
                pass
    except Exception as e:
        logger.error(f"Error in batch processing: {e}")
        new_status_text = MSG_PROCESSING_ERROR.format(
            error=str(e),
            processed=processed_count,
            total=num_files,
            error_id=uuid.uuid4().hex[:8]
        )
        if new_status_text != last_status_text:
            try:
                await status_msg.edit(new_status_text)
                last_status_text = new_status_text
            except MessageNotModified:
                pass

@StreamBot.on_message(filters.command("link") & ~filters.private)
@check_banned
@require_token
@shorten_link
@force_channel_check
async def link_handler(client, message, shortener=True):
    user_id = message.from_user.id
    if not await db.is_user_exist(user_id):
        try:
            invite_link = f"https://t.me/{client.me.username}?start=start"
            await message.reply_text(
                MSG_ERROR_START_BOT.format(invite_link=invite_link),
                link_preview_options=LinkPreviewOptions(is_disabled=True),
                parse_mode=enums.ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(MSG_BUTTON_START_CHAT, url=invite_link)]]),
                quote=True
            )
        except Exception:
            pass
        return 
        
    if not await db.is_user_allowed(user_id):
        await message.reply_text(
            "❌ You do not have access. Please enter the correct password using /pass password."
        )
        return
    
    if await rate_limiter.is_rate_limited(user_id):
        reset_time = await rate_limiter.get_reset_time(user_id)
        await message.reply_text(
            MSG_ERROR_RATE_LIMIT.format(seconds=f"{reset_time:.0f}"),
            quote=True
        )
        return
    if message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
        is_admin = await check_admin_privileges(client, message.chat.id)
        if not is_admin:
            await message.reply_text(
                MSG_ERROR_NOT_ADMIN,
                quote=True
            )
            return
    if not message.reply_to_message:
        await message.reply_text(
            MSG_ERROR_REPLY_FILE,
            quote=True
        )
        return
    reply_msg = message.reply_to_message
    if not reply_msg.media:
        await message.reply_text(
            MSG_ERROR_NO_FILE,
            quote=True
        )
        return
    command_parts = message.text.strip().split()
    num_files = 1
    if len(command_parts) > 1:
        try:
            num_files = int(command_parts[1])
            if num_files < 1 or num_files > 100:
                await message.reply_text(
                    MSG_ERROR_NUMBER_RANGE,
                    quote=True
                )
                return
        except ValueError:
            await message.reply_text(
                MSG_ERROR_INVALID_NUMBER,
                quote=True
            )
            return
    processing_msg = await message.reply_text(
        MSG_PROCESSING_REQUEST,
        quote=True
    )
    try:
        if num_files == 1:
            result = await process_media_message(client, message, reply_msg, shortener=shortener)
            if result:
                if message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
                    try:
                        cached_data = await CACHE.get(get_file_unique_id(reply_msg))
                        if cached_data:
                            msg_text = (
                                MSG_LINKS.format(
                                    file_name=cached_data['media_name'],
                                    file_size=cached_data['media_size'],
                                    download_link=cached_data['online_link'],
                                    stream_link=cached_data['stream_link']
                                )
                            )
                            await client.send_message(
                                chat_id=message.from_user.id,
                                text=MSG_LINK_FROM_GROUP.format(
                                    chat_title=message.chat.title,
                                    links_message=msg_text
                                ),
                                link_preview_options=LinkPreviewOptions(is_disabled=True),
                                parse_mode=enums.ParseMode.MARKDOWN,
                                reply_markup=InlineKeyboardMarkup([
                                    [
                                        InlineKeyboardButton(MSG_BUTTON_STREAM_NOW, url=cached_data['stream_link']),
                                        InlineKeyboardButton(MSG_BUTTON_DOWNLOAD, url=cached_data['online_link'])
                                    ]
                                ]),
                            )
                    except Exception as e:
                        logger.debug(f"Error sending DM to user {message.from_user.id} from group: {e}")
                        await message.reply_text(
                            MSG_ERROR_DM_FAILED,
                            quote=True
                        )
                await processing_msg.delete()
            else:
                try:
                    await processing_msg.edit(MSG_ERROR_PROCESSING_MEDIA)
                except MessageNotModified:
                    pass
        else:
            await process_multiple_messages(client, message, reply_msg, num_files, processing_msg, shortener)
    except Exception as e:
        logger.error(f"Error handling link command: {e}")
        try:
            await processing_msg.edit(MSG_ERROR_PROCESSING_MEDIA)
        except MessageNotModified:
            pass

@StreamBot.on_message(
    filters.private & filters.incoming & (
        filters.document | filters.video | filters.photo | filters.audio |
        filters.voice | filters.animation | filters.video_note
    ),
    group=4
)
@check_banned
@require_token
@shorten_link
@force_channel_check
async def private_receive_handler(client, message, shortener=True):
    if not message.from_user:
        return 
    user_id=message.from_user.id
    if not await db.is_user_allowed(user_id):
        await message.reply_text(
            "❌ You do not have access. Please enter the correct password using /pass password."
        )
        return    
    if await rate_limiter.is_rate_limited(message.from_user.id):
        reset_time = await rate_limiter.get_reset_time(message.from_user.id)
        await message.reply_text(
            MSG_ERROR_RATE_LIMIT.format(seconds=f"{reset_time:.0f}"),
            quote=True
        )
        return
    await log_new_user(
        bot=client,
        user_id=message.from_user.id,
        first_name=message.from_user.first_name
    )
    processing_msg = await message.reply_text(
        MSG_PROCESSING_FILE,
        quote=True
    )
    try:
        result = await process_media_message(client, message, message, shortener=shortener)
        if result:
            await processing_msg.delete()
        else:
            try:
                await processing_msg.edit(MSG_ERROR_PROCESSING_MEDIA)
            except MessageNotModified:
                pass
    except Exception as e:
        logger.error(f"Error in private handler: {e}")
        try:
            await processing_msg.edit(MSG_ERROR_PROCESSING_MEDIA)
        except MessageNotModified:
            pass

@StreamBot.on_message(
    filters.channel & filters.incoming & (
        filters.document | filters.video | filters.photo | filters.audio |
        filters.voice | filters.animation | filters.video_note
    ) & ~filters.chat(Var.BIN_CHANNEL),
    group=-1
)
@shorten_link
async def channel_receive_handler(client, broadcast, shortener=True):
    if hasattr(Var, 'BANNED_CHANNELS') and int(broadcast.chat.id) in Var.BANNED_CHANNELS:
        await client.leave_chat(broadcast.chat.id)
        return
    can_edit = False
    try:
        member = await client.get_chat_member(broadcast.chat.id, client.me.id)
        can_edit = member.status in [
            enums.ChatMemberStatus.ADMINISTRATOR,
            enums.ChatMemberStatus.OWNER
        ]
    except Exception:
        can_edit = False
    retries = 0
    max_retries = 3
    while retries < max_retries:
        try:
            log_msg = await forward_media(broadcast)
            stream_link, online_link, media_name, media_size = await generate_media_links(log_msg, shortener=shortener)
            await log_request(log_msg, broadcast.chat, stream_link, online_link)
            if can_edit:
                try:
                    await client.edit_message_reply_markup(
                        chat_id=broadcast.chat.id,
                        message_id=broadcast.id,
                        reply_markup=InlineKeyboardMarkup([
                            [
                                InlineKeyboardButton(MSG_BUTTON_STREAM_NOW, url=stream_link),
                                InlineKeyboardButton(MSG_BUTTON_DOWNLOAD, url=online_link)
                            ]
                        ])
                    )
                except Exception:
                    pass
            break
        except FloodWait as e:
            await handle_flood_wait(e)
            retries += 1
            continue
        except (FileReferenceExpired, FileReferenceInvalid):
            retries += 1
            await asyncio.sleep(0.5)
            continue
        except Exception as e:
            logger.error(f"Error handling channel message: {e}")
            if retries < max_retries - 1:
                retries += 1
                await asyncio.sleep(0.5)
                continue
            await notify_owner(
                client,
                MSG_CRITICAL_ERROR.format(error=e, error_id=uuid.uuid4().hex[:8])
            )
            break

async def clean_cache_task():
    while True:
        try:
            await asyncio.sleep(3600)
            await CACHE.clean_expired()
        except Exception as e:
            logger.error(f"Error in cache cleaning task: {e}")

StreamBot.loop.create_task(clean_cache_task())

        
