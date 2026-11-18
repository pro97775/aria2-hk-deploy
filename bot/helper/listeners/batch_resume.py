"""Resume unfinished batch downloads after a restart."""

from asyncio import sleep
from base64 import b64decode
from os import path as ospath
from time import time

from aiofiles import open as aiopen
from aiofiles.os import makedirs

from ... import DOWNLOAD_DIR, LOGGER, intervals
from ...core.telegram_manager import TgClient
from ..ext_utils.db_handler import database
from .aria2_batch import add_aria2_batch

RESUME_DIR = f"{DOWNLOAD_DIR}resume"


async def resume_batches():
    await sleep(5)
    if intervals["stopAll"]:
        return
    try:
        records = await database.batch_task_unfinished()
    except Exception as error:
        LOGGER.error(f"Batch resume lookup failed: {error}")
        return
    if not records:
        return
    LOGGER.info(f"Batch resume: {len(records)} unfinished task(s) found")
    for record in records:
        if intervals["stopAll"]:
            break
        try:
            await _resume_task(record)
        except Exception as error:
            LOGGER.error(
                f"Batch resume failed for {record.get('name') or record['_id']}: {error}"
            )
            await database.batch_task_save(
                record["_id"], {"status": "failed", "updated_at": time()}
            )


async def _restore_source(record):
    kind = record.get("source_kind")
    source = record.get("source")
    if not source:
        raise ValueError("stored source is missing")
    if kind == "uri":
        return str(source)
    if kind == "torrent":
        await makedirs(RESUME_DIR, exist_ok=True)
        name = str(record["_id"]).replace(":", "-").replace("/", "-")
        path = ospath.join(RESUME_DIR, f"{name}.torrent")
        async with aiopen(path, "wb") as file:
            await file.write(b64decode(source))
        return path
    raise ValueError(f"unknown stored source kind: {kind}")


async def _resume_task(record):
    from pyrogram.types import User

    from ...modules.mirror_leech import Mirror

    chat_id = record["chat_id"]
    thread_id = record.get("thread_id") or None
    name = record.get("name") or record["_id"]
    link = await _restore_source(record)
    message = await TgClient.bot.send_message(
        chat_id,
        f"Resuming unfinished batch: {name}",
        message_thread_id=thread_id,
        disable_notification=True,
    )
    message.from_user = User(id=int(record["user_id"]))
    listener = Mirror(TgClient.bot, message, is_leech=bool(record.get("is_leech")))
    listener.chat_thread_id = record.get("chat_thread_id")
    listener.up_dest = record.get("up_dest") or ""
    listener.folder_name = record.get("folder_name") or ""
    listener.file_types = list(record.get("file_types") or [])
    # add_aria2_batch sets the torrent name after adding; the stored one keeps
    # the original "-n" out override so paths stay identical across restarts.
    listener.name = record.get("out_name") or ""
    listener.link = link
    await listener.before_start()
    listener.enable_batch_mode()
    listener.file_types = list(record.get("file_types") or [])
    listener.files_per_batch = int(record.get("files_per_batch") or 0)
    listener.batch_max_size = int(record.get("batch_max_size") or 0)
    listener.split_size = int(record.get("split_size") or 0)
    listener.as_doc = bool(record.get("as_doc"))
    listener.rc_flags = record.get("rc_flags") or ""
    LOGGER.info(
        f"Batch resume: starting {name}, "
        f"{record.get('done_count', 0)} file(s) already uploaded"
    )
    path = f"{DOWNLOAD_DIR}{listener.mid}{listener.folder_name}"
    await add_aria2_batch(listener, path, [], None, None, run_in_background=False)
