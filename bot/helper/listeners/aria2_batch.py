"""Batch download support for aria2 BitTorrent downloads."""

from asyncio import sleep
from base64 import b64encode
from hashlib import sha256
from os import path as ospath
from re import search as re_search
from time import time

from aiofiles.os import path as aiopath, remove

from ... import LOGGER, bot_loop, intervals, task_dict, task_dict_lock
from ...core.config_manager import Config
from ...core.torrent_manager import TorrentManager, aria2_name, is_metadata
from ..ext_utils.files_utils import clean_download
from ..ext_utils.task_manager import check_running_tasks
from ..mirror_leech_utils.status_utils.aria2_status import Aria2Status
from ..telegram_helper.message_utils import (
    send_status_message,
    update_status_message,
)
from .batch import BatchError, BatchRunner

METADATA_TIMEOUT = 300
STALL_TIMEOUT = 1800
MAX_RESUME_TORRENT_BYTES = 9 * 1024 * 1024


def _source_key(source):
    if isinstance(source, bytes):
        return "t:" + sha256(source).hexdigest()[:32]
    if match := re_search(r"xt=urn:btih:([0-9a-zA-Z]+)", source):
        return "m:" + match[1].lower()
    return "u:" + sha256(source.encode()).hexdigest()[:32]


async def _wait_for_files(gid):
    started = time()
    while True:
        try:
            download = await TorrentManager.aria2.tellStatus(gid)
        except Exception as error:
            if time() - started >= METADATA_TIMEOUT:
                raise BatchError(
                    "Could not read the torrent file list. Metadata may be unavailable."
                ) from error
            await sleep(1)
            continue
        status = download.get("status", "")
        if status in ("error", "removed"):
            message = download.get("errorMessage", "")
            raise BatchError(
                f"Aria2 download failed: {message}"
                if message
                else "Aria2 download was removed."
            )
        followed = download.get("followedBy", [])
        if followed:
            gid = followed[0]
            continue
        files = download.get("files", [])
        if files and not is_metadata(download):
            return gid, download
        if time() - started >= METADATA_TIMEOUT:
            raise BatchError(
                "Could not read the torrent file list. Metadata may be unavailable."
            )
        await sleep(1)


class Aria2Batch(BatchRunner):
    engine = "Aria2"

    def __init__(self, listener, gid, files, source, save_path, options):
        entries = []
        for file_info in files:
            path = file_info.get("path", "")
            if not path or path.startswith("[METADATA]"):
                continue
            name = (
                ospath.relpath(path, save_path)
                if path.startswith(save_path)
                else ospath.basename(path)
            )
            entries.append(
                {
                    "index": int(file_info["index"]),
                    "name": name,
                    "size": int(file_info.get("length") or 0),
                    "path": path,
                }
            )
        super().__init__(
            listener,
            {"hash": gid, "content_path": save_path},
            entries,
            task_root=save_path,
            task_key=_source_key(source),
        )
        self.gid = gid
        self.source = source
        self.options = dict(options)
        self.started = False

    def _source_payload(self):
        if isinstance(self.source, bytes):
            if len(self.source) > MAX_RESUME_TORRENT_BYTES:
                LOGGER.warning(
                    "Batch resume: torrent file is too large to store, progress "
                    f"for {self.listener.name} will not survive a restart"
                )
                return None
            return "torrent", b64encode(self.source).decode()
        if not self.source:
            return None
        return "uri", str(self.source)

    def _record_extra(self):
        return {"out_name": self.options.get("out", "")}

    def _candidate_paths(self, file_info):
        return [file_info["path"]]

    async def _apply_batch(self, batch):
        indices = ",".join(str(file_info["index"]) for file_info in batch)
        try:
            if not self.started:
                if not isinstance(self.source, bytes):
                    download = await TorrentManager.aria2.tellStatus(self.gid)
                    if download.get("status") == "active":
                        await TorrentManager.aria2.forcePause(self.gid)
                await TorrentManager.aria2.changeOption(
                    self.gid, {"select-file": indices}
                )
                await TorrentManager.aria2.unpause(self.gid)
                self.started = True
            else:
                await self._remove_torrent()
                options = dict(self.options)
                if isinstance(self.source, bytes):
                    options["pause"] = "true"
                    params = [b64encode(self.source).decode(), [], options]
                    gid = await TorrentManager.aria2.jsonrpc("addTorrent", params)
                else:
                    options.pop("pause", None)
                    gid = await TorrentManager.aria2.addUri(
                        uris=[self.source], options=options
                    )
                self.gid = gid
                self.hash = gid
                gid, download = await _wait_for_files(gid)
                self.gid = gid
                self.hash = gid
                if not isinstance(self.source, bytes):
                    download = await TorrentManager.aria2.tellStatus(gid)
                    if download.get("status") == "active":
                        await TorrentManager.aria2.forcePause(gid)
                paths = {
                    int(file_info["index"]): file_info["path"]
                    for file_info in download.get("files", [])
                    if file_info.get("path")
                    and not file_info["path"].startswith("[METADATA]")
                }
                for file_info in self.files:
                    path = paths.get(file_info["index"])
                    if path:
                        file_info["path"] = path
                async with task_dict_lock:
                    if self.listener.mid in task_dict:
                        task_dict[self.listener.mid] = Aria2Status(
                            self.listener, gid
                        )
                await TorrentManager.aria2.changeOption(
                    gid, {"select-file": indices}
                )
                await TorrentManager.aria2.unpause(gid)
        except Exception as error:
            raise BatchError(f"Could not start the next aria2 batch: {error}") from error
        self.batch_offset = len(self.done)
        self.listener.batch_current = self.batch_offset + 1
        await update_status_message(self.listener.message.chat.id)

    async def _wait_batch(self, batch):
        indices = {str(file_info["index"]) for file_info in batch}
        stopped_time = 0
        stall_time = 0
        last_completed = -1
        failures = 0
        while True:
            if self.listener.is_cancelled or intervals["stopAll"]:
                return False
            try:
                download = await TorrentManager.aria2.tellStatus(self.gid)
                failures = 0
            except Exception as error:
                LOGGER.error(f"Aria2 batch status error: {error}")
                failures += 1
                if failures >= 3:
                    return False
                await sleep(3)
                continue
            status = download.get("status", "")
            if status in ("removed", "error"):
                message = download.get("errorMessage", "")
                if message:
                    raise BatchError(f"Aria2 download failed: {message}")
                return False
            if status == "complete":
                return True
            if status in ("paused", "waiting"):
                stopped_time += 3
                if stopped_time >= 60:
                    return False
            else:
                stopped_time = 0
            completed = int(download.get("completedLength") or 0)
            if completed == last_completed:
                stall_time += 3
                if stall_time >= (Config.TORRENT_TIMEOUT or STALL_TIMEOUT):
                    LOGGER.error(
                        f"Aria2 batch stalled for {stall_time}s with no progress: "
                        f"{self.listener.name}"
                    )
                    return False
            else:
                stall_time = 0
                last_completed = completed
            files = {
                str(file_info.get("index")): file_info
                for file_info in download.get("files", [])
            }
            complete = True
            for index in indices:
                file_info = files.get(index)
                if not file_info:
                    complete = False
                    break
                length = int(file_info.get("length") or 0)
                if length and int(file_info.get("completedLength") or 0) < length:
                    complete = False
                    break
            if complete:
                return True
            await sleep(3)

    async def _stop_download_for_upload(self, batch):
        try:
            download = await TorrentManager.aria2.tellStatus(self.gid)
            if download.get("status") in ("active", "paused", "waiting"):
                await TorrentManager.aria2.forcePause(self.gid)
        except Exception as error:
            LOGGER.error(f"Aria2 batch pause before upload: {error}")

    async def _remove_torrent(self):
        try:
            download = await TorrentManager.aria2.tellStatus(self.gid)
        except Exception:
            download = {"gid": self.gid}
        try:
            await TorrentManager.aria2_remove(download)
        except Exception as error:
            LOGGER.error(f"Aria2 batch remove error: {error}")

    async def _restore_status(self):
        async with task_dict_lock:
            if self.listener.mid in task_dict:
                task_dict[self.listener.mid] = Aria2Status(self.listener, self.gid)
        await update_status_message(self.listener.message.chat.id)

    async def _finish(self):
        await super()._finish()
        await clean_download(self.listener.dir)


async def add_aria2_batch(
    listener, dpath, header, ratio, seed_time, run_in_background=True
):
    from aiofiles import open as aiopen

    options = {"dir": dpath}
    if listener.name:
        options["out"] = listener.name
    if header:
        options["header"] = header
    if listener.seed:
        if ratio:
            options["seed-ratio"] = ratio
        if seed_time:
            options["seed-time"] = seed_time
    if Config.TORRENT_TIMEOUT:
        options["bt-stop-timeout"] = f"{Config.TORRENT_TIMEOUT}"

    try:
        if await aiopath.exists(listener.link):
            async with aiopen(listener.link, "rb") as torrent_file:
                source = await torrent_file.read()
            options["pause"] = "true"
        else:
            source = listener.link
            options["pause-metadata"] = "true"
    except Exception as error:
        await listener.on_download_error(f"{error}")
        return

    add_to_queue, event = await check_running_tasks(listener)
    try:
        if isinstance(source, bytes):
            params = [b64encode(source).decode(), [], options]
            gid = await TorrentManager.aria2.jsonrpc("addTorrent", params)
        else:
            gid = await TorrentManager.aria2.addUri(uris=[source], options=options)
    except Exception as error:
        LOGGER.error(f"Aria2c batch download error: {error}")
        await listener.on_download_error(f"{error}")
        return

    download = await TorrentManager.aria2.tellStatus(gid)
    if download.get("errorMessage"):
        error = str(download["errorMessage"]).replace("<", " ").replace(">", " ")
        LOGGER.error(f"Aria2c batch download error: {error}")
        await TorrentManager.aria2_remove(download)
        await listener.on_download_error(error)
        return
    if isinstance(source, bytes) and await aiopath.exists(listener.link):
        await remove(listener.link)

    try:
        gid, download = await _wait_for_files(gid)
    except BatchError as error:
        await TorrentManager.aria2_remove(download)
        await listener.on_download_error(str(error))
        return

    listener.name = aria2_name(download) or listener.name
    listener.is_torrent = True
    files = download.get("files", [])
    batch = Aria2Batch(listener, gid, files, source, dpath, options)
    async with task_dict_lock:
        task_dict[listener.mid] = Aria2Status(listener, gid, queued=add_to_queue)
    await listener.on_download_start()
    if add_to_queue:
        await event.wait()
        if listener.is_cancelled:
            return
        async with task_dict_lock:
            if task := task_dict.get(listener.mid):
                task.queued = False
    if listener.multi <= 1 and not listener.is_rss:
        await send_status_message(listener.message)
    if run_in_background:
        bot_loop.create_task(batch.run())
    else:
        await batch.run()
