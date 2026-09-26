import asyncio
import collections
import hashlib
import logging
import multiprocessing
import re
from datetime import datetime
from pathlib import Path

import aiohttp

from models import Codec, Station, StreamClip
from utils.title_parser import get_title_and_artist

DEFAULT_CURRENT_TITLE = "[No title set]"


class StreamProcesser:
    MAX_FILE_DURATION_SEC = 600

    def __init__(self, url, stream_id, data_folder, stream_id_obj, queue):
        self.url = url
        self.stream_id = stream_id
        self.data_folder = Path(data_folder)
        self._queue = queue
        self._stream_obj = stream_id_obj

        # Audio stream state
        self._audio_buffer = bytearray()
        self._file_count = 0
        self._file_start_time = None
        self._out_file = None
        self._out_file_name = None
        self._current_file = None

        # Deduplication cache (last ~8 seconds of frames)
        self._recent_hashes = collections.deque(maxlen=300)

        # MPEG 1 Layer 3 lookup tables
        self._BITRATE_TABLE = [
            0,
            32,
            40,
            48,
            56,
            64,
            80,
            96,
            112,
            128,
            160,
            192,
            224,
            256,
            320,
            0,
        ]
        self._SAMPLERATE_TABLE = [44100, 48000, 32000, 0]

        # ICY Track State
        self.current_title = DEFAULT_CURRENT_TITLE

    async def _create_outfile(self):
        if self._out_file is not None:
            await self._close_outfile()

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_dir = self.data_folder / f"radio_{self.stream_id}"
        output_dir.mkdir(parents=True, exist_ok=True)

        self._file_count += 1
        filename = f"{timestamp}.mp3"
        self._out_file_name = output_dir / filename

        logging.info(
            "[Stream %s] Creating track file: %s",
            self.stream_id,
            self._out_file_name.name,
        )
        self._out_file = open(self._out_file_name, "wb")
        self._file_start_time = asyncio.get_running_loop().time()
        self._current_file = await StreamClip.create(
            station_id=self._stream_obj,
            file_path=self._out_file_name,
        )

    async def _write_frame(self, frame_bytes: bytes):
        # Check 6-minute timeout without metadata change
        if self._out_file is not None and self._file_start_time is not None:
            elapsed = asyncio.get_running_loop().time() - self._file_start_time
            if elapsed >= self.MAX_FILE_DURATION_SEC:
                logging.info(
                    "[Stream %s] File reached 6 minutes without track change. Rotating...",
                    self.stream_id,
                )
                await self._close_outfile()

        if self._out_file is None:
            await self._create_outfile()

        self._out_file.write(frame_bytes)

    async def _close_outfile(self):
        if self._out_file is not None:
            self._out_file.close()
            logging.info(
                "[Stream %s] Closed file: %s", self.stream_id, self._out_file_name.name
            )

            # loop = asyncio.get_running_loop()
            # try:
            # await loop.run_in_executor(None, self._queue.put, (self.stream_id, self._out_file_name))
            # except Exception as e:
            #     logging.error("[Stream %s] Queue put error: %s", self.stream_id, e)

            self._out_file = None
            self._out_file_name = None

    def _is_frame_processed(self, frame_bytes: bytes) -> bool:
        frame_hash = hashlib.md5(frame_bytes).hexdigest()
        if frame_hash in self._recent_hashes:
            return True
        self._recent_hashes.append(frame_hash)
        return False

    def parse_mp3_frame_size(self, header_bytes: bytes) -> int:
        b1, b2, b3, _ = header_bytes
        if not (b1 == 0xFF and (b2 & 0xE0) == 0xE0):
            return -1

        bitrate_idx = (b3 >> 4) & 0x0F
        sample_rate_idx = (b3 >> 2) & 0x03
        padding_bit = (b3 >> 1) & 0x01

        bitrate = self._BITRATE_TABLE[bitrate_idx] * 1000
        sample_rate = self._SAMPLERATE_TABLE[sample_rate_idx]

        if bitrate == 0 or sample_rate == 0:
            return -1

        return int(144 * bitrate / sample_rate) + padding_bit

    async def _process_audio_buffer(self):
        """Parses and writes all complete MP3 frames sitting in _audio_buffer."""
        while (
            len(self._audio_buffer) >= 4 and self.current_title != DEFAULT_CURRENT_TITLE
        ):
            if self._audio_buffer[0] == 0xFF and (self._audio_buffer[1] & 0xE0) == 0xE0:
                frame_size = self.parse_mp3_frame_size(self._audio_buffer[:4])
                if frame_size == -1:
                    self._audio_buffer.pop(0)
                    continue
                if len(self._audio_buffer) < frame_size:
                    break

                frame_bytes = self._audio_buffer[:frame_size]
                del self._audio_buffer[:frame_size]

                if not self._is_frame_processed(frame_bytes):
                    await self._write_frame(frame_bytes)
            else:
                self._audio_buffer.pop(0)

    async def _handle_metadata_change(self, meta_str: str):
        if not meta_str:
            return None

        new_title = ""
        if "StreamTitle=" in meta_str:
            title_part = meta_str.split("StreamTitle=")[1]
            song_title = title_part.split(";")[0].strip(" '\"")
            if song_title and song_title != self.current_title:
                new_title = song_title
        if new_title == '':
            return 

        logging.info(
            "[Stream %s] ICY Track Change: '%s' -> '%s'",
            self.stream_id,
            self.current_title,
            new_title,
        )

        # Flush pending audio frames to old track before rotating files
        await self._process_audio_buffer()
        await self._close_outfile()
        
        if self.current_title != DEFAULT_CURRENT_TITLE:
            # basically now do the writing and metadata writing.
            data = await get_title_and_artist(self.current_title)
            self._current_file.raw_stream_title = self.current_title
            self._current_file.is_track = data["is_track"]
            self._current_file.parser_metadata_id = data["meta_id"]
            if data["is_track"]:
                self._current_file.artists = data["artist"]
                self._current_file.title = data["title"]
            await self._current_file.save(
                update_fields=[
                    "raw_stream_title",
                    "is_track",
                    "parser_metadata_id",
                    "artists",
                    "title",
                ]
            )

        self.current_title = new_title

    async def record_mp3_stream(self):
        retry_delay = 0
        connector = aiohttp.TCPConnector(ssl=False)
        timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_read=15)

        async with aiohttp.ClientSession(
            connector=connector, timeout=timeout
        ) as session:
            while True:
                try:
                    logging.info(
                        "[Stream %s] Connecting to %s...", self.stream_id, self.url
                    )
                    async with session.get(
                        self.url, headers={"Icy-MetaData": "1"}
                    ) as response:
                        if response.status != 200:
                            raise aiohttp.ClientError(f"HTTP Status {response.status}")

                        metaint = int(response.headers.get("icy-metaint", 0))
                        logging.info(
                            "[Stream %s] Connected (icy-metaint=%d)",
                            self.stream_id,
                            metaint,
                        )
                        retry_delay = 0

                        state = "AUDIO"
                        audio_bytes_needed = metaint
                        meta_bytes_needed = 0
                        meta_buffer = bytearray()
                        raw_stream = bytearray()

                        async for chunk in response.content.iter_chunked(4096):
                            raw_stream.extend(chunk)

                            while raw_stream:
                                if metaint == 0:
                                    self._audio_buffer.extend(raw_stream)
                                    raw_stream.clear()
                                    break

                                if state == "AUDIO":
                                    take = min(len(raw_stream), audio_bytes_needed)
                                    self._audio_buffer.extend(raw_stream[:take])
                                    del raw_stream[:take]
                                    audio_bytes_needed -= take

                                    if audio_bytes_needed == 0:
                                        state = "META_LENGTH"

                                elif state == "META_LENGTH":
                                    length_byte = raw_stream.pop(0)
                                    meta_bytes_needed = length_byte * 16

                                    if meta_bytes_needed == 0:
                                        audio_bytes_needed = metaint
                                        state = "AUDIO"
                                    else:
                                        meta_buffer.clear()
                                        state = "META_PAYLOAD"

                                elif state == "META_PAYLOAD":
                                    take = min(len(raw_stream), meta_bytes_needed)
                                    meta_buffer.extend(raw_stream[:take])
                                    del raw_stream[:take]
                                    meta_bytes_needed -= take

                                    if meta_bytes_needed == 0:
                                        meta_str = meta_buffer.decode(
                                            "utf-8", errors="ignore"
                                        ).rstrip("\x00")
                                        await self._handle_metadata_change(meta_str)

                                        audio_bytes_needed = metaint
                                        state = "AUDIO"

                            await self._process_audio_buffer()

                        await self._close_outfile()

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    await self._close_outfile()
                    retry_delay = min(retry_delay + 1, 10) if retry_delay > 0 else 0.1
                    logging.info(
                        "[Stream %s] Disconnected (%s). Reconnecting in %ds...",
                        self.stream_id,
                        e,
                        retry_delay,
                    )
                    await asyncio.sleep(retry_delay)

    @classmethod
    async def from_stream(
        cls, url, key, data_folder: str, queue: multiprocessing.Queue
    ):
        """Start multiple streams generator"""
        station, _ = await Station.get_or_create(
            station_identifier=key, url=url, codec=Codec.MP3
        )
        return cls(url, key, data_folder, station, queue)
