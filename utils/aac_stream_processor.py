import asyncio
import collections
import hashlib
import logging
import multiprocessing
import re
import uuid
from datetime import datetime
from pathlib import Path

import aiohttp
import av

from models import Codec, Station, StreamClip
from utils.title_parser import get_title_and_artist

DEFAULT_CURRENT_TITLE = "[No title set]"


class AACStreamProcessor:
    MAX_FILE_DURATION_SEC = 600

    def __init__(
        self,
        url,
        stream_id,
        data_folder,
        stream_id_obj,
        queue,
        output_format: str = "mp3",
    ):
        self.url = url
        self.stream_id = stream_id
        self.data_folder = Path(data_folder)
        self._queue = queue
        self._stream_obj = stream_id_obj

        # Supported output formats: 'm4a', 'mp3', or 'aac'
        self.output_format = output_format.lower().strip()
        if self.output_format not in ("m4a", "mp3", "aac"):
            raise ValueError(
                f"Unsupported format '{output_format}'. Choose 'm4a', 'mp3', or 'aac'."
            )

        # Audio stream state
        self._audio_buffer = bytearray()
        self._file_count = 0
        self._file_start_time = None
        self._out_file = None
        self._temp_aac_path = None
        self._final_out_path = None
        self._current_file = None

        # Deduplication cache (last ~8 seconds of frames)
        self._recent_hashes = collections.deque(maxlen=300)

        # ICY Track State
        self.current_title = DEFAULT_CURRENT_TITLE

    async def _create_outfile(self):
        if self._out_file is not None:
            await self._close_outfile()

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_dir = self.data_folder / f"radio_{self.stream_id}"
        output_dir.mkdir(parents=True, exist_ok=True)

        self._file_count += 1

        # Temporary raw AAC file captured during live streaming
        self._temp_aac_path = output_dir / f".temp_{timestamp}.aac"
        # Final output track path (.m4a / .mp3 / .aac)
        self._final_out_path = output_dir / f"{timestamp}.{self.output_format}"

        logging.info(
            "[Stream %s] Recording track: %s", self.stream_id, self._final_out_path.name
        )
        self._out_file = open(self._temp_aac_path, "wb")
        self._file_start_time = asyncio.get_running_loop().time()
        self._current_file = await StreamClip.create(
            station_id=self._stream_obj,
            file_path=self._temp_aac_path,
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

    @staticmethod
    def _pyav_remux_aac_to_m4a(src_aac: Path, dst_file: Path):
        """Converts raw ADTS AAC file into an M4A (MP4) container using PyAV."""
        with av.open(str(src_aac)) as in_container, av.open(
            str(dst_file), mode="w", format="mp4"
        ) as out_container:
            in_stream = in_container.streams.audio[0]

            rate = in_stream.rate or 44100
            layout_str = (
                in_stream.layout.name
                if getattr(in_stream, "layout", None)
                else ("stereo" if getattr(in_stream, "channels", 2) == 2 else "mono")
            )

            out_stream = out_container.add_stream("aac", rate=rate)
            out_stream.layout = layout_str
            if getattr(in_stream, "bit_rate", None):
                out_stream.bit_rate = in_stream.bit_rate

            # Audio resampler ensures frame sample format (fltp) and layout alignment
            resampler = av.AudioResampler(
                format="fltp",
                layout=layout_str,
                rate=rate,
            )

            # Decode raw AAC frames and encode into M4A container
            for frame in in_container.decode(in_stream):
                for resampled_frame in resampler.resample(frame):
                    for packet in out_stream.encode(resampled_frame):
                        out_container.mux(packet)

            # Flush resampler buffer
            for resampled_frame in resampler.resample(None):
                for packet in out_stream.encode(resampled_frame):
                    out_container.mux(packet)

            # Flush encoder buffer
            for packet in out_stream.encode(None):
                out_container.mux(packet)

    @staticmethod
    def _pyav_transcode_aac_to_mp3(src_aac: Path, dst_file: Path):
        """Transcodes raw ADTS AAC file into an MP3 file using PyAV."""
        with av.open(str(src_aac)) as in_container, av.open(
            str(dst_file), mode="w", format="mp3"
        ) as out_container:
            in_stream = in_container.streams.audio[0]

            rate = in_stream.rate or 44100
            layout_str = (
                in_stream.layout.name
                if getattr(in_stream, "layout", None)
                else ("stereo" if getattr(in_stream, "channels", 2) == 2 else "mono")
            )

            out_stream = out_container.add_stream("mp3", rate=rate)

            resampler = av.AudioResampler(
                format="s16p",
                layout=layout_str,
                rate=rate,
            )

            for frame in in_container.decode(in_stream):
                for resampled_frame in resampler.resample(frame):
                    for packet in out_stream.encode(resampled_frame):
                        out_container.mux(packet)

            # Flush resampler
            for resampled_frame in resampler.resample(None):
                for packet in out_stream.encode(resampled_frame):
                    out_container.mux(packet)

            # Flush encoder
            for packet in out_stream.encode(None):
                out_container.mux(packet)

    async def _convert_aac_file(self, src_aac: Path, dst_file: Path):
        try:
            if self.output_format == "m4a":
                await asyncio.to_thread(self._pyav_remux_aac_to_m4a, src_aac, dst_file)
            elif self.output_format == "mp3":
                await asyncio.to_thread(
                    self._pyav_transcode_aac_to_mp3, src_aac, dst_file
                )
            else:
                # Format is raw 'aac'
                src_aac.replace(dst_file)
        finally:
            if src_aac.exists():
                src_aac.unlink()

    async def _close_outfile(self):
        if self._out_file is not None:
            self._out_file.close()
            self._out_file = None

            if self._temp_aac_path and self._temp_aac_path.exists():
                logging.info(
                    "[Stream %s] Finalizing %s file...",
                    self.stream_id,
                    self.output_format.upper(),
                )
                try:
                    await self._convert_aac_file(
                        self._temp_aac_path, self._final_out_path
                    )

                    # Update the filepath in the schema
                    self._current_file.file_path = self._final_out_path
                    await self._current_file.save(update_fields=["file_path"])
                    logging.info(
                        "[Stream %s] Saved: %s",
                        self.stream_id,
                        self._final_out_path.name,
                    )

                    # loop = asyncio.get_running_loop()
                    # await loop.run_in_executor(
                    #     None, self._queue.put, (self.stream_id, self._final_out_path)
                    # )
                except Exception as e:
                    logging.error(
                        "[Stream %s] Error converting file: %s", self.stream_id, e
                    )

            self._temp_aac_path = None
            self._final_out_path = None

    def _is_frame_processed(self, frame_bytes: bytes) -> bool:
        frame_hash = hashlib.md5(frame_bytes).hexdigest()
        if frame_hash in self._recent_hashes:
            return True
        self._recent_hashes.append(frame_hash)
        return False

    def parse_adts_frame_size(self, header_bytes: bytes) -> int:
        """Parses ADTS frame length from the 7-byte AAC header."""
        if len(header_bytes) < 7:
            return -1
        b0, b1, _, _, b4, b5, _ = header_bytes[:7]

        # Check ADTS syncword (12 bits set to 1)
        if not (b0 == 0xFF and (b1 & 0xF0) == 0xF0):
            return -1

        b3 = header_bytes[3]
        frame_length = ((b3 & 0x03) << 11) | (b4 << 3) | (b5 >> 5)
        return frame_length

    async def _process_audio_buffer(self):
        """Parses and writes all complete ADTS AAC frames sitting in _audio_buffer."""
        while (
            len(self._audio_buffer) >= 7 and self.current_title != DEFAULT_CURRENT_TITLE
        ):
            b0 = self._audio_buffer[0]
            b1 = self._audio_buffer[1]
            if b0 == 0xFF and (b1 & 0xF0) == 0xF0:
                frame_size = self.parse_adts_frame_size(self._audio_buffer[:7])
                if frame_size <= 0:
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
        if new_title == "":
            return

        logging.info(
            "[Stream %s] ICY Track Change: '%s' -> '%s'",
            self.stream_id,
            self.current_title,
            new_title,
        )

        if self.current_title != DEFAULT_CURRENT_TITLE:
            # Flush pending audio frames to old track before rotating files
            await self._process_audio_buffer()
            await self._close_outfile()
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

    async def record_aac_stream(self):
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
        cls,
        url,
        key,
        data_folder: str,
        queue: multiprocessing.Queue,
        output_format: str = "mp3",
    ):
        """Start multiple streams generator with specified output format ('m4a' or 'mp3')."""
        station, _ = await Station.get_or_create(
            station_identifier=key, url=url, codec=Codec.AAC
        )
        return cls(url, key, data_folder, station, queue, output_format=output_format)
