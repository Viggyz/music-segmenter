import asyncio
import collections
from datetime import datetime
import hashlib
import logging
import multiprocessing
from pathlib import Path
import re

import aiohttp


class StreamProcesser:
    MAX_FILE_DURATION_SEC = 600
    def __init__(self, url, stream_id, data_folder, queue):
        self.url = url
        self.stream_id = stream_id
        self.data_folder = Path(data_folder)
        self._queue = queue

        # Audio stream state
        self._audio_buffer = bytearray()
        self._file_count = 0
        self._file_start_time = None
        self._out_file = None
        self._out_file_name = None

        # Deduplication cache (last ~8 seconds of frames)
        self._recent_hashes = collections.deque(maxlen=300)

        # MPEG 1 Layer 3 lookup tables
        self._BITRATE_TABLE = [
            0, 32, 40, 48, 56, 64, 80, 96, 
            112, 128, 160, 192, 224, 256, 320, 0
        ]
        self._SAMPLERATE_TABLE = [44100, 48000, 32000, 0]

        # ICY Track State
        self.current_artist = "Unknown"
        self.current_song = "Track"

    @property
    def current_title(self) -> str:
        return f"{self.current_artist.replace(" ","_")}-{self.current_song.replace(" ","_")}"

    def _sanitize_filename(self, title: str) -> str:
        """Strips invalid OS characters while preserving 'Artist - Song Name' structure."""
        clean = re.sub(r'[\\/*?:"<>|\']', "", title)
        clean = re.sub(r"\s+", " ", clean).strip()
        return clean or "Unknown_Track"

    def _parse_icy_title(self, meta_str: str) -> tuple[str, str] | None:
        """
        Parses ICY metadata string.
        Extracts StreamTitle and splits into (Artist, Song Name) if formatted as 'Artist - Song'.
        """
        match = re.search(r"StreamTitle='([^']*)'", meta_str)
        if not match:
            return None

        raw_title = match.group(1).strip()
        if not raw_title:
            return None

        # Regex to capture "[Artist] - [Song Name]" with flexible spacing
        artist_song_match = re.match(r"^(?P<artist>.+?)\s*-\s*(?P<song>.+)$", raw_title)
        if artist_song_match:
            artist = artist_song_match.group("artist").strip()
            song = artist_song_match.group("song").strip()
            return artist, song

        return "Unknown_Artist", raw_title

    async def _create_outfile(self, title: str):
        if self._out_file is not None:
            await self._close_outfile()

        safe_title = self._sanitize_filename(title)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_dir = self.data_folder / f"radio_{self.stream_id}"
        output_dir.mkdir(parents=True, exist_ok=True)

        self._file_count += 1
        filename = f"{safe_title}_{timestamp}.mp3"
        self._out_file_name = output_dir / filename
        
        logging.info("[Stream %s] Creating track file: %s", self.stream_id, self._out_file_name.name)
        self._out_file = open(self._out_file_name, "wb")
        self._file_start_time = asyncio.get_running_loop().time()

    async def _write_frame(self, frame_bytes: bytes):
        # Check 6-minute timeout without metadata change
        if self._out_file is not None and self._file_start_time is not None:
            elapsed = asyncio.get_running_loop().time() - self._file_start_time
            if elapsed >= self.MAX_FILE_DURATION_SEC:
                logging.info(
                    "[Stream %s] File reached 6 minutes without track change. Rotating...", 
                    self.stream_id
                )
                await self._close_outfile()

        if self._out_file is None:
            await self._create_outfile(self.current_title)

        self._out_file.write(frame_bytes)

    async def _close_outfile(self):
        if self._out_file is not None:
            self._out_file.close()
            logging.info("[Stream %s] Closed file: %s", self.stream_id, self._out_file_name.name)
            
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
        while len(self._audio_buffer) >= 4 and self.current_song != "Track":
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
        parsed = self._parse_icy_title(meta_str)
        if not parsed:
            return

        new_artist, new_song = parsed
        if (new_artist, new_song) == (self.current_artist, self.current_song):
            return

        logging.info(
            "[Stream %s] ICY Track Change: '%s - %s' -> '%s - %s'",
            self.stream_id, self.current_artist, self.current_song, new_artist, new_song
        )
        
        # Flush pending audio frames to old track before rotating files
        await self._process_audio_buffer()
        await self._close_outfile()
        
        self.current_artist = new_artist
        self.current_song = new_song

    async def record_mp3_stream(self):
        retry_delay = 0
        connector = aiohttp.TCPConnector(ssl=False)
        timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_read=15)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            while True:
                try:
                    logging.info("[Stream %s] Connecting to %s...", self.stream_id, self.url)
                    async with session.get(self.url, headers={"Icy-MetaData": "1"}) as response:
                        if response.status != 200:
                            raise aiohttp.ClientError(f"HTTP Status {response.status}")

                        metaint = int(response.headers.get("icy-metaint", 0))
                        logging.info("[Stream %s] Connected (icy-metaint=%d)", self.stream_id, metaint)
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
                                        meta_str = meta_buffer.decode("utf-8", errors="ignore").rstrip("\x00")
                                        await self._handle_metadata_change(meta_str)
                                        
                                        audio_bytes_needed = metaint
                                        state = "AUDIO"

                            await self._process_audio_buffer()

                        await self._close_outfile()

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    await self._close_outfile()
                    retry_delay = min(retry_delay + 1, 10) if retry_delay > 0 else 0.1
                    logging.info("[Stream %s] Disconnected (%s). Reconnecting in %ds...", self.stream_id, e, retry_delay)
                    await asyncio.sleep(retry_delay)

    @classmethod
    def from_stream(cls, urls: dict[str, str], data_folder: str, queue: multiprocessing.Queue):
        """Start multiple streams generator"""
        for key, url in urls.items():
            yield cls(url, key, data_folder, queue).record_mp3_stream()