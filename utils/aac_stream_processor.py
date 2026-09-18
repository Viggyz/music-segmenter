import asyncio
import collections
from datetime import datetime
import hashlib
import logging
import multiprocessing
from pathlib import Path
import re

import aiohttp


class AACStreamProcessor:
    MAX_FILE_DURATION_SEC = 600

    def __init__(self, url, stream_id, data_folder, queue, output_format: str = "mp3"):
        self.url = url
        self.stream_id = stream_id
        self.data_folder = Path(data_folder)
        self._queue = queue
        
        # Supported output formats: 'm4a', 'mp3', or 'aac'
        self.output_format = output_format.lower().strip()
        if self.output_format not in ("m4a", "mp3", "aac"):
            raise ValueError(f"Unsupported format '{output_format}'. Choose 'm4a', 'mp3', or 'aac'.")

        # Audio stream state
        self._audio_buffer = bytearray()
        self._file_count = 0
        self._file_start_time = None
        self._out_file = None
        self._temp_aac_path = None
        self._final_out_path = None

        # Deduplication cache (last ~8 seconds of frames)
        self._recent_hashes = collections.deque(maxlen=300)

        # ICY Track State
        self.current_artist = "Unknown"
        self.current_song = "Track"

    @property
    def current_title(self) -> str:
        return f"{self.current_artist.replace(' ', '_')}-{self.current_song.replace(' ', '_')}"

    def _sanitize_filename(self, title: str) -> str:
        """Strips invalid OS characters while preserving 'Artist - Song Name' structure."""
        title = title[:120]
        clean = re.sub(r'[\\/*?:"<>|\']', "", title)
        clean = re.sub(r"\s+", " ", clean).strip()
        return clean or "Unknown_Track"

    def _parse_icy_title(self, meta_str: str) -> tuple[str, str] | None:
        """
        Parses ICY metadata string.
        Extracts title from StreamTitle='...' if present, then extracts Artist and Song.
        Supports both:
        1. iHeart / Key-Value format: "Artist - text="Song" ..."
        2. Standard format: "Artist - Song"
        """
        if not meta_str:
            return None

        # Step 1: Extract string inside StreamTitle='...' if present
        match_title = re.search(r"StreamTitle='([^']*)'", meta_str)
        raw_title = match_title.group(1).strip() if match_title else meta_str.strip()

        if not raw_title:
            return None

        # Step 2: Try Pattern 1 (iHeart style), then Pattern 2 (Standard style)
        patterns = [
            r'^(?P<artist>.+?)\s*-\s*text="(?P<song>[^"]+)"',  # Pattern 1 (iHeart)
            r'^(?P<artist>.+?)\s*-\s*(?P<song>.+)$',            # Pattern 2 (Standard)
        ]

        for pattern in patterns:
            match = re.search(pattern, raw_title)
            if match:
                artist = match.group("artist").strip()
                song = match.group("song").strip()
                if artist and song:
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
        
        # Temporary raw AAC file captured during live streaming
        self._temp_aac_path = output_dir / f".temp_{safe_title}_{timestamp}.aac"
        # Final output track path (.m4a / .mp3 / .aac)
        self._final_out_path = output_dir / f"{safe_title}_{timestamp}.{self.output_format}"
        
        logging.info("[Stream %s] Recording track: %s", self.stream_id, self._final_out_path.name)
        self._out_file = open(self._temp_aac_path, "wb")
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

    async def _convert_aac_file(self, src_aac: Path, dst_file: Path):
        """Asynchronously uses FFmpeg to containerize into M4A or transcode into MP3."""
        if self.output_format == "m4a":
            # Lossless remuxing: copies raw AAC bitstream into MP4 container
            cmd = [
                'ffmpeg', '-y', '-hide_banner', '-loglevel', 'error',
                '-i', str(src_aac),
                '-c:a', 'copy',
                str(dst_file)
            ]
        elif self.output_format == "mp3":
            # Transcodes AAC to high quality VBR MP3 (~190-250 kbps)
            cmd = [
                'ffmpeg', '-y', '-hide_banner', '-loglevel', 'error',
                '-i', str(src_aac),
                '-c:a', 'libmp3lame', '-q:a', '2',
                str(dst_file)
            ]
        else:
            # Output format is already raw 'aac'
            src_aac.replace(dst_file)
            return

        # Execute FFmpeg without blocking the asyncio loop
        proc = await asyncio.create_subprocess_exec(*cmd)
        await proc.wait()

        # Delete the temporary .aac file after conversion succeeds
        if src_aac.exists():
            src_aac.unlink()

    async def _close_outfile(self):
        if self._out_file is not None:
            self._out_file.close()
            self._out_file = None

            if self._temp_aac_path and self._temp_aac_path.exists():
                logging.info(
                    "[Stream %s] Finalizing %s file...", 
                    self.stream_id, self.output_format.upper()
                )
                try:
                    await self._convert_aac_file(self._temp_aac_path, self._final_out_path)
                    logging.info("[Stream %s] Saved: %s", self.stream_id, self._final_out_path.name)
                    
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(
                        None, self._queue.put, (self.stream_id, self._final_out_path)
                    )
                except Exception as e:
                    logging.error("[Stream %s] Error converting file: %s", self.stream_id, e)

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
        while len(self._audio_buffer) >= 7 and self.current_song != "Track":
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

    async def record_aac_stream(self):
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
    def from_stream(
        cls, 
        urls: dict[str, str], 
        data_folder: str, 
        queue: multiprocessing.Queue, 
        output_format: str = "m4a"
    ):
        """Start multiple streams generator with specified output format ('m4a' or 'mp3')."""
        for key, url in urls.items():
            yield cls(url, key, data_folder, queue, output_format=output_format).record_aac_stream()