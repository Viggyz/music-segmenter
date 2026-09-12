import collections
from datetime import datetime
import hashlib
import logging
from pathlib import Path

import aiohttp

import asyncio


class StreamProcesser:
    def __init__(self, url, stream_id, data_folder, queue):
        self.url = url
        self.stream_id = stream_id
        self.data_folder = Path(data_folder)
        self._queue = queue

        # Persistent state preserved across disconnects
        self._buffer = bytearray()
        self._frame_count = 0
        self._file_count = 0 
        self._out_file = None
        self._out_file_name = None

        # Store MD5 hashes of the last 300 frames (~8 seconds) to prevent duplicates
        self._recent_hashes = collections.deque(maxlen=300)

        # Bitrate lookup table (MPEG 1 Layer 3) in kbps
        self._BITRATE_TABLE = [0, 32, 40, 48, 56, 64, 80,
                               96, 112, 128, 160, 192, 224, 256, 320, 0]
        # Sample rate lookup table in Hz
        self._SAMPLERATE_TABLE = [44100, 48000, 32000, 0]
        # 1,152 samples/frame at 44.1kHz -> 1,225 frames = 32 seconds
        # 1,152 samples/frame at 48kHz -> 1,250 frames = 30 seconds
        # 1,152 samples/frame at 32kHZ -> 750 frames = 27 seconds
        self._TARGET_AUDIO_FRAMES_TABLE = [1225, 1250, 750]

    async def _create_outfile(self):
        if self._out_file is not None:
            await self._close_outfile()
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self._out_file_name = self.data_folder / \
            f"radio_{self.stream_id}" / f"{timestamp}.mp3"
        logging.info("Creating %s...", self._out_file_name)
        self._out_file = open(self._out_file_name, "wb")

    def _write_to_outfile(self, bytes):
        assert self._out_file is not None, "Missing outfile"
        self._out_file.write(bytes)

    async def _close_outfile(self):
        self._out_file.close()
        logging.info('Closing %s and Adding queue', self._out_file_name)
        await self._queue.put((self.stream_id, self._out_file_name))
        self._file_count += 1

    def _is_frame_processed(self, frame_bytes):
        # --- Deduplication Check ---
        frame_hash = hashlib.md5(frame_bytes).hexdigest()
        if frame_hash in self._recent_hashes:
            # Skip duplicate frame sent by server burst buffer
            return True

        self._recent_hashes.append(frame_hash)
        return False

    def parse_mp3_frame_size(self, header_bytes: bytes) -> int:
        """Parses a 4-byte header and returns the exact byte size of the MP3 frame."""
        b1, b2, b3, _ = header_bytes

        # Check MP3 Sync Word (11 consecutive 1s: 0xFF followed by top 3 bits = 1)
        if not (b1 == 0xFF and (b2 & 0xE0) == 0xE0):
            return -1
        # b1 being ff + e means that its a valid frame sync header.
        # b1 + b2 just tells us its mpeg version 1 and layer 3

        bitrate_idx = (b3 >> 4) & 0x0F
        sample_rate_idx = (b3 >> 2) & 0x03
        padding_bit = (b3 >> 1) & 0x01

        bitrate = self._BITRATE_TABLE[bitrate_idx] * 1000
        sample_rate = self._SAMPLERATE_TABLE[sample_rate_idx]

        if bitrate == 0 or sample_rate == 0:
            return -1

        # Formula: FrameSize = int(144 * BitRate / SampleRate) + Padding
        frame_size = int(144 * bitrate / sample_rate) + padding_bit
        return frame_size

    async def record_mp3_stream(self):
        """Listens to a live HTTP MP3 endpoint and writes exact 30s audio files asynchronously."""

        await self._create_outfile()

        retry_delay = 0  # Start with 0ms immediate retryF
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            while True: # break once we process 5 files
                try:
                    logging.info("[Stream %s] Connecting to %s...",
                                 self.stream_id, self.url)
                    async with session.get(self.url, headers={"Icy-MetaData": "0"}) as response:
                        if response.status != 200:
                            raise aiohttp.ClientError(
                                f"HTTP Status {response.status}")
                        logging.info(
                            "[Stream %s] Connected/Resumed successfully.", self.stream_id)
                        retry_delay = 0  # Reset delay on successful connection

                        # Read raw HTTP chunks asynchronously
                        async for chunk in response.content.iter_chunked(4096):
                            self._buffer.extend(chunk)

                            # Process complete frames out of the incoming byte buffer
                            while len(self._buffer) >= 4:
                                # Search for MP3 sync word (0xFFE0)
                                if self._buffer[0] == 0xFF and (self._buffer[1] & 0xE0) == 0xE0:
                                    frame_size = self.parse_mp3_frame_size(
                                        self._buffer[:4])

                                    if frame_size == -1:
                                        # Corrupted/invalid header bit, move forward 1 byte
                                        self._buffer.pop(0)
                                        continue

                                    # Wait for the rest of the frame if buffer is short
                                    if len(self._buffer) < frame_size:
                                        break

                                    # Extract frame bytes
                                    frame_bytes = self._buffer[:frame_size]
                                    del self._buffer[:frame_size]

                                    if self._is_frame_processed(frame_bytes):
                                        continue
                                    self._write_to_outfile(frame_bytes)
                                    self._frame_count += 1

                                    sample_rate_idx = (
                                        frame_bytes[2] >> 2) & 0x03
                                    # Rotate file when exactly 32s worth of frames are reached
                                    if self._frame_count >= self._TARGET_AUDIO_FRAMES_TABLE[sample_rate_idx]:
                                        # at 1,225 frames we get around 32s worth of playback
                                        self._frame_count = 0
                                        await self._create_outfile()
                                else:
                                    # Advance byte index to find sync header
                                    self._buffer.pop(0)

                        await self._close_outfile()

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    # Immediate reconnection strategy
                    retry_delay = min(
                        retry_delay + 1, 10) if retry_delay > 0 else 0.1
                    logging.info(
                        "[Stream %s] Disconnected (%s). Reconnecting in %ds...", self.stream_id, e, retry_delay)
                    await asyncio.sleep(retry_delay)

    @classmethod
    def from_stream(cls, urls: dict[str, str], data_folder: str, queue: asyncio.Queue):
        """Start multiple streams"""
        for key, url in urls.items():
            yield cls(url, key, data_folder, queue).record_mp3_stream()
