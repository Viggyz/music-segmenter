import asyncio
import collections
import hashlib
import aiohttp
import logging
import sys
import aiofiles
import aiofiles.os
from pathlib import Path

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.StreamHandler(sys.stdout)
                    ])


# Bitrate lookup table (MPEG 1 Layer 3) in kbps
BITRATE_TABLE = [0, 32, 40, 48, 56, 64, 80,
                 96, 112, 128, 160, 192, 224, 256, 320, 0]
# Sample rate lookup table in Hz
SAMPLERATE_TABLE = [44100, 48000, 32000, 0]
# 1,152 samples/frame at 44.1kHz -> 1,225 frames = 32 seconds
# 1,152 samples/frame at 48kHz -> 1,250 frames = 30 seconds
# 1,152 samples/frame at 32kHZ -> 750 frames = 27 seconds
TARGET_AUDIO_FRAMES_TABLE = [1225, 1250, 750]


async def make_folder(folder_path: str | Path) -> None:
    folder_path = Path(folder_path)
    if not await aiofiles.os.path.exists(folder_path):
        await aiofiles.os.makedirs(folder_path, exist_ok=True)
        logging.info("%s created successfully", folder_path)
    else:
        logging.info("%s already exists", folder_path)


def parse_mp3_frame_size(header_bytes: bytes) -> int:
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

    bitrate = BITRATE_TABLE[bitrate_idx] * 1000
    sample_rate = SAMPLERATE_TABLE[sample_rate_idx]

    if bitrate == 0 or sample_rate == 0:
        return -1

    # Formula: FrameSize = int(144 * BitRate / SampleRate) + Padding
    frame_size = int(144 * bitrate / sample_rate) + padding_bit
    return frame_size


async def record_mp3_stream(url: str, stream_id: int):
    """Listens to a live HTTP MP3 endpoint and writes exact 30s audio files asynchronously."""
    # Persistent state preserved across disconnects
    buffer = bytearray()
    frame_count = 0
    file_index = 0

    # Store MD5 hashes of the last 300 frames (~8 seconds) to prevent duplicates
    recent_hashes = collections.deque(maxlen=300)
    out_file = open(f"streams/radio_{stream_id}/part_{file_index:04d}.mp3", "wb")

    retry_delay = 0  # Start with 0ms immediate retryF
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                logging.info("[Stream %s] Connecting to %s...", stream_id, url)
                async with session.get(url, headers={"Icy-MetaData": "0"}) as response:
                    if response.status != 200:
                        raise aiohttp.ClientError(f"HTTP Status {response.status}")
                    logging.info("[Stream %s] Connected/Resumed successfully.", stream_id)
                    retry_delay = 0  # Reset delay on successful connection

                    # Read raw HTTP chunks asynchronously
                    async for chunk in response.content.iter_chunked(4096):
                        buffer.extend(chunk)

                        # Process complete frames out of the incoming byte buffer
                        while len(buffer) >= 4:
                            # Search for MP3 sync word (0xFFE0)
                            if buffer[0] == 0xFF and (buffer[1] & 0xE0) == 0xE0:
                                frame_size = parse_mp3_frame_size(buffer[:4])

                                if frame_size == -1:
                                    # Corrupted/invalid header bit, move forward 1 byte
                                    buffer.pop(0)
                                    continue

                                # Wait for the rest of the frame if buffer is short
                                if len(buffer) < frame_size:
                                    break

                                # Extract frame bytes
                                frame_bytes = buffer[:frame_size]
                                del buffer[:frame_size]

                                # --- Deduplication Check ---
                                frame_hash = hashlib.md5(frame_bytes).hexdigest()
                                if frame_hash in recent_hashes:
                                    # Skip duplicate frame sent by server burst buffer
                                    continue

                                recent_hashes.append(frame_hash)
                                out_file.write(frame_bytes)
                                frame_count += 1

                                sample_rate_idx = (frame_bytes[2] >> 2) & 0x03
                                # Rotate file when exactly 32s worth of frames are reached
                                if frame_count >= TARGET_AUDIO_FRAMES_TABLE[sample_rate_idx]:
                                    # at 1,225 frames we get around 32s worth of playback
                                    out_file.close()
                                    file_index += 1
                                    frame_count = 0
                                    logging.info(
                                        "Creating new part for stream %s to file for part %04d", stream_id, file_index)
                                    out_file = open(
                                        f"streams/radio_{stream_id}/part_{file_index:04d}.mp3", "wb")
                            else:
                                # Advance byte index to find sync header
                                buffer.pop(0)

                    out_file.close()

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                # Immediate reconnection strategy
                retry_delay = min(retry_delay + 1, 10) if retry_delay > 0 else 0.1
                print(f"[Stream {stream_id}] Disconnected ({e}). Reconnecting in {retry_delay}s...")
                await asyncio.sleep(retry_delay)


async def main():
    urls = {
        "93.5": "https://stream-175.surfernetwork.com/9phrkb1e3v8uv?zt=eyJhbGciOiJIUzI1NiJ9.eyJzdHJlYW0iOiI5cGhya2IxZTN2OHV2IiwiaG9zdCI6InN0cmVhbS0xNzUuc3VyZmVybmV0d29yay5jb20iLCJydHRsIjo1LCJqdGkiOiJhUm5UX2JTVFNFeTYydzFSaVdlWU93IiwiaWF0IjoxNzg4NzgzMjk1LCJleHAiOjE3ODg3ODMzNTV9.EqcQk40GQZqHiCiblEfAWw01o-haGuVF6vrM0BBTrok",
        "98.3": "https://stream-284.surfernetwork.com/wgaznsmt92quv?zt=eyJhbGciOiJIUzI1NiJ9.eyJzdHJlYW0iOiJ3Z2F6bnNtdDkycXV2IiwiaG9zdCI6InN0cmVhbS0yODQuc3VyZmVybmV0d29yay5jb20iLCJydHRsIjo1LCJqdGkiOiJUNkQ1aUppNVNYLUdfd0RnTmZ4QTNBIiwiaWF0IjoxNzg4NzgzMzI4LCJleHAiOjE3ODg3ODMzODh9.dkgi3QzfkUhy5md7fVlk-asHJznvFOfMHDSJJLcuovY",
        "92.7": "https://stream-280.surfernetwork.com/dbstwo3dvhhtv?zt=eyJhbGciOiJIUzI1NiJ9.eyJzdHJlYW0iOiJkYnN0d28zZHZoaHR2IiwiaG9zdCI6InN0cmVhbS0yODAuc3VyZmVybmV0d29yay5jb20iLCJydHRsIjo1LCJqdGkiOiJ2SkVKajkzQlNNQzBWeHJ1LVlxejR3IiwiaWF0IjoxNzg4NzgzMzQ2LCJleHAiOjE3ODg3ODM0MDZ9.pJAxrf58sJYhzIVBtIM8FxPlfJ1ai0A212HAtOobEFo"
        # Add up to 10 HTTP MP3 URLs here
    }

    await make_folder("streams")
    for key, _ in urls.items():
        await make_folder(f"streams/radio_{key}")

    tasks = [record_mp3_stream(url, key) for key, url in urls.items()]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
