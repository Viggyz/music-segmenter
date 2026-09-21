import datetime
import logging
import os
import sys

import asyncio
import aiohttp

# Add the 'radio_listener' root directory to the Python path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from utils.radio_browser_url_fetcher import RadioBrowserUrlFetcher

# --- Logging Configuration ---
# Create directory if missing
os.makedirs("logs/metadata_streamer", exist_ok=True)

# Generate a timestamped log filename per run
log_filename = datetime.datetime.now().strftime("logs/metadata_streamer/run_%Y%m%d_%H%M%S.log")

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)

async def stream_icy_metadata(stream_id: str, url: str):
    headers = {
        "Icy-MetaData": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    }

    initial_delay = 2.0
    max_delay = 60.0
    retry_delay = initial_delay

    while True:
        try:
            logging.info(f"Connecting to stream {stream_id} at {url}...")
            
            # Use ClientTimeout to prevent hanging on stalled network requests
            timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_read=30)

            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, headers=headers) as response:
                    # Retrieve ICY metadata byte interval
                    metaint_header = response.headers.get("icy-metaint")

                    if not metaint_header:
                        logging.error(f"Error: Stream {stream_id} does not support ICY metadata.")
                        # Non-ICY stream won't magically support ICY on retry; retry with long delay
                        await asyncio.sleep(max_delay)
                        continue

                    metaint = int(metaint_header)
                    logging.info(f"Connected to stream {stream_id}! Metadata interval: {metaint} bytes")

                    # Reset retry delay on successful connection and header retrieval
                    retry_delay = initial_delay
                    current_song = ""

                    while True:
                        # 1. Read exactly 'metaint' bytes of audio data
                        try:
                            audio_chunk = await response.content.readexactly(metaint)
                        except (asyncio.IncompleteReadError, aiohttp.ClientPayloadError):
                            logging.warning(f"Stream {stream_id} ended unexpectedly or dropped connection.")
                            break

                        # 2. Read 1 byte for metadata length indicator
                        try:
                            meta_byte = await response.content.readexactly(1)
                        except (asyncio.IncompleteReadError, aiohttp.ClientPayloadError):
                            logging.warning(f"Stream {stream_id} disconnected during metadata length read.")
                            break

                        # Actual metadata length is length_byte * 16
                        meta_length = ord(meta_byte) * 16

                        if meta_length > 0:
                            # 3. Read the exact metadata payload
                            try:
                                meta_data = await response.content.readexactly(meta_length)
                            except (asyncio.IncompleteReadError, aiohttp.ClientPayloadError):
                                logging.warning(f"Stream {stream_id} disconnected during metadata body read.")
                                break

                            # 4. Extract and parse StreamTitle
                            try:
                                meta_str = meta_data.decode("utf-8", errors="ignore")

                                if "StreamTitle=" in meta_str:
                                    title_part = meta_str.split("StreamTitle=")[1]
                                    song_title = title_part.split(";")[0].strip(" '\"")
                                    if song_title and song_title != current_song:
                                        current_song = song_title       
                                        logging.info(f"[NOW PLAYING][{stream_id}] {current_song}")

                            except Exception as e:
                                logging.error(f"[{stream_id}] Error parsing metadata block: {e}")

        except asyncio.CancelledError:
            logging.info(f"Stream task for {stream_id} was cancelled.")
            break
        except Exception as e:
            logging.error(f"[{stream_id}] Connection error: {e}")

        logging.info(f"[{stream_id}] Reconnecting in {retry_delay:.1f} seconds...")
        await asyncio.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, max_delay)

async def fetch_urls(url_fetcher, urls):
    """ Generates list of urls given a url fetcher """
    async for stream_id, url, _ in url_fetcher.fetch_urls():
        urls[stream_id] = url

async def async_producer(urls):
    processes = []
    for stream_id, url in urls.items():
        processes.append(asyncio.create_task(stream_icy_metadata(stream_id, url)))
    await asyncio.gather(*processes)

if __name__ == "__main__":
    url_fetcher = RadioBrowserUrlFetcher()
    urls = {}
    asyncio.run(fetch_urls(url_fetcher, urls))

    # Run the async loop
    asyncio.run(async_producer(urls))