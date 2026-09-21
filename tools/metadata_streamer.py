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

async def stream_icy_metadata(stream_id:str, url: str):
    headers = {
        "Icy-MetaData": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                # Retrieve ICY metadata byte interval
                metaint_header = response.headers.get("icy-metaint")

                if not metaint_header:
                    logging.error(f"Error: Stream {stream_id} does not support ICY metadata.")
                    return

                metaint = int(metaint_header)
                logging.info(f"Connected to stream {stream_id}! Metadata interval: {metaint} bytes\n")

                current_song = ""

                while True:
                    # 1. Read exactly 'metaint' bytes of audio data
                    try:
                        audio_chunk = await response.content.readexactly(metaint)
                    except asyncio.IncompleteReadError:
                        logging.warn(f"Stream {stream_id} ended unexpectedly.")
                        break

                    # 2. Read 1 byte for metadata length indicator
                    try:
                        meta_byte = await response.content.readexactly(1)
                    except asyncio.IncompleteReadError:
                        break

                    # Actual metadata length is length_byte * 16
                    meta_length = ord(meta_byte) * 16

                    if meta_length > 0:
                        # 3. Read the exact metadata payload
                        meta_data = await response.content.readexactly(meta_length)

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
                            logging.error(f"Error parsing metadata block: {e}")

    except Exception as e:
        logging.error(f"Connection failed: {e}")

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