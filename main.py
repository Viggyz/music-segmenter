import asyncio

import logging
import sys

from utils.stream_processing import StreamProcesser
from utils.file import make_folder
from utils.rupture_segmenter import RuptureSegmenter
from utils.radio_browser_url_fetcher import RadioBrowserUrlFetcher

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.StreamHandler(sys.stdout)
                    ])


async def main():
    # urls = {
    #     # "93.5": "https://stream-175.surfernetwork.com/9phrkb1e3v8uv?zt=eyJhbGciOiJIUzI1NiJ9.eyJzdHJlYW0iOiI5cGhya2IxZTN2OHV2IiwiaG9zdCI6InN0cmVhbS0xNzUuc3VyZmVybmV0d29yay5jb20iLCJydHRsIjo1LCJqdGkiOiJhUm5UX2JTVFNFeTYydzFSaVdlWU93IiwiaWF0IjoxNzg4NzgzMjk1LCJleHAiOjE3ODg3ODMzNTV9.EqcQk40GQZqHiCiblEfAWw01o-haGuVF6vrM0BBTrok",
    #     "98.3": "https://stream-284.surfernetwork.com/wgaznsmt92quv?zt=eyJhbGciOiJIUzI1NiJ9.eyJzdHJlYW0iOiJ3Z2F6bnNtdDkycXV2IiwiaG9zdCI6InN0cmVhbS0yODQuc3VyZmVybmV0d29yay5jb20iLCJydHRsIjo1LCJqdGkiOiJUNkQ1aUppNVNYLUdfd0RnTmZ4QTNBIiwiaWF0IjoxNzg4NzgzMzI4LCJleHAiOjE3ODg3ODMzODh9.dkgi3QzfkUhy5md7fVlk-asHJznvFOfMHDSJJLcuovY",
    #     "92.7": "https://stream-280.surfernetwork.com/dbstwo3dvhhtv?zt=eyJhbGciOiJIUzI1NiJ9.eyJzdHJlYW0iOiJkYnN0d28zZHZoaHR2IiwiaG9zdCI6InN0cmVhbS0yODAuc3VyZmVybmV0d29yay5jb20iLCJydHRsIjo1LCJqdGkiOiJ2SkVKajkzQlNNQzBWeHJ1LVlxejR3IiwiaWF0IjoxNzg4NzgzMzQ2LCJleHAiOjE3ODg3ODM0MDZ9.pJAxrf58sJYhzIVBtIM8FxPlfJ1ai0A212HAtOobEFo",
    #     "old_hits": "https://stream-287.surfernetwork.com/a2gyqzwpwfeuv?zt=eyJhbGciOiJIUzI1NiJ9.eyJzdHJlYW0iOiJhMmd5cXp3cHdmZXV2IiwiaG9zdCI6InN0cmVhbS0yODcuc3VyZmVybmV0d29yay5jb20iLCJydHRsIjo1LCJqdGkiOiI4TFFkMkRXN1FUQ3NjSHVieVhsX0VRIiwiaWF0IjoxNzg5MTM4ODk3LCJleHAiOjE3ODkxMzg5NTd9.a_WgF6HlnN3ddqjrqf6lfndLsc6bGCLhLdzEPTLJ460",
    #     # Add up to 10 HTTP MP3 URLs here
    # }

    url_fetcher = RadioBrowserUrlFetcher()
    urls = {stream_id: url async for stream_id, url in url_fetcher.fetch_urls()}

    DATA_FOLDER = "streams"
    SEGMENT_FOLDER = "segments"

    await make_folder(DATA_FOLDER)
    await make_folder(SEGMENT_FOLDER)
    for key, _ in urls.items():
        await make_folder(f"{DATA_FOLDER}/radio_{key}")
        await make_folder(f"{SEGMENT_FOLDER}/radio_{key}")

    queue = asyncio.Queue(maxsize=10)

    consumers = [asyncio.create_task(RuptureSegmenter.create_and_return_segmenter(
        DATA_FOLDER, SEGMENT_FOLDER, queue)) for _ in range(len(urls.keys()))]

    producers = [asyncio.create_task(
        task) for task in StreamProcesser.from_stream(urls, DATA_FOLDER, queue)]
    
    await asyncio.gather(*producers)


    await queue.join()
    logging.warning("Queue joined")

    for c in consumers:
        logging.warning("Closing consumer")
        c.cancel()

if __name__ == "__main__":
    asyncio.run(main())
