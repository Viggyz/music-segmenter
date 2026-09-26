import datetime
import logging
import os
import sys
import csv

import laya
from laya import Router

# Initialize router with preloading (avoids swap delay)
router = Router(preload=True)


# --- Logging Configuration ---
# Create directory if missing
os.makedirs("logs/split3", exist_ok=True)

# Generate a timestamped log filename per run
log_filename = datetime.datetime.now().strftime("logs/split3/run_%Y%m%d_%H%M%S.log")

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)

logging.getLogger("typesafe_sdk").setLevel(logging.DEBUG)

if __name__ == "__main__":
    file = open(sys.argv[1], 'r', encoding='utf-8')
    seen = set()
    titles = csv.DictReader(file)   

    for row in titles:
        # Define multiple questions of different primitives
        questions = {
            "title_type": {
                "type": "choice",
                "instructions": "This is a radio stream title, it can refer to a radio jingle, an commercial, a song track playing or a content segment, which of these is it?",
                "criteria": {
                    "content_segment": "planned block of programming to keep listeners engaged",
                    "song": "title has an identifyable track title and artist name",
                    "commercial": "a paid advertiserment to promote a product",
                    "radio_jingle": "a short, catchy tune to promote the station"
                }
            },
        }

        # Single forward pass: evaluates all questions simultaneously
        res = router.predict(row['stream_title'], questions, model='multilingual')

        print("Routing Decision :", res["routing"]["model"])    
        # -> english

        print("Assigned Queue   :", res["answers"]["title_type"]["choice"], " for :", row["stream_title"])
        # -> infrastructure (confidence: 0.96)
