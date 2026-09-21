import datetime
import json
import logging
import os
import re
import sys

import asyncio
from typesafe_sdk import AsyncTypeSafeClient, Choice, Noul
from dotenv import load_dotenv

load_dotenv()

# --- Logging Configuration ---
# Create directory if missing
os.makedirs("logs/split2", exist_ok=True)

# Generate a timestamped log filename per run
log_filename = datetime.datetime.now().strftime("logs/split2/run_%Y%m%d_%H%M%S.log")

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)

logging.getLogger("typesafe_sdk").setLevel(logging.DEBUG)

# 1. Regex to extract content inside double quotes
RE_DOUBLE_QUOTES = re.compile(r'"([^"]*)"')

# 2. Regex to split on delimiters: ' - ' (spaced hyphen), '/', '|', or '->'
# RE_ALL_DELIMITERS = re.compile(r'\s+-\s+|\s*,\s|\s* and \s*|\s*/\s*|\s*\|\s*|\s*->\s*')
RE_ALL_DELIMITERS = re.compile(r'\s+-\s+|\s*/\s*|\s*\|\s*|\s*->\s*')

# 3. Regex to strip standalone XML attribute tags/keys when unquoted (e.g., song_spot=, MediaBaseId=)
RE_XML_ATTR = re.compile(r'\b[a-zA-Z0-9_]+=\b')

def extract_text_blocks(raw_line):
    """
    Extracts text blocks from both quoted XML logs and unquoted raw text.
    Splits on ' - ', '/', '|', and '->' while keeping 'KHFI-FM' intact.
    """
    quoted_matches = RE_DOUBLE_QUOTES.findall(raw_line)
    
    # Target segments: either quoted strings OR the full unquoted line
    if quoted_matches:
        target_segments = quoted_matches
    else:
        # If unquoted, strip any orphan XML attribute tags (e.g. key=) before parsing
        clean_unquoted = RE_XML_ATTR.sub('', raw_line)
        target_segments = [clean_unquoted]
        
    all_blocks = []
    
    for segment in target_segments:
        if not segment.strip():
            continue  # Skip empty attributes like MediaBaseId=""
            
        # Split on any of the supported delimiters
        sub_blocks = RE_ALL_DELIMITERS.split(segment)
        
        for b in sub_blocks:
            cleaned = b.strip()
            if cleaned:
                all_blocks.append(cleaned)
                
    return all_blocks

async def main():
    file = open(sys.argv[1], 'r')
    seen = set()
    test_lines = []
    results = []
    for line in file:
        if line not in seen:
            test_lines.append(line)

    for _, stream_title in enumerate(test_lines, 1):
        blocks = extract_text_blocks(stream_title)
        logging.info("Logging for line %s, with blocks %s", stream_title, blocks)
        formatted_blocks = {key: None for key in blocks}
        async with AsyncTypeSafeClient() as client:
            result = await client.system_one(
                stream_title,
                {
                    "title": Choice(
                        instructions="This is a radio stream title, what's track name from this metadata",
                        criteria=formatted_blocks | {"none": "None of these is the requested value"},
                    ),
                    "primary_artist": Choice(
                        instructions="This is a radio stream title, multiple artists could be a string separated by commas. Who is the primary artist.",
                        criteria=formatted_blocks | {"none": "None of these is the requested value"},
                    ),
                    "secondary_artist": Choice(
                                            instructions="This is a radio stream title, multiple artists could be a string separated by commas. Who is the secondary artist",
                                            criteria=formatted_blocks | {"none": "None of these is the requested value"},
                                        ),
                    "title_type": Choice(instructions="This is a radio stream title, it can refer to a radio jingle, an commercial, a song track playing or a content segment, which of these is it?",
                                         criteria={
                                             "content_segment": None,
                                             "track": None,
                                             "commercial": None,
                                             "radio_jingle": None
                                         })

                },
            )
        # print(line, " - ", blocks)
        results.append({
            "original_line": stream_title,
            "title": {
                'choice': result.choices['title'].choice,
                'confidence': result.choices['title'].confidence,
                'probabilities': result.choices['title'].probabilities
            },
            "primary_artist": {
                'choice': result.choices['primary_artist'].choice,
                'confidence': result.choices['primary_artist'].confidence,
                'probabilities': result.choices['primary_artist'].probabilities
            },
            "secondary_artist": {
                'choice': result.choices['secondary_artist'].choice,
                'confidence': result.choices['secondary_artist'].confidence,
                'probabilities': result.choices['secondary_artist'].probabilities
            },
            "title_type": {
                'choice': result.choices['title_type'].choice,
                'confidence': result.choices['title_type'].confidence,
                'probabilities': result.choices['title_type'].probabilities
            }
        })
        # pprint.pp(results[-1])
    json.dump(results, open(sys.argv[2], 'w'))

if __name__ == "__main__":
    assert sys.argv[2] is not None, "Pass a second arg to write to file`"
    asyncio.run(main())