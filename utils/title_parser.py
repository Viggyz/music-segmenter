import csv
import logging
import re
import sys

from models import StreamTitleParserMetadata

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# --- Pre-compiled Regular Expressions ---
RE_ASTERISK = re.compile(r"^\*\*\*")
RE_WWW = re.compile(r"www\.")
RE_TALK_SPOT = re.compile(r'song_spot="+T"+')
RE_KISS_CHECK = re.compile(r'song_spot="+(?:M|F)"+')
RE_KISS_TITLE_FLAG = re.compile(r'title="+|-\s*text="+"')

RE_VON = re.compile(r'"*([^"]+)"*\s+von\s+(.+)', re.IGNORECASE)
RE_SLASH = re.compile(r"^(.*?)\s*/\s*(.+)$")
RE_HYPHEN = re.compile(r"^(.*?)\s+-\s+(.+)$")
RE_PIPE = re.compile(r"\|\s*([^|]+?)\s*-\s*([^|]+?)\s*\|")
RE_PLAIN = re.compile(r'^\s*([^/\-"=]+?)\s*$')

# Field-specific extraction regexes
RE_KISS_ARTIST_1 = re.compile(r'artist="+(.*?)"{1,2}(?:,|$|\s)')
RE_KISS_ARTIST_2 = re.compile(r'([^",]+?)\s*-\s*text="+"')
RE_KISS_TITLE_1 = re.compile(r'title="+(.*?)"{1,2}(?:,|$|\s)')
RE_KISS_TITLE_2 = re.compile(r'-\s*text="+(.*?)"{1,2}(?:\s+|$)')
RE_TALK_TITLE_2 = re.compile(r'text="+(.*?)"{1,2}(?:\s+|$)')


def parse_stream_title(stream_title: str) -> dict:
    """Parses a single stream_title string into artist, title, and format flags."""
    if not stream_title:
        return {"artist": None, "title": None, "no_title_split": False}

    # --- Format Condition Checks ---
    starts_with_three_astrisk = bool(RE_ASTERISK.search(stream_title))
    has_www = bool(RE_WWW.search(stream_title))
    is_talk_spot = bool(RE_TALK_SPOT.search(stream_title))

    is_kiss_format = bool(
        RE_KISS_CHECK.search(stream_title) and RE_KISS_TITLE_FLAG.search(stream_title)
    )
    is_von_format = bool(RE_VON.search(stream_title)) and not starts_with_three_astrisk
    is_slash_format = (
        bool(RE_SLASH.search(stream_title))
        and not re.search(r"\s+-\s+", stream_title)
        and not starts_with_three_astrisk
    )

    # --- Artist Extraction (Sequential Fallback) ---
    artist = None
    if is_kiss_format:
        if m := RE_KISS_ARTIST_1.search(stream_title):
            artist = m.group(1)
        elif m := RE_KISS_ARTIST_2.search(stream_title):
            artist = m.group(1)

    if artist is None and is_von_format:
        if m := RE_VON.search(stream_title):
            artist = m.group(2)  # Group 2 is Artist in "von" format

    if artist is None:
        if m := RE_PIPE.search(stream_title):
            artist = m.group(1)

    if artist is None:
        if m := RE_HYPHEN.search(stream_title):
            artist = m.group(1)

    if artist is None and is_slash_format:
        if m := RE_SLASH.search(stream_title):
            artist = m.group(2)  # Group 2 is Artist in slash format

    # --- Title Extraction (Sequential Fallback) ---
    title = None
    if is_kiss_format:
        if m := RE_KISS_TITLE_1.search(stream_title):
            title = m.group(1)
        elif m := RE_KISS_TITLE_2.search(stream_title):
            title = m.group(1)

    if title is None and is_talk_spot:
        if m := RE_KISS_TITLE_1.search(stream_title):
            title = m.group(1)
        elif m := RE_TALK_TITLE_2.search(stream_title):
            title = m.group(1)

    if title is None and is_von_format:
        if m := RE_VON.search(stream_title):
            title = m.group(1)  # Group 1 is Title in "von" format

    if title is None:
        if m := RE_PIPE.search(stream_title):
            title = m.group(2)

    if title is None:
        if m := RE_HYPHEN.search(stream_title):
            title = m.group(2)

    if title is None and is_slash_format:
        if m := RE_SLASH.search(stream_title):
            title = m.group(1)

    if title is None:
        if m := RE_PLAIN.search(stream_title):
            title = m.group(1)

    # --- Boolean Format Flags ---
    return {
        "artist": artist,
        "title": title,
        "kiss_format_split": is_kiss_format,
        "frisky_format_split": bool(RE_PIPE.search(stream_title)),
        "von_format_split": is_von_format,
        "hyphen_split": bool(
            RE_HYPHEN.search(stream_title)
            and not starts_with_three_astrisk
            and not has_www
        ),
        "parsed_slash_split": is_slash_format,
        "no_title_split": bool(
            RE_PLAIN.search(stream_title)
            or is_talk_spot
            or starts_with_three_astrisk
            or has_www
        ),
    }


async def get_title_and_artist(stream_title: str) -> str:
    processed = parse_stream_title(stream_title)

    # write to db
    meta_ = await StreamTitleParserMetadata.create(metadata_fields=processed)

    if processed["no_title_split"] or not processed['title'] or not processed['artist']:
        logging.info("[TITLE_PARSER] %s is not a valid track", processed["title"])
        return {"artist": None, "title": None, "is_track": False, "meta_id": meta_.id}
    else:
        logging.info(
            "[TITLE_PARSER] Got a valid track %s - %s",
            processed["title"],
            processed["artist"],
        )
        return {
            "artist": processed["artist"],
            "title": processed["title"],
            "is_track": True,
            "meta_id": meta_.id,
        }
