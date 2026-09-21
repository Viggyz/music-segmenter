import json
import re
import sys

# 1. Independent Attribute Search (matches title and artist anywhere in the line)
RE_TITLE_ATTR = re.compile(r'title=["\']([^"\']+)["\']', re.IGNORECASE)
RE_ARTIST_ATTR = re.compile(r'artist=["\']([^"\']+)["\']', re.IGNORECASE)

# 2. Extract Artist before - text="Title"
RE_TEXT_ATTR = re.compile(r'^(?P<artist>.+?)\s*-\s*text=["\'](?P<title>[^"\']+)["\']', re.IGNORECASE)

# 3. Title / Artist Reversed Match
RE_SLASH_REVERSED = re.compile(r'^(?P<title>.+?)\s*/\s*(?P<artist>[^/-]+)$', re.IGNORECASE)

# 4. Duplicated Name Fixer (e.g., "ColdplayColdplay" -> "Coldplay")
RE_DUPLICATE_NAME = re.compile(r'\b([A-Z][a-zA-Z0-9_]+(?:\s+[A-Z][a-zA-Z0-9_]+)*)\1\b')

# 5. Station Noise & Promo Keyword Filter
JUNK_PATTERNS = [
    r'song_spot=["\']T["\']',              # Radio Tease/Promo tag
    r'^StreamTitle=$',                     # Empty stream title
    # r'^Unknown_Artist\s*-\s*(-|Unknown|LA|NY|Dance Wave!|Tracklist:.*|All about Dance.*)$',
    # r'^(MY RADIO DJ|MY CLUB REMIX|My Radip Dj|radioBollyFM|NIUS Radio|SWR3|FFH|Radio Caroline).*',
    # r'^(Kontakt zu|Advert:|Backstage \* \*|\*\*\* Werbung|SWR3 Nachrichten|SWR3 Verkehrszentrum).*',
    # r'^(New Pop - Das Festival|EUROPE 2 - POP RADIO|Kurze Werbepause|Jetzt: NIUS).*'
]
RE_JUNK = re.compile('|'.join(JUNK_PATTERNS), re.IGNORECASE)


def clean_artist_field(artist_str):
    """Helper to sanitize artist strings."""
    if not artist_str:
        return None
    artist_str = artist_str.strip()
    if artist_str.lower() in ["unknown_artist", "unknown", ""]:
        return None
    return artist_str


def parse_log_line(line):
    line = line.strip()
    if not line:
        return None

    # # Step 1: Filter out promos, teases, and station noise
    if RE_JUNK.search(line):
        return None

    # Step 2: Handle Key-Value dumps independently (order doesn't matter)
    title_match = RE_TITLE_ATTR.search(line)
    artist_match = RE_ARTIST_ATTR.search(line)
    if title_match and artist_match:
        return {
            "artist": clean_artist_field(artist_match.group(1)),
            "title": title_match.group(1).strip()
        }

    # Step 3: Global Pre-Processing (Apply fixes BEFORE matching rest of rules)
    cleaned = RE_DUPLICATE_NAME.sub(r'\1', line)
    cleaned = re.sub(r'^Unknown_Artist\s*-\s*', '', cleaned, flags=re.IGNORECASE).strip()

    # Step 4: Handle "Artist - text='Title'" format
    match = RE_TEXT_ATTR.match(cleaned)
    if match:
        return {
            "artist": clean_artist_field(match.group("artist")),
            "title": match.group("title").strip()
        }

    # Step 5: Handle "Title / Artist" (Reversed slash format)
    match = RE_SLASH_REVERSED.match(cleaned)
    if match:
        return {
            "artist": clean_artist_field(match.group("artist")),
            "title": match.group("title").strip()
        }

    # Step 6: Standard "Artist - Title" format
    if " - " in cleaned:
        parts = cleaned.split(" - ")
        
        # Discard lines where field repeats (e.g., "Title Music - Title Music")
        if len(parts) == 2 and parts[0].strip().lower() == parts[1].strip().lower():
            return None
            
        if len(parts) == 2:
            return {
                "artist": clean_artist_field(parts[0]),
                "title": parts[1].strip()
            }
        
        # Multi-hyphen smart splitter
        split_idx = 1
        for i in range(1, len(parts) - 1):
            p = parts[i].lower()
            if any(k in p for k in [',', 'and', '&', 'feat', 'ft.']):
                split_idx = i + 1
            else:
                break
        
        artist = " - ".join(parts[:split_idx]).strip()
        title = " - ".join(parts[split_idx:]).strip()
        
        if artist.lower() == title.lower():
            return None
            
        return {
            "artist": clean_artist_field(artist),
            "title": title
        }

    return {"artist": None, "title": cleaned}

songs = []
for line in open(sys.argv[1], 'r'):
    res = parse_log_line(line)
    if res is not None:
        songs.append(res)
with open('tools/processed_titles.json', 'w') as f:
    json.dump(songs, f)