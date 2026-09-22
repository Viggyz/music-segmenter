import csv
import json
import musicbrainzngs
import time
import sys

# --- CONFIGURATION ---
# 1. Initialize the mandatory User-Agent (Format: AppName, Version, Contact)
musicbrainzngs.set_useragent(
    "RadioLogParserApp", "1.0", "devvig19@example.com")

# Optional: Disable the library's default 1-request-per-second built-in limit
# so we can manage our own custom pacing.
musicbrainzngs.set_rate_limit(False)

# Target ~30 requests per second pacing
REQUEST_DELAY = 1.0 / 30.0


def search_musicbrainz(artist, title):
    """Query MusicBrainz using the official musicbrainzngs Python package"""
    try:
        # The library abstracts away the URL formatting and JSON parsing
        result = musicbrainzngs.search_recordings(
            artist=artist, recording=title, limit=1)

        recordings = result.get("recording-list", [])
        if recordings:
            rec = recordings[0]
            rec_title = rec.get("title", title)

            artist_name = artist
            if "artist-credit" in rec and rec["artist-credit"]:
                credit = rec["artist-credit"][0]
                if isinstance(credit, dict) and "artist" in credit:
                    artist_name = credit["artist"].get("name", artist)
                elif isinstance(credit, dict) and "name" in credit:
                    artist_name = credit.get("name", artist)

            return {
                "source": "musicbrainzngs",
                "artist": artist_name,
                "title": rec_title
            }

    except musicbrainzngs.WebServiceError as e:
        print(f"MusicBrainz WebService Error: {e}")
    except Exception as e:
        print(f"Unexpected Error: {e}")

    return {"source": "none", "artist": artist, "title": title}


def process_queries_to_csv(queries, output_filename="musicbrainzngs_resolved_logs.csv"):
    """Processes tracks via musicbrainzngs library and writes results to CSV"""
    fieldnames = ["input_artist", "input_title",
                  "source", "resolved_artist", "resolved_title"]

    print(
        f"Processing queries tracks using 'musicbrainzngs' -> Saving to '{output_filename}'...\n")

    with open(output_filename, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for idx, query in enumerate(queries):

            artist = query.get("artist")
            title = query.get("title")

            print(
                f"[{idx+1}/??] Querying -> Artist: '{artist}' | Title: '{title}'")

            # Query via library function
            resolved = search_musicbrainz(artist, title)

            # Write row to CSV
            writer.writerow({
                "input_artist": artist,
                "input_title": title,
                "source": resolved["source"],
                "resolved_artist": resolved["artist"],
                "resolved_title": resolved["title"]
            })

    print(f"\nDone! Results successfully saved to {output_filename}")


def preprocess_json_to_dict(input_file):
    THRESHOLD_CONFIDENCE = 0.6

    dict = json.load(open(input_file, 'r'))
    queries = []
    seen = set()
    for entry in dict:
        if entry['title_type']['choice'] == 'track' and entry['title_type']['confidence'] >= THRESHOLD_CONFIDENCE:
            if tup := (entry['primary_artist']['choice'], entry['title']['choice']) in seen:
                continue
            seen.add(tup)
            value = {}
            if (artist := entry['primary_artist']['choice']) != 'none':
                value['artist'] = artist
            if (title := entry['title']['choice']) != 'none':
                value['title'] = title
            yield value


def preprocess_queries_from_parsed_artists(input_file):
    parsed_artists = csv.DictReader(open(input_file, 'r', encoding='utf-8'))
    seen = set()
    for entry in parsed_artists:
        if tup := (entry['artist'], entry['title']) in seen:
            print(f"Already seen {tup}")
            continue
        seen.add(tup)
        yield {
            "artist": entry['artist'],
            "title": entry['title']
        }


# --- TEST BATCH DATA ---
test_queries = [
    {"artist": "Coldplay", "title": "Higher Power"},
    {"artist": "Taylor Swift", "title": "Blank Space"},
    {"artist": "Swedish House Mafia", "title": "Don"}
]
# all_queries = json.load(open(sys.argv[1], 'r'))
# deduped_queries = set((d['artist'], d['title']) for d in all_queries)
# test_queries = [{'artist': d[0], 'title': d[1]} for d in deduped_queries]
# print(len(all_queries), len(test_queries))

# Run the pipeline and save to file
# process_queries_to_csv(test_queries)

# this is for jev pipeline
# process_queries_to_csv(preprocess_json_to_dict(
# sys.argv[1]), output_filename=sys.argv[2])

# this is for regex parsing
process_queries_to_csv(preprocess_queries_from_parsed_artists(
    sys.argv[1]), output_filename=sys.argv[2])
