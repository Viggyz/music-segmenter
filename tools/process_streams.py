import asyncio
import auditok
import logging
from pathlib import Path

SEGMENTER_PREFIX = "[SEGMENTER]:"

if __name__ == "__main__":
    file_name = "../../radio_intercept/2026-09-06_10-21-51_radio_sample.mp3"
    stream_id = "92.7"
    events = auditok.split(file_name,
                           sr=44100,
                           ch=2,
                           validator="otsu",
                           max_dur=40,
                           min_dur=0.2,
                           max_silence=1,
                           max_leading_silence=0.75,   # prepend up to 200ms before each event
                           max_trailing_silence=0.25,  # keep up to 150ms of silence after each event
                           )
    events = list(events)

    logging.info("%s Split %s into %d segments",
                 SEGMENTER_PREFIX, stream_id, len(events))
    segment_sub_folder = Path("segments") / f"radio_{stream_id}"
    files = [f for f in segment_sub_folder.glob("*") if f.is_file()]
    latest_file = None
    if files:
        latest_file = max(files, key=lambda f: f.stat().st_mtime)

    for i, r in enumerate(events):
        # AudioRegions returned by `split` have defined 'start' and 'end' attributes
        print(f"Event {i}: {r.start:.3f}s -- {r.end:.3f}s")
        # logging.info("%s Event {i}: {r.start:.3f}s -- {r.end:.3f}s", SEGMENTER_PREFIX, stream_id, file_name)

        # # Play the audio event
        # r.play(progress_bar=True)
        r1 = r
        if i == 0 and latest_file:
            prev_segment = auditok.load(latest_file)
            r1 = prev_segment + r

        # Save the event with start and end times in the filename
        file_to_save = f"../segments/radio_{stream_id}/segment_{r.start:.3f}-{r.end:.3f}.wav"
        filename = r1.save(file_to_save)
        logging.info("%s Writing %s segment to file %s",
                     SEGMENTER_PREFIX, stream_id, "abc")
        # print(f"Event saved as: {filename}")F
    logging.info("%s Written %d segments for file %s",
                 SEGMENTER_PREFIX, len(events), file_name)
