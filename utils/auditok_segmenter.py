import auditok
import logging

SEGMENTER_PREFIX = "[SEGMENTER]:"

from .segmenter import Segmenter


class AuditokSegmenter(Segmenter):
    def _segment_file(self, input_file, stream_id, output_folder):
        events = auditok.split(input_file,
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
        
        for i, r in enumerate(events):
        
            # Save the event with start and end times in the filename
            file_to_save = f"{output_folder}/{str(input_file).split('\\')[-1]}_{i}.wav"
            r.save(file_to_save)
            # logging.info("%s Writing %s segment to file %s",
            #              SEGMENTER_PREFIX, stream_id, filename)
            # print(f"Event saved as: {filename}")F
        logging.info("%s Written %d segments for file %s",
                    SEGMENTER_PREFIX, len(events), input_file)