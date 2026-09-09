import auditok
from pathlib import Path
import logging

SEGMENTER_PREFIX = "[SEGMENTER]:"


class Segmenter:
    def __init__(self, data_folder, segment_folder, queue):
        self._queue = queue
        self.data_folder = Path(data_folder)
        self.segment_folder = segment_folder

    async def segment_and_write(self):
        while True:
            stream_id, file_name = await self._queue.get()
            logging.info("%s Segmenting for %s:%s",
                        SEGMENTER_PREFIX, stream_id, file_name)
            
            try:
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

                for i, r in enumerate(events):

                    # Save the event with start and end times in the filename
                    file_to_save = f"{self.segment_folder}/radio_{stream_id}/{str(file_name).split('\\')[-1]}_{i}.wav"
                    r.save(file_to_save)
                    # logging.info("%s Writing %s segment to file %s",
                    #              SEGMENTER_PREFIX, stream_id, filename)
                    # print(f"Event saved as: {filename}")F
                logging.info("%s Written %d segments for file %s",
                            SEGMENTER_PREFIX, len(events), file_name)
            except Exception as e:
                logging.error("❌ [Consumer %s] Error processing %s: %s", stream_id, file_name, e)
            finally:
                self._queue.task_done()

    @classmethod
    def create_and_return_segmenter(cls, data_folder, segment_folder, queue):
        return cls(data_folder, segment_folder, queue).segment_and_write()
