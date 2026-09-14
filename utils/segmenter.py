from pathlib import Path
import logging

SEGMENTER_PREFIX = "[SEGMENTER]:"

class Segmenter:
    def __init__(self, data_folder, segment_folder, queue):
        self._queue = queue
        self.data_folder = Path(data_folder)
        self.segment_folder = segment_folder

    def _segment_file(self, input_file, stream_id, output_folder):
        raise AttributeError("File process is not implemented")

    def segment_and_write(self):
        while True:
            stream_id, file_name = self._queue.get()
            logging.info("%s Segmenting for %s:%s",
                        SEGMENTER_PREFIX, stream_id, file_name)
        
            try:
                self._segment_file(file_name, stream_id, f"{self.segment_folder}/radio_{stream_id}/")
            except Exception as e:
                logging.error("❌ [Consumer %s] Error processing %s: %s", stream_id, file_name, e)
            # we no longer need to call task_done to say that queue is proccesed.
            finally:
                if hasattr(self._queue, "task_done") and callable(getattr(self._queue, "task_done")):
                    self._queue.task_done()

    @classmethod
    def create_and_return_segmenter(cls, data_folder, segment_folder, queue):
        return cls(data_folder, segment_folder, queue).segment_and_write()