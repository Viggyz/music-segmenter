import asyncio
import logging
import os
import shutil
import sys
from multiprocessing import Process, Queue

from utils.rupture_segmenter import RuptureSegmenter

LOG_DIR = "logs"
NUM_WORKERS = 3

def setup_process_logging(process_identifier):
    os.makedirs(LOG_DIR, exist_ok=True)
    log_filename = os.path.join(LOG_DIR, f"{process_identifier}.log")
    
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    
    formatter = logging.Formatter(
        '%(asctime)s - [%(processName)s (PID:%(process)d)] - %(name)s - %(levelname)s - %(message)s'
    )
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    file_handler = logging.FileHandler(log_filename, mode='a')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logging.getLogger("numba").disabled = True

class AckQueueWrapper:
    """
    Wraps work_queue for RuptureSegmenter. Automatically sends an 
    ACK back to producer via ack_queue after each item is processed.
    """
    def __init__(self, work_queue, ack_queue):
        self.work_queue = work_queue
        self.ack_queue = ack_queue
        self.current_stream_id = None

    def get(self):
        # Send ACK for the previously processed item before fetching next
        if self.current_stream_id is not None:
            self.ack_queue.put(self.current_stream_id)
            self.current_stream_id = None

        item = self.work_queue.get()
        if item is not None:
            self.current_stream_id = item[0]  # Store stream_id for ACK
        return item

    def get_nowait(self):
        return self.get()

    def task_done(self):
        # Triggered if consumer calls queue.task_done()
        if self.current_stream_id is not None:
            self.ack_queue.put(self.current_stream_id)
            self.current_stream_id = None

def file_producer_worker(work_queue, ack_queue, data_folder):
    setup_process_logging("file_producer")
    logging.info("File producer initialized with ACK synchronization.")

    if not os.path.exists(data_folder):
        logging.error(f"Directory '{data_folder}' does not exist.")
        for _ in range(NUM_WORKERS):
            work_queue.put(None)
        return

    # 1. Discover and group all files per stream_id
    stream_files = {}
    for entry in sorted(os.listdir(data_folder)):
        radio_dir = os.path.join(data_folder, entry)
        if os.path.isdir(radio_dir) and entry.startswith("radio_"):
            stream_id = entry.replace("radio_", "", 1)
            files = sorted([
                os.path.join(radio_dir, f) 
                for f in os.listdir(radio_dir) 
                if not f.startswith('.')
            ])
            if files:
                stream_files[stream_id] = files

    in_flight_streams = set()

    # 2. Producer loop with backpressure per stream_id
    while stream_files or in_flight_streams:
        # Enqueue available files for streams that are NOT currently in-flight
        available_streams = [s for s in list(stream_files.keys()) if s not in in_flight_streams]

        for stream_id in available_streams:
            next_file = stream_files[stream_id].pop(0)
            if not stream_files[stream_id]:
                del stream_files[stream_id]  # All files queued for this stream

            in_flight_streams.add(stream_id)
            logging.debug(f"Dispatching ({stream_id}, {next_file})")
            work_queue.put((stream_id, next_file))

        # Block and wait for an ACK signal from any consumer before releasing next chunk
        if in_flight_streams:
            completed_stream_id = ack_queue.get()
            in_flight_streams.remove(completed_stream_id)
            logging.debug(f"ACK received for stream_id '{completed_stream_id}'")

    # Send poison pills to shut down consumers
    for _ in range(NUM_WORKERS):
        work_queue.put(None)

    logging.info("Completed queueing all streams with verified ACK ordering.")

def consumer_worker(work_queue, ack_queue, data_folder, segment_folder, worker_id):
    setup_process_logging(f"consumer_{worker_id}")
    logging.info(f"Consumer worker {worker_id} initialized")
    
    wrapped_queue = AckQueueWrapper(work_queue, ack_queue)

    RuptureSegmenter.create_and_return_segmenter(data_folder, segment_folder, wrapped_queue)
    # Flush final ACK upon exit
    if wrapped_queue.current_stream_id is not None:
        ack_queue.put(wrapped_queue.current_stream_id)

if __name__ == "__main__":
    setup_process_logging("replay_main")
    logging.info("Starting local stream replay orchestration with Handshake")

    DATA_FOLDER = "streams"
    SEGMENT_FOLDER = "segments"

    if os.path.exists(SEGMENT_FOLDER):
        shutil.rmtree(SEGMENT_FOLDER)
    os.makedirs(SEGMENT_FOLDER, exist_ok=True)

    if os.path.exists(DATA_FOLDER):
        for entry in os.listdir(DATA_FOLDER):
            if entry.startswith("radio_") and os.path.isdir(os.path.join(DATA_FOLDER, entry)):
                os.makedirs(os.path.join(SEGMENT_FOLDER, entry), exist_ok=True)

    work_queue = Queue(maxsize=20)
    ack_queue = Queue()

    # 1. Producer
    producer_process = Process(
        target=file_producer_worker, 
        name="FileProducerProcess", 
        args=(work_queue, ack_queue, DATA_FOLDER)
    )
    producer_process.start()

    # 2. Consumers
    consumer_processes = []
    for i in range(1, NUM_WORKERS + 1):
        p = Process(
            target=consumer_worker, 
            name=f"ConsumerProcess-{i}", 
            args=(work_queue, ack_queue, DATA_FOLDER, SEGMENT_FOLDER, i)
        )
        consumer_processes.append(p)
        p.start()

    producer_process.join()
    for p in consumer_processes:
        p.join()

    logging.warning("All replay tasks finished successfully with strict per-stream order")