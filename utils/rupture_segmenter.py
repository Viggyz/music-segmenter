import logging
import os
import glob

import numpy as np
import librosa
import ruptures as rpt
import soundfile as sf

from .segmenter import Segmenter

SEGMENTER_PREFIX = "[SEGMENTER]:"

class RuptureSegmenter(Segmenter):
    def _get_latest_segment(self, folder_path, extension="*.wav"):
        """Finds the most recently modified audio file in folder_path."""
        files = glob.glob(os.path.join(folder_path, extension))
        if not files:
            return None
        return max(files, key=os.path.getmtime)

    def _segment_file(self, input_file, stream_id, output_folder):
        os.makedirs(output_folder, exist_ok=True)

        # 1. Locate the latest existing segment in the target folder BEFORE saving new files
        latest_file = self._get_latest_segment(output_folder)
        if latest_file:
            logging.info(f"{SEGMENTER_PREFIX} Found previous latest segment to bridge: {latest_file}")
        else:
            logging.info(f"{SEGMENTER_PREFIX} FNo previous segments found in output directory.")

        logging.info(f"{SEGMENTER_PREFIX} Loading {input_file}...")
        hop_length = 512
        min_segment_sec=3
        penalty=25
        # Load audio (downsample to 22050Hz for standard audio feature mapping)
        y, sr = librosa.load(input_file, sr=22050)
        
        # 1. Extract Chroma Features (captures harmonic/chord distributions)
        chroma = librosa.feature.chroma_stft(y=y, sr=sr, hop_length=hop_length)
        
        # 2. Extract MFCCs (captures timbral texture)
        mfcc = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=13, hop_length=hop_length)
        
        # Combine features into a single matrix and transpose it for ruptures
        # Shape: (Number of Frames, Number of Features)
        features = np.vstack([chroma, mfcc]).T
        
        # We use a Cosine Cost Function
        # to evaluate changes in the mean/variance of the combined feature stream
        # min_frames = int((min_segment_sec * sr) / hop_length)
        # algo = rpt.KernelCPD(kernel="cosine", min_size=min_frames).fit(features)
        algo = rpt.KernelCPD(kernel="cosine").fit(features)
        
        # The 'pen' (penalty) parameter controls the sensitivity.
        # Lower penalty = finds more splits; Higher penalty = finds only major song changes.
        result_frames = algo.predict(pen=penalty)
        
        # Convert the resulting frames directly to audio sample indices
        boundary_samples = librosa.frames_to_samples(result_frames[:-1], hop_length=hop_length)
        all_bounds = [0] + list(boundary_samples) + [len(y)]

        logging.info("%s Split %s into %d segments",
                            SEGMENTER_PREFIX, stream_id, len(all_bounds))
        # print("\n--- Exporting Audio Segments ---")
        for idx in range(len(all_bounds) - 1):
            start_sample = all_bounds[idx]
            end_sample = all_bounds[idx + 1]

            segment_audio = y[start_sample:end_sample]

            # Concatenate previous tail audio with the first segment of the new partition
            if idx == 0 and latest_file:
                print(f"Stitching {os.path.basename(latest_file)} + Segment 01...")
                prev_audio, _ = librosa.load(latest_file, sr=sr)
                segment_audio = np.concatenate([prev_audio, segment_audio])

            output_path = os.path.join(
                output_folder, f"{str(input_file).split('\\')[-1]}_{idx + 1:02d}.wav")
            sf.write(output_path, segment_audio, sr)

            # Remove the old unstitched file from the disk
            if idx == 0 and latest_file:
                os.remove(latest_file)
                logging.info(f"{SEGMENTER_PREFIX} Removed old bridged segment: {latest_file}")

            # print(f"Segment {idx + 1:02d}: {start_time:.2f}s - {end_time:.2f}s | Saved to {output_path}")
        logging.info("%s Written %d segments for file %s",
                            SEGMENTER_PREFIX, len(all_bounds), input_file)
