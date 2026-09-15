import glob
import logging
import os
from pathlib import Path

import librosa
import numpy as np
import scipy.ndimage
import scipy.sparse.csgraph
import scipy.sparse.linalg
import soundfile as sf

from .segmenter import SEGMENTER_PREFIX, Segmenter


class LaplacianSegmenter(Segmenter):
    def _get_latest_segment(self, folder_path, extension="*.wav"):
        """Finds the most recently modified audio file in folder_path."""
        files = glob.glob(os.path.join(folder_path, extension))
        if not files:
            return None
        return max(files, key=os.path.getmtime)

    def _compute_laplacian_peaks(self, y, sr, hop_length=2048, k=5, min_segment_sec=30):
        """Detects structural boundaries using Laplacian spectral decomposition."""
        # 1. Feature Extraction (Chroma + MFCC)
        chroma = librosa.feature.chroma_stft(y=y, sr=sr, hop_length=hop_length)
        mfcc = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=13, hop_length=hop_length)
        features = np.vstack([chroma, mfcc])

        # 2. Build Recurrence Affinity Matrix
        R = librosa.segment.recurrence_matrix(
            features, mode="affinity", metric="cosine", sparse=True
        )

        # 3. Compute Normalized Graph Laplacian
        L = scipy.sparse.csgraph.laplacian(R, normed=True)

        # 4. Extract Smallest Eigenvectors
        _, evecs = scipy.sparse.linalg.eigsh(L, k=k, which="SM")

        # 5. Smooth Eigenvectors across time (~15s window)
        filter_size = int((3 * sr) / hop_length)
        if filter_size % 2 == 0:
            filter_size += 1
        smoothed_evecs = scipy.ndimage.median_filter(evecs, size=(filter_size, 1))

        # 6. Compute Boundary Estimation Curve
        diffs = np.sqrt(np.sum(np.diff(smoothed_evecs, axis=0) ** 2, axis=1))
        diffs = np.pad(diffs, (1, 0), mode="constant")

        # 7. Peak Picking with Minimum Frame Constraint
        min_frames = int((min_segment_sec * sr) / hop_length)
        half_frames = max(1, min_frames // 2)

        peaks = librosa.util.peak_pick(
            diffs,
            pre_max=half_frames,
            post_max=half_frames,
            pre_avg=half_frames,
            post_avg=half_frames,
            delta=0.01,
            wait=max(1, min_frames),
        )

        return peaks

    def _segment_file(self, input_file, stream_id, output_folder):
        os.makedirs(output_folder, exist_ok=True)

        # 1. Locate the latest existing segment in target folder
        latest_file = self._get_latest_segment(output_folder)
        if latest_file:
            logging.info(
                f"{SEGMENTER_PREFIX} Found previous latest segment to bridge: {latest_file}"
            )
        else:
            logging.info(
                f"{SEGMENTER_PREFIX} No previous segments found in output directory."
            )

        logging.info(f"{SEGMENTER_PREFIX} Loading {input_file}...")
        hop_length = 2048
        min_segment_sec = 30
        k = 5

        # Load audio downsampled to 22050Hz for standard audio feature mapping
        y, sr = librosa.load(input_file, sr=22050)

        # Compute peak frame indices
        peak_frames = self._compute_laplacian_peaks(
            y, sr, hop_length=hop_length, k=k, min_segment_sec=min_segment_sec
        )

        # Convert peak frames directly to audio sample indices
        boundary_samples = librosa.frames_to_samples(peak_frames, hop_length=hop_length)
        all_bounds = [0] + list(boundary_samples) + [len(y)]

        total_segments = len(all_bounds) - 1
        logging.info(
            "%s Split %s into %d segments using Laplacian method",
            SEGMENTER_PREFIX,
            stream_id,
            total_segments,
        )

        filename_stem = Path(input_file).name

        for idx in range(total_segments):
            start_sample = all_bounds[idx]
            end_sample = all_bounds[idx + 1]
            segment_audio = y[start_sample:end_sample]

            # Concatenate previous tail audio with the first segment of new partition
            if idx == 0 and latest_file:
                logging.info(
                    f"{SEGMENTER_PREFIX} Stitching {os.path.basename(latest_file)} + Segment 01..."
                )
                prev_audio, prev_sr = sf.read(latest_file)

                if prev_sr != sr:
                    prev_audio = librosa.resample(prev_audio, orig_sr=prev_sr, target_sr=sr)

                segment_audio = np.concatenate([prev_audio, segment_audio])

                # Safely delete the old bridged file
                try:
                    os.remove(latest_file)
                    logging.info(
                        f"{SEGMENTER_PREFIX} Removed old bridged segment: {latest_file}"
                    )
                except PermissionError:
                    import gc
                    gc.collect()
                    os.remove(latest_file)
                    logging.info(
                        f"{SEGMENTER_PREFIX} Removed old bridged segment: {latest_file}"
                    )

            output_path = os.path.join(
                output_folder, f"{filename_stem}_{idx + 1:02d}.wav"
            )
            sf.write(output_path, segment_audio, sr)

        logging.info(
            "%s Written %d segments for file %s",
            SEGMENTER_PREFIX,
            total_segments,
            input_file,
        )