import os
import logging
import glob

import librosa
import numpy as np
import ruptures as rpt  # our packageimport matplotlib.pyplot as plt
from scipy.stats import zscore
from ruptures.base import BaseCost
from scipy.ndimage import gaussian_filter1d
from scipy.signal import find_peaks
import soundfile as sf

from .segmenter import Segmenter


SEGMENTER_PREFIX = "[SEGMENTER]:"

class EnsembleSegmenter(Segmenter):

    def _get_latest_segment(self, folder_path, extension="*.wav"):
        """Finds the most recently modified audio file in folder_path."""
        files = glob.glob(os.path.join(folder_path, extension))
        if not files:
            return None
        return max(files, key=os.path.getmtime)

    def _drop_in_segmenter(self, y, sr, hop_length, width_frames):
        """ Detects fade out transitions """
        S = librosa.feature.melspectrogram(
            y=y, sr=sr, n_mels=13, hop_length=hop_length
        )
        S_dB = librosa.power_to_db(S, ref=np.max)
        features = zscore(S_dB, axis=1).T

        algo = rpt.Window(model="rbf", width=width_frames, jump=2).fit(features)
        return algo, zscore(S_dB, axis=1)
        # Can predict here to get teh result.
    
    def _sweeper_segmenter(self, y, sr, hop_length, width_frames):
        """ Detects sudden jump in freqency, a 'sweeper' transition """
        y_harmonic, y_percussive = librosa.effects.hpss(y)
        rms_harmonic = librosa.feature.rms(y=y_harmonic, hop_length=hop_length)[0]
        rms_percussive = librosa.feature.rms(y=y_percussive, hop_length=hop_length)[0]
        spec_centroid = librosa.feature.spectral_centroid(y=y, sr=sr, hop_length=hop_length)[0]

        # Normalize and calculate score
        rms_h_norm = rms_harmonic / (np.max(rms_harmonic) + 1e-6)
        rms_p_norm = rms_percussive / (np.max(rms_percussive) + 1e-6)
        spec_c_norm = spec_centroid / (np.max(spec_centroid) + 1e-6)

        sweeper_score = gaussian_filter1d((rms_p_norm * spec_c_norm) / (rms_h_norm + 0.05), sigma=3)
        signal = sweeper_score.reshape(-1, 1)
        algo = rpt.Window(model="l1", width=width_frames, jump=2).fit(signal)
        return algo, sweeper_score

    def _segment_file(self, input_file, stream_id, output_folder):
        os.makedirs(output_folder, exist_ok=True)
        
        # 1. Locate the latest existing segment in the target folder BEFORE saving new files
        latest_file = self._get_latest_segment(output_folder)
        if latest_file:
            logging.info(
                f"{SEGMENTER_PREFIX} Found previous latest segment to bridge: {latest_file}")
        else:
            logging.info(
                f"{SEGMENTER_PREFIX} FNo previous segments found in output directory.")

        logging.info(f"{SEGMENTER_PREFIX} Loading {input_file}...")

        sr = 22050
        hop_length = 2048
        y, sr = librosa.load(input_file, sr=sr)
        window_sec = 3.0
        width_frames = int((window_sec * sr) / hop_length)
        algo_1, features_1 = self._drop_in_segmenter(y, sr, hop_length, width_frames)
        algo_2, features_2 = self._sweeper_segmenter(y, sr, hop_length, width_frames)

        # onset detection
        oenv = librosa.onset.onset_strength(
            y=y, sr=sr, hop_length=hop_length
        )

        score_arr = np.c_[algo_1.score, algo_2.score]
        stacked_features = np.vstack([features_1, features_2])
        algo_expert_intersection = rpt.Window(width=width_frames, jump=2).fit(stacked_features.T)
        algo_expert_intersection.score = score_arr.min(axis=1)

        # Get breakpoints
        bkps_intersection_predicted = algo_expert_intersection.predict(pen=440)[:-1]
        boundary_samples = librosa.frames_to_samples(bkps_intersection_predicted, hop_length=hop_length)

        all_bounds = [0] + list(boundary_samples) + [len(y)]

        # Write samples to file.
        logging.info("%s Split %s into %d segments",
                     SEGMENTER_PREFIX, stream_id, len(all_bounds))
        # print("\n--- Exporting Audio Segments ---")
        for idx in range(len(all_bounds) - 1):
            start_sample = all_bounds[idx]
            end_sample = all_bounds[idx + 1]
        
            segment_audio = y[start_sample:end_sample]
        
            # Concatenate previous tail audio with the first segment of the new partition
            if idx == 0 and latest_file:
                logging.info(
                    f"Stitching {os.path.basename(latest_file)} + Segment 01...")
                prev_audio, _ = librosa.load(latest_file, sr=sr)
                segment_audio = np.concatenate([prev_audio, segment_audio])
        
            output_path = os.path.join(
                output_folder, f"{str(input_file).split('\\')[-1]}_{idx + 1:02d}.wav")
            sf.write(output_path, segment_audio, sr)
        
            # Remove the old unstitched file from the disk
            if idx == 0 and latest_file:
                os.remove(latest_file)
                logging.info(
                    f"{SEGMENTER_PREFIX} Removed old bridged segment: {latest_file}")
        
        logging.info("%s Written %d segments for file %s",
                     SEGMENTER_PREFIX, len(all_bounds), input_file)
        
         



        