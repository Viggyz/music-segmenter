import numpy as np
import librosa
import ruptures as rpt


def find_crossfade_via_ruptures(mp3_path, penalty=10, min_segment_sec=3):
    print(f"Loading {mp3_path}...")
    hop_length = 1024
    # Load audio (downsample to 22050Hz for standard audio feature mapping)
    # y, sr = librosa.load(mp3_path, sr=22050)
    y, sr = librosa.load(mp3_path)

    print("Extracting multivariate musical features...")
    # 1. Extract Chroma Features (captures harmonic/chord distributions)
    chroma = librosa.feature.chroma_stft(y=y, sr=sr, hop_length=hop_length)

    # 2. Extract MFCCs (captures timbral texture)
    mfcc = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=13, hop_length=hop_length)

    # Combine features into a single matrix and transpose it for ruptures
    # Shape: (Number of Frames, Number of Features)
    features = np.vstack([chroma, mfcc]).T

    print("Running Change-Point Detection (Pelt Algorithm)...")
    # We use a Linear Cost Function (cost_rbf or cost_linear)
    # to evaluate changes in the mean/variance of the combined feature stream
    min_frames = int((min_segment_sec * sr) / hop_length)
    algo = rpt.KernelCPD(kernel="cosine", min_size=min_frames).fit(features)

    # The 'pen' (penalty) parameter controls the sensitivity.
    # Lower penalty = finds more splits; Higher penalty = finds only major song changes.
    result_frames = algo.predict(pen=penalty)

    # Convert the resulting frame indices back into exact seconds
    detected_timestamps = librosa.frames_to_time(
        result_frames[:-1], sr=sr, hop_length=hop_length)

    print("/n--- Structural Change Points Found ---")
    for idx, timestamp in enumerate(detected_timestamps):
        print(f"Transition {idx + 1}: {timestamp:.2f} seconds")

    return detected_timestamps


# --- Example Usage ---
# splits = find_crossfade_via_ruptures("D:/Intrest/radio_intercept/multiple_breakpoints.mp3", penalty=25)
# Mj detecs only a single
# splits = find_crossfade_via_ruptures("D:/Intrest/radio_intercept/2026-09-06_10-23-55_radio_sample.mp3", penalty=25)
# Good 1/4 splits are correct.
# splits = find_crossfade_via_ruptures("D:/Intrest/radio_intercept/2026-09-06_10-30-33_radio_sample.mp3", penalty=25)
# Catches multiple transitions at 6s mark, 28s mark, 52s mark, 61s mark
# splits = find_crossfade_via_ruptures("D:/Intrest/radio_intercept/2026-09-05_23-30-43_radio_sample.mp3", penalty=25)
# boundaries at 23s and 95s caught correctly
splits = find_crossfade_via_ruptures("D:/Intrest/radio_intercept/2026-09-05_23-27-39_radio_sample.mp3", penalty=25)
# Finds too many boundaries which aren't song end boundaries.
# splits = find_crossfade_via_ruptures(
#     "D:/Intrest/radio_intercept/2026-09-05_23-24-00_radio_sample.mp3", penalty=25)