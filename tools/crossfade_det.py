import numpy as np
import librosa
import scipy.signal

def find_crossfade_cleaned(audio_path, window_seconds=2.0):
    print("Loading audio...")
    y, sr = librosa.load(audio_path, sr=22050)
    
    print("Calculating raw Spectral Flux...")
    # Get raw frame-by-frame onset strength
    onset_env = librosa.onset.onset_strength(y=y, sr=sr)
    times = librosa.times_like(onset_env, sr=sr)
    
    # 1. APPLY TEMPORAL SMOOTHING (Moving Average Filter)
    # Convert our target window in seconds to number of audio frames
    fps = sr / 512  # librosa default hop_length is 512
    window_frames = int(window_seconds * fps)
    if window_frames % 2 == 0: 
        window_frames += 1  # Window must be odd for the Hanning filter
        
    # Smooth the curve using a Hanning window to wash out fast transient drum beats
    window = scipy.signal.windows.hann(window_frames)
    smoothed_flux = np.convolve(onset_env, window / window.sum(), mode='same')
    
    # 2. ADAPTIVE THRESHOLD PEAK DETECTION
    # We look for peaks that are wide and significantly taller than the background noise
    # We set a high distance requirement so it can't pick two points within the same crossfade
    min_distance_frames = int(5.0 * fps) # Crossfades can't happen within 5 seconds of each other
    
    peaks, properties = scipy.signal.find_peaks(
        smoothed_flux, 
        prominence=0.5,                # Must stand out drastically above baseline
        distance=min_distance_frames,   # Minimum separation between distinct crossfades
        width=int(1.0 * fps)           # Must be a sustained energy shift (at least 1 second wide)
    )
    
    detected_times = times[peaks]
    return detected_times, smoothed_flux, times

# --- Example Usage ---
detected_splits, flux_curve, timeline = find_crossfade_cleaned("D:/Intrest/radio_intercept/2026-09-05_23-34-14_radio_sample.mp3")
for i, timestamp in enumerate(detected_splits):
    print(f"Possible Crossfade Split {i+1}: {timestamp:.2f} seconds")
