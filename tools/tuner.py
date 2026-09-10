import numpy as np
import librosa
import scipy.signal
import matplotlib.pyplot as plt

def visual_tuning_tool(audio_path):
    print("Loading audio and extracting features...")
    y, sr = librosa.load(audio_path, sr=22050)
    
    # Extract the raw, noisy spectral flux
    raw_flux = librosa.onset.onset_strength(y=y, sr=sr)
    times = librosa.times_like(raw_flux, sr=sr)
    
    # --- TUNING PARAMETERS ---
    # Change these values below to fine-tune your detection accuracy!
    window_seconds = 2.0    # Larger = smoother line (washes away rapid drum beats)
    prominence_target = 0.3 # Higher = stricter threshold (ignores quieter shifts)
    min_gap_seconds = 3.0   # Enforces a dead-zone spacing between crossfades
    min_width_seconds = 1.5 # How wide/sustained the energy shift must be
    # -------------------------
    
    # Calculate frames per second for sample alignment
    fps = sr / 512
    window_frames = int(window_seconds * fps)
    if window_frames % 2 == 0: 
        window_frames += 1
        
    # Smooth the curve
    window = scipy.signal.windows.hann(window_frames)
    smoothed_flux = np.convolve(raw_flux, window / window.sum(), mode='same')
    
    # Run peak detection
    peaks, props = scipy.signal.find_peaks(
        smoothed_flux, 
        prominence=prominence_target,
        distance=int(min_gap_seconds * fps),
        width=int(min_width_seconds * fps)
    )
    
    # --- PLOTTING THE GRAPH ---
    plt.figure(figsize=(14, 6))
    
    # 1. Plot raw noisy spikes in light background color
    plt.plot(times, raw_flux, label='Raw Audio Spikes (Beats/Noise)', color='lightgray', alpha=0.7)
    
    # 2. Plot the smoothed structural curve
    plt.plot(times, smoothed_flux, label='Smoothed Structural Curve (Crossfade Energy)', color='blue', linewidth=2)
    
    # 3. Mark the detected crossfade points
    if len(peaks) > 0:
        plt.scatter(times[peaks], smoothed_flux[peaks], color='red', marker='X', s=150, zorder=5, 
                    label=f'Detected Crossfades ({len(peaks)} found)')
        
        # Draw vertical lines across the entire chart at split points
        for p in times[peaks]:
            plt.axvline(x=p, color='red', linestyle='--', alpha=0.6)
            
    # Chart Styling
    plt.title('Audio Structure Analysis for Crossfade Detection', fontsize=14, fontweight='bold')
    plt.xlabel('Time (Seconds)', fontsize=12)
    plt.ylabel('Acoustic Energy / Flux Variance', fontsize=12)
    plt.legend(loc='upper right', frameon=True, shadow=True)
    plt.grid(axis='x', linestyle=':', alpha=0.6)
    plt.tight_layout()
    
    # Display the interactive window
    plt.show()

# --- Run the tool ---
visual_tuning_tool("D:/Intrest/radio_intercept/2026-09-05_23-25-45_radio_sample.mp3")
