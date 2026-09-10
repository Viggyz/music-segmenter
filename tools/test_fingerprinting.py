import acoustid
import os
from dotenv import load_dotenv

load_dotenv()

os.environ['FPCALC'] =  "D:/applications/chromaprint-fpcalc-1.6.1-windows-x86_64/fpcalc.exe"
file_path = "D:/Intrest/radio_intercept/2026-09-06_10-23-55_radio_sample.mp3"

# duration, fingerprint = acoustid.fingerprint_file(file_path)
# print(duration, fingerprint)
# print(acoustid.lookup("HSXoL2fzSt", fingerprint, 10))  

for score, recording_id, title, artist in acoustid.match("HSXoL2fzSt", file_path):
    print(score, recording_id, title, artist)