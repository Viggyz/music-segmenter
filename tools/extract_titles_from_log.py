import re
import sys, os, csv

pattern = r"\[NOW PLAYING\]\[(?P<station_name>[^\]]+)\]\s*(?P<stream_title>.+)$"

if __name__ == "__main__":
    *log_filenames, outfilename = sys.argv[1:]
    with open(outfilename, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=["station_name", "stream_title"])
        writer.writeheader()
        for filename in log_filenames:
            f = open(filename, 'r')
            for line in f:
                match = re.search(pattern, line)
                if match:
                    writer.writerow({
                        "station_name": match.group("station_name"),
                        "stream_title": match.group("stream_title")
                    })