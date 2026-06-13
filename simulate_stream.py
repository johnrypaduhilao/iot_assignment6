# turns the csv  into a folder of files that spark watches like a live stream.
# all the events go in one file, plus a small "flush" file dated a day later. that future
# batch just nudges spark's clock so it knows the older visits are over and prints them.
import csv, os, shutil
from datetime import datetime, timedelta

SOURCE = "sample.csv"
OUT = "stream_input"

rows = list(csv.reader(open(SOURCE, encoding="utf-16")))
header, data = rows[0], rows[1:]
data.sort(key=lambda r: r[0])          # put events back in time order

if os.path.exists(OUT):
    shutil.rmtree(OUT)
os.makedirs(OUT)

with open(os.path.join(OUT, "01_events.csv"), "w", newline="") as f:
    w = csv.writer(f); w.writerow(header); w.writerows(data)

# a few throwaway rows one day after the last real event. they carry no carts or buys,
# they only move time forward so spark releases the finished sessions.
last = data[-1][0].replace(" UTC", "")
future = datetime.strptime(last, "%Y-%m-%d %H:%M:%S") + timedelta(days=1)
ft = future.strftime("%Y-%m-%d %H:%M:%S") + " UTC"
flush = [[ft, "view", "0", "0", "", "", "0.0", f"flush_{i}", "flush"] for i in range(3)]
with open(os.path.join(OUT, "02_flush.csv"), "w", newline="") as f:
    w = csv.writer(f); w.writerow(header); w.writerows(flush)

print(f"loaded {len(data)} events into {OUT}/")
