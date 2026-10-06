# Databricks notebook source
# Daily increment: fetch everything since Silver's high-water mark, land it, verify.
import json, time, random, requests
from datetime import date, timedelta

dbutils.widgets.text("catalog", "ped_dev")
CATALOG = dbutils.widgets.get("catalog")
VOL     = f"/Volumes/{CATALOG}/landing/raw"
BASE    = ("https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/"
           "pedestrian-counting-system-monthly-counts-per-hour")
SENSORS = ("https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/"
           "pedestrian-counting-system-sensor-locations")


def fetch(url, params, max_attempts=6):
    for attempt in range(1, max_attempts + 1):
        r = requests.get(url, params=params, timeout=300)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429 or r.status_code >= 500:
            ra = r.headers.get("Retry-After")
            wait = float(ra) if ra and ra.isdigit() else min(2**attempt, 60) + random.random()
            print(f"  HTTP {r.status_code} → sleeping {wait:.1f}s")
            time.sleep(wait)
            continue
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
    raise RuntimeError("retries exhausted")


def write_ndjson(rows, path):
    """Write to .tmp then rename — Auto Loader must never see a partial file."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        for rec in rows:
            fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
    dbutils.fs.mv(f"file:{tmp}" if tmp.startswith("/dbfs") else tmp, path)
    return len(rows)


# ── watermark, straight from the lakehouse ────────────────────────────────
# Bootstrap case: on a fresh deployment Silver doesn't exist yet, so there's
# no watermark to read. Fall back to the start of the source's rolling window.
try:
    high = spark.sql(
        f"SELECT max(sensing_date) AS d FROM {CATALOG}.silver.silver_pedestrian_hourly"
    ).first()["d"]
except Exception as e:
    print(f"Silver not readable ({type(e).__name__}) — treating as first run.")
    high = None

# Re-fetch a 3-day overlap: the publisher backfills late corrections, and an
# exact-watermark resume would miss them permanently. Silver's dedup makes
# the overlap free.
start = (high - timedelta(days=3)) if high else date(2024, 10, 1)
today = date.today()
print(f"watermark {high} → fetching from {start}")

landed_files = 0

# ── counts, one file per affected month ───────────────────────────────────
cur = start.replace(day=1)
while cur <= today:
    nxt   = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
    month = f"{cur:%Y-%m}"
    where = (f"sensing_date >= date'{max(cur, start):%Y-%m-%d}' "
             f"AND sensing_date < date'{nxt:%Y-%m-%d}'")
    rows = fetch(f"{BASE}/exports/json", {"where": where, "order_by": "sensing_date"})
    if rows:
        d = f"{VOL}/pedestrian_hourly/ingest_month={month}"
        dbutils.fs.mkdirs(d)
        # Distinct filename per run so a re-landed month is a NEW file to
        # Auto Loader, not an ambiguous overwrite of one it already read.
        out = f"{d}/counts_{month}_{today:%Y%m%d}.jsonl"
        with open(out, "w", encoding="utf-8") as fh:
            for rec in rows:
                fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
        print(f"{month}  {len(rows):,} rows")
        landed_files += 1
    cur = nxt

# ── sensor snapshot: full copy daily, feeds SCD2 ──────────────────────────
sensors = fetch(f"{SENSORS}/exports/json", {})
stamped = [r | {"_snapshot_date": today.isoformat()} for r in sensors]
d = f"{VOL}/sensor_locations/ingest_date={today:%Y-%m-%d}"
dbutils.fs.mkdirs(d)
with open(f"{d}/sensors.jsonl", "w", encoding="utf-8") as fh:
    for rec in stamped:
        fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
print(f"sensors  {len(stamped)}")
landed_files += 1

# ── the assertion that makes "nothing arrived" a FAILURE, not a green run ──
if landed_files == 0:
    raise RuntimeError("No files landed. The API returned nothing for the "
                       f"window starting {start}. Failing rather than letting "
                       "the pipeline report success on zero new data.")

print(f"\nOK — {landed_files} files landed")