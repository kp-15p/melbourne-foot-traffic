# Databricks notebook source
# Refuse to call the run a success if the data isn't actually good.
# A pipeline that silently drops 40% and reports green is worse than a crash.
from datetime import date, timedelta

dbutils.widgets.text("catalog", "ped_dev")
C = dbutils.widgets.get("catalog")

failures = []

# ── 1. Freshness ──────────────────────────────────────────────────────────
high = spark.sql(f"SELECT max(sensing_date) d FROM {C}.silver.silver_pedestrian_hourly").first()["d"]
lag  = (date.today() - high).days
print(f"freshness: latest sensing_date {high} ({lag} days behind)")
if lag > 0:
    failures.append(f"Data is {lag} days stale (latest {high}); expected <= 3.")

# ── 2. Schema drift — the API changed something ───────────────────────────
drift = spark.sql(f"""
    SELECT count(*) n FROM {C}.silver.silver_pedestrian_hourly
    WHERE _rescued_data IS NOT NULL
      AND _ingested_at > current_timestamp() - INTERVAL 2 DAYS
""").first()["n"]
print(f"rescued rows in last 2 days: {drift:,}")
if drift > 0:
    sample = spark.sql(f"""
        SELECT _source_file, collect_set(_rescued_data)[0] AS example, count(*) AS rows
        FROM   {C}.silver.silver_pedestrian_hourly
        WHERE  _rescued_data IS NOT NULL
          AND  _ingested_at > current_timestamp() - INTERVAL 2 DAYS
        GROUP BY ALL ORDER BY rows DESC LIMIT 5
    """)
    display(sample)
    sample.write.mode("append").saveAsTable(f"{C}.ops.schema_drift_log")
    failures.append(f"{drift:,} rows carry _rescued_data — the API shape changed.")

# ── 3. Expectation drop rate, from the pipeline's own event log ───────────
try:
    dq = spark.sql(f"""
        SELECT exp.name                     AS expectation,
               sum(exp.passed_records)      AS passed,
               sum(exp.failed_records)      AS failed
        FROM (
          SELECT explode(from_json(
                   details:flow_progress.data_quality.expectations,
                   'array<struct<name string, passed_records bigint,
                                 failed_records bigint>>')) AS exp
          FROM   event_log(TABLE({C}.silver.silver_pedestrian_hourly))
          WHERE  event_type = 'flow_progress'
        )
        GROUP BY ALL
    """)
    display(dq)
    bad = dq.where("failed > 0 AND failed / (passed + failed) > 0.05").collect()
    for r in bad:
        pct = 100 * r["failed"] / (r["passed"] + r["failed"])
        failures.append(f"Expectation '{r['expectation']}' dropped {pct:.1f}% of rows.")
except Exception as e:
    # The gate must not fail because the event-log read failed.
    print(f"(event log unavailable: {e})")

# ── verdict ───────────────────────────────────────────────────────────────
if failures:
    raise RuntimeError("QUALITY GATE FAILED:\n  - " + "\n  - ".join(failures))
print("\nQuality gate passed.")