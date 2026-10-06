from pyspark import pipelines as dp
from pyspark.sql import functions as F

MELB = "Australia/Melbourne"


# ── FACT ───────────────────────────────────────────────────────────────────
@dp.table(
    name="silver.silver_pedestrian_hourly",
    comment="Typed, deduplicated hourly counts with a derived timestamp.",
    table_properties={"quality": "silver"},
    cluster_by=["location_id", "sensing_date"],
)
# DROP — unusable rows
@dp.expect_or_drop("location_present",   "location_id IS NOT NULL")
@dp.expect_or_drop("date_present",       "sensing_date IS NOT NULL")
@dp.expect_or_drop("hour_in_range",      "hourday BETWEEN 0 AND 23")
@dp.expect_or_drop("count_not_negative", "pedestrian_count >= 0")
# WARN — keep the row, but count it. Dropping these destroys the evidence.
@dp.expect("no_api_drift", "_rescued_data IS NULL")
@dp.expect("directions_reconcile",
           "direction_1_count IS NULL OR direction_2_count IS NULL "
           "OR direction_1_count + direction_2_count = pedestrian_count")
# FAIL — can only mean my own derivation is broken
@dp.expect_or_fail("no_future_dates", "sensing_date <= current_date()")
def silver_pedestrian_hourly():
    return (
        spark.readStream.table("bronze_pedestrian_hourly")
        # The API gives date and hour separately — build the timestamp.
        .withColumn("sensing_ts_local", F.make_timestamp(
            F.year("sensing_date"), F.month("sensing_date"),
            F.dayofmonth("sensing_date"), F.col("hourday"),
            F.lit(0), F.lit(0)))
        # Local wall-clock is what analysts want; UTC is what joins correctly
        # across sources. Carry both, let the consumer choose.
        .withColumn("sensing_ts_utc", F.to_utc_timestamp("sensing_ts_local", MELB))
        .withColumn("day_of_week", F.date_format("sensing_date", "EEEE"))
        .withColumn("is_weekend",  F.dayofweek("sensing_date").isin(1, 7))
        # Re-fetching an overlap window is deliberate (late corrections),
        # so duplicates are expected by design and dedup makes them free.
        .withWatermark("sensing_ts_local", "7 days")
        .dropDuplicatesWithinWatermark(["location_id", "sensing_ts_local"])
        .select(
            "id", "location_id", "sensor_name",
            "sensing_date", "hourday", "sensing_ts_local", "sensing_ts_utc",
            "day_of_week", "is_weekend",
            # Renamed: on the sensor table these names hold LABELS, not counts.
            F.col("direction_1").alias("direction_1_count"),
            F.col("direction_2").alias("direction_2_count"),
            F.col("pedestriancount").alias("pedestrian_count"),
            "_rescued_data", "_source_file", "_ingested_at",
            # `location` is dropped: it duplicates the sensor dimension, and a
            # fact table should carry keys, not denormalised geography.
        )
    )


# ── DIMENSION (SCD Type 2) ─────────────────────────────────────────────────
dp.create_streaming_table(
    name="silver.silver_sensor_scd",
    comment="SCD2 sensor dimension. A relocation or status change opens a new version.",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target="silver.silver_sensor_scd",
    source="bronze_sensor_locations",
    keys=["location_id"],
    sequence_by="_snapshot_date",
    stored_as_scd_type="2",
    # THE critical argument. The source is a full snapshot every run, so
    # without naming what counts as a real change, AUTO CDC opens a new
    # version per sensor per day and the dimension outgrows the fact table.
    track_history_column_list=[
        "sensor_description", "sensor_name", "status",
        "latitude", "longitude", "location_type",
        "direction_1", "direction_2", "note",
    ],
    except_column_list=["_rescued_data", "_source_file", "_ingested_at",
                        "ingest_date", "location"],
)


@dp.materialized_view(
    name="silver.silver_sensor_current",
    comment="The open SCD2 version per sensor. Join this for 'as it is now'.",
)
def silver_sensor_current():
    return (
        spark.read.table("silver.silver_sensor_scd")
        .filter(F.col("__END_AT").isNull())
        .withColumn("installation_date", F.to_date("installation_date"))
        .withColumn("is_active", F.col("status") == F.lit("A"))
        .withColumnRenamed("direction_1", "direction_1_label")
        .withColumnRenamed("direction_2", "direction_2_label")
    )