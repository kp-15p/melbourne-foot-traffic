from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("project.catalog", "ped_dev")
RAW     = f"/Volumes/{CATALOG}/landing/raw"


@dp.table(
    name="bronze_pedestrian_hourly",
    comment="Hourly counts exactly as the API returned them. Replay source of truth.",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"},
)
def bronze_pedestrian_hourly():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints",
                "id BIGINT, location_id INT, hourday INT, pedestriancount INT, "
                "direction_1 INT, direction_2 INT, sensing_date DATE, sensor_name STRING")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.partitionColumns", "ingest_month")
        .option("cloudFiles.useIncrementalListing", "false")
        .load(f"{RAW}/pedestrian_hourly")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )


@dp.table(
    name="bronze_sensor_locations",
    comment="Daily full snapshot of sensor metadata. Feeds the SCD2 dimension.",
    table_properties={"quality": "bronze"},
)
def bronze_sensor_locations():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints",
                "location_id INT, latitude DOUBLE, longitude DOUBLE, _snapshot_date DATE")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.partitionColumns", "ingest_date")
        .load(f"{RAW}/sensor_locations")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )