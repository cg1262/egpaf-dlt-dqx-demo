# Lakeflow Spark Declarative Pipeline: org unit history with SCD Type 2, Python version
#
# Same tables as 01_pipeline_org_unit_scd2.sql. Use one or the other in a pipeline, not both.
#
# Setup in the pipeline settings:
#   * Serverless on, default catalog and schema = the ones used in step 0
#   * Configuration key  org_units_path = /Volumes/<catalog>/<schema>/raw/dhis2_org_units

from pyspark import pipelines as dp
from pyspark.sql import functions as F

org_units_path = spark.conf.get("org_units_path")


# Bronze: org unit records exactly as they arrived
@dp.table(
    name="dhis2_org_units_bronze",
    comment="Raw DHIS2 org unit records exactly as they arrived",
)
def dhis2_org_units_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option(
            "cloudFiles.schemaHints",
            "level INT, opening_date STRING, closed_date STRING, last_updated STRING",
        )
        .load(org_units_path)
        .select(
            "*",
            F.col("_metadata.file_path").alias("source_file"),
            F.current_timestamp().alias("ingested_at"),
        )
    )


# Clean up types before the history step (not saved, just feeds the flow)
@dp.temporary_view(name="dhis2_org_units_changes")
def dhis2_org_units_changes():
    return (
        spark.readStream.table("dhis2_org_units_bronze")
        .where("org_unit_uid IS NOT NULL")
        .select(
            "org_unit_uid",
            "org_unit_name",
            "district",
            "level",
            F.col("opening_date").cast("date").alias("opening_date"),
            F.col("closed_date").cast("date").alias("closed_date"),
            "phone_number",
            F.col("last_updated").cast("timestamp").alias("last_updated"),
        )
    )


# Silver: the history table, filled by AUTO CDC
dp.create_streaming_table(
    name="dim_org_unit_history",
    comment="Every version of every org unit. Current version has __END_AT IS NULL",
)

dp.create_auto_cdc_flow(
    target="dim_org_unit_history",
    source="dhis2_org_units_changes",
    keys=["org_unit_uid"],
    sequence_by="last_updated",
    stored_as_scd_type=2,
    # only these columns create a new version; others are updated on the current row
    track_history_column_list=["org_unit_name", "district", "closed_date"],
)


# Gold: one row per org unit, as it is today
@dp.materialized_view(
    name="dim_org_unit_current",
    comment="One row per org unit with its current name, district and status",
)
def dim_org_unit_current():
    return (
        spark.read.table("dim_org_unit_history")
        .where("__END_AT IS NULL")
        .select(
            "org_unit_uid",
            "org_unit_name",
            "district",
            "level",
            "opening_date",
            "closed_date",
            F.col("closed_date").isNull().alias("is_open"),
            "phone_number",
            F.col("__START_AT").alias("current_since"),
        )
    )


# Gold: the full history with an is_current flag
@dp.materialized_view(
    name="dim_org_unit_history_flagged",
    comment="Every version of every org unit, with an is_current flag",
)
def dim_org_unit_history_flagged():
    return spark.read.table("dim_org_unit_history").withColumn(
        "is_current", F.col("__END_AT").isNull()
    )
