-- Lakeflow Spark Declarative Pipeline: org unit history with SCD Type 2 (fake DHIS2 data)
--
-- Setup in the pipeline settings:
--   * Serverless on, default catalog and schema = the ones used in step 0
--   * Configuration key  org_units_path = /Volumes/<catalog>/<schema>/raw/dhis2_org_units
--
-- You can add this file to the Nov 18 demo pipeline as another source file, or make a new
-- pipeline for it. Use this SQL file OR the Python version, not both: they define the same tables.

-- ---------------------------------------------------------------------------
-- Bronze: org unit records exactly as they arrived, one row per export record.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE dhis2_org_units_bronze
COMMENT 'Raw DHIS2 org unit records exactly as they arrived'
AS SELECT
  *,
  _metadata.file_path AS source_file,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '${org_units_path}',
  format => 'json',
  schemaHints => 'level INT, opening_date STRING, closed_date STRING, last_updated STRING'
);

-- ---------------------------------------------------------------------------
-- Clean up types before the history step. A temporary view is enough here:
-- it is not saved anywhere, it just feeds the flow below.
-- ---------------------------------------------------------------------------
CREATE TEMPORARY VIEW dhis2_org_units_changes AS
SELECT
  org_unit_uid,
  org_unit_name,
  district,
  level,
  CAST(opening_date AS DATE)      AS opening_date,
  CAST(closed_date AS DATE)       AS closed_date,
  phone_number,
  CAST(last_updated AS TIMESTAMP) AS last_updated
FROM STREAM(dhis2_org_units_bronze)
WHERE org_unit_uid IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Silver: the history table. Declare it empty, then let AUTO CDC fill it.
--
--   KEYS              = what identifies one org unit (the DHIS2 UID)
--   SEQUENCE BY       = which version is newer (DHIS2 lastUpdated), so late files land in order
--   STORED AS SCD TYPE 2 = keep every version, with __START_AT and __END_AT
--   TRACK HISTORY ON  = only these columns create a new version. Any other column
--                       (phone number, level) is just updated on the current row.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE dim_org_unit_history
COMMENT 'Every version of every org unit. Current version has __END_AT IS NULL';

CREATE FLOW org_unit_history_flow AS AUTO CDC INTO dim_org_unit_history
FROM STREAM(dhis2_org_units_changes)
KEYS (org_unit_uid)
SEQUENCE BY last_updated
STORED AS SCD TYPE 2
TRACK HISTORY ON org_unit_name, district, closed_date;

-- ---------------------------------------------------------------------------
-- Gold: one row per org unit, as it is today. This is what most reports join to.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW dim_org_unit_current
COMMENT 'One row per org unit with its current name, district and status'
AS SELECT
  org_unit_uid,
  org_unit_name,
  district,
  level,
  opening_date,
  closed_date,
  closed_date IS NULL AS is_open,
  phone_number,
  __START_AT AS current_since
FROM dim_org_unit_history
WHERE __END_AT IS NULL;

-- ---------------------------------------------------------------------------
-- Gold: the full history with a yes/no "is this the current version" flag,
-- for people who prefer a flag to checking __END_AT.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW dim_org_unit_history_flagged
COMMENT 'Every version of every org unit, with an is_current flag'
AS SELECT
  *,
  __END_AT IS NULL AS is_current
FROM dim_org_unit_history;
