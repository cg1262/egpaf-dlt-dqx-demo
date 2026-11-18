-- Lakeflow Spark Declarative Pipeline: fake DHIS2 facility reports, bronze to silver to gold
--
-- Setup in the pipeline settings:
--   * Serverless on, default catalog and schema = the ones used in step 0
--   * Configuration key  source_path = /Volumes/<catalog>/<schema>/raw/dhis2_monthly
--
-- Talking point: you describe the tables you want. The pipeline works out the order,
-- only reads new files, and records how many rows passed or failed each rule.

-- ---------------------------------------------------------------------------
-- Bronze: land the raw JSON as is. No cleanup, so we can always go back to it.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze.dq_demo.dhis2_monthly_bronze
COMMENT 'Raw facility monthly reports exactly as they arrived'
AS SELECT
  *,
  _metadata.file_path AS source_file,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/bronze/dq_demo/raw/dhis2_monthly',
  format => 'json',
  schemaHints => 'period STRING, hiv_tests_done INT, hiv_tests_positive INT'
);

-- ---------------------------------------------------------------------------
-- Silver: the same rows, with the rules written down next to the table.
--   DROP ROW    = keep it out of silver (it goes to quarantine below)
--   no action   = keep the row, just count it as a warning
--   FAIL UPDATE = stop the whole run (save this for "never ever" rules)
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver.dq_demo.dhis2_monthly_silver (
  CONSTRAINT has_org_unit           EXPECT (org_unit_uid IS NOT NULL)                ON VIOLATION DROP ROW,
  CONSTRAINT tests_not_negative     EXPECT (coalesce(hiv_tests_done, 0) >= 0)                                    ON VIOLATION DROP ROW,
  CONSTRAINT positives_not_negative EXPECT (coalesce(hiv_tests_positive, 0) >= 0)                                ON VIOLATION DROP ROW,
  CONSTRAINT positives_within_tests EXPECT (coalesce(hiv_tests_positive, 0) <= coalesce(hiv_tests_done, 0))    ON VIOLATION DROP ROW,
  CONSTRAINT period_not_in_future   EXPECT (try_to_date(period, 'yyyyMM') <= current_date())
)
COMMENT 'Facility monthly reports that passed the hard rules'
AS SELECT
  org_unit_uid,
  org_unit_name,
  district,
  period,
  hiv_tests_done,
  hiv_tests_positive,
  source_file,
  ingested_at
FROM STREAM(bronze.dq_demo.dhis2_monthly_bronze);

-- ---------------------------------------------------------------------------
-- Quarantine: the rows silver dropped, with the reason, so someone can follow up
-- with the facility instead of the row just disappearing.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver.dq_demo.dhis2_monthly_quarantine
COMMENT 'Reports that broke a hard rule, with the reason'
AS SELECT
  *,
  concat_ws(', ',
    CASE WHEN org_unit_uid IS NULL THEN 'missing org unit' END,
    CASE WHEN coalesce(hiv_tests_done, 0) < 0 THEN 'negative tests' END,
    CASE WHEN coalesce(hiv_tests_positive, 0) < 0 THEN 'negative positives' END,
    CASE WHEN coalesce(hiv_tests_positive, 0) > coalesce(hiv_tests_done, 0) THEN 'positives more than tests' END
  ) AS failed_rules
FROM STREAM(bronze.dq_demo.dhis2_monthly_bronze)
WHERE org_unit_uid IS NULL
   OR coalesce(hiv_tests_done, 0) < 0
   OR coalesce(hiv_tests_positive, 0) < 0
   OR coalesce(hiv_tests_positive, 0) > coalesce(hiv_tests_done, 0);

-- ---------------------------------------------------------------------------
-- Gold: what a Power BI or AI/BI dashboard would read.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold.dq_demo.district_monthly_testing
COMMENT 'HIV testing totals and positivity by district and month'
AS SELECT
  district,
  period,
  SUM(hiv_tests_done)     AS tests_done,
  SUM(hiv_tests_positive) AS tests_positive,
  ROUND(100 * SUM(hiv_tests_positive) / NULLIF(SUM(hiv_tests_done), 0), 1) AS positivity_pct
FROM silver.dq_demo.dhis2_monthly_silver
GROUP BY district, period;

-- ---------------------------------------------------------------------------
-- Checks on the whole table, not one row at a time.
-- These are private: they run with the pipeline but do not show up in the catalog.
-- ---------------------------------------------------------------------------

-- Same facility and month reported more than once. A warning, not a drop:
-- this is the kind of thing Mike hit with the Cameroon import.
CREATE OR REFRESH PRIVATE MATERIALIZED VIEW gold.dq_demo.check_one_report_per_facility_month (
  CONSTRAINT one_report_per_facility_month EXPECT (duplicate_keys = 0)
)
AS SELECT COUNT(*) AS duplicate_keys
FROM (
  SELECT org_unit_uid, period
  FROM silver.dq_demo.dhis2_monthly_silver
  GROUP BY org_unit_uid, period
  HAVING COUNT(*) > 1
);

-- Nothing gets lost: every bronze row is either in silver or in quarantine.
-- If this ever fails, the run stops before anyone reads a wrong total.
CREATE OR REFRESH PRIVATE MATERIALIZED VIEW gold.dq_demo.check_nothing_lost (
  CONSTRAINT bronze_equals_silver_plus_quarantine
    EXPECT (bronze_rows = silver_rows + quarantine_rows) ON VIOLATION FAIL UPDATE
)
AS SELECT
  (SELECT COUNT(*) FROM bronze.dq_demo.dhis2_monthly_bronze)     AS bronze_rows,
  (SELECT COUNT(*) FROM silver.dq_demo.dhis2_monthly_silver)     AS silver_rows,
  (SELECT COUNT(*) FROM silver.dq_demo.dhis2_monthly_quarantine) AS quarantine_rows;



