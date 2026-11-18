-- Optional: join the facility monthly reports to the org unit AS IT WAS in that month.
--
-- This needs dhis2_monthly_silver from the Nov 18 demo, so add this file to the same pipeline
-- as 01_pipeline_dhis2_quality.sql and the SCD2 file.
--
-- Why it matters: River Health Centre 2 moved to Lake Shore in April 2026.
-- Joined to the CURRENT org unit, all of its reports would count for Lake Shore, even January.
-- Joined AS OF the reporting month, January to April stay in River Bend.
-- (In the fake reports the district is typed on each report, so you can compare the two.)
--
-- Rule for "as of a date D": __START_AT <= D and (__END_AT > D or __END_AT is null).
-- Here D is the first day of the reporting month.

CREATE OR REFRESH MATERIALIZED VIEW reports_with_org_unit_as_of
COMMENT 'Facility monthly reports with the org unit name, district and status that applied that month'
AS SELECT
  r.org_unit_uid,
  r.period,
  r.hiv_tests_done,
  r.hiv_tests_positive,
  r.district         AS district_on_report,
  h.org_unit_name    AS org_unit_name_that_month,
  h.district         AS district_that_month,
  h.closed_date,
  -- a report for a month after the facility closed is worth a follow-up
  coalesce(to_date(r.period, 'yyyyMM') > h.closed_date, false) AS reported_after_closure
FROM dhis2_monthly_silver r
LEFT JOIN dim_org_unit_history h
  ON  r.org_unit_uid = h.org_unit_uid
  AND to_timestamp(r.period, 'yyyyMM') >= h.__START_AT
  AND (to_timestamp(r.period, 'yyyyMM') < h.__END_AT OR h.__END_AT IS NULL);
