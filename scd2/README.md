# SCD Type 2 example: keeping the history of DHIS2 org units

All data here is made up. Run it on your own workspace, not on EGPAF systems.

Org units do not change often, but when they do (a facility is renamed, moves to another
district, or closes) you usually want two answers: what it looks like today, and what it
looked like in a given month. Slowly changing dimension Type 2 (SCD2) keeps both by adding
a new row for each change instead of overwriting the old one.

## Files

1. `00_make_fake_org_units.py`: notebook. Run with `batch = 1` for the first load of 20
   facilities. Later run with `batch = 2` to land only the changes: one rename, one district
   move, one closure, one phone number change and one new facility.
2. `01_pipeline_org_unit_scd2.sql`: pipeline source file (serverless). Set the configuration
   key `org_units_path` to the volume folder from step 1. Builds bronze, the history table
   `dim_org_unit_history` with `AUTO CDC ... STORED AS SCD TYPE 2`, and two gold views.
3. `01_pipeline_org_unit_scd2_python.py`: the same pipeline in Python. Use this or the SQL
   file, not both.
4. `02_reports_with_org_unit_as_of.sql`: optional. Joins the Nov 18 facility reports to the
   org unit as it was in each reporting month. Add it to the same pipeline as the Nov 18 demo.

Run order: step 0 with batch 1, run the pipeline, step 0 with batch 2, run the pipeline again.

## What the history table looks like

AUTO CDC adds two columns to `dim_org_unit_history`:

| Column | Meaning |
|---|---|
| `__START_AT` | When this version started (the `last_updated` value that brought it in) |
| `__END_AT` | When this version stopped. Empty (NULL) for the current version |

There is no separate active flag column. A row is current when `__END_AT IS NULL`.
The view `dim_org_unit_history_flagged` adds `is_current` if you prefer a flag.

After batch 2, River Health Centre 2 has two rows:

| org_unit_uid | district | __START_AT | __END_AT |
|---|---|---|---|
| FAC402AbCdE | River Bend | 2025-12-01 08:00 | 2026-04-15 09:00 |
| FAC402AbCdE | Lake Shore | 2026-04-15 09:00 | NULL |

The old row's `__END_AT` equals the new row's `__START_AT`, so there are no gaps or overlaps.

`TRACK HISTORY ON org_unit_name, district, closed_date` means only those columns create a
new version. The phone number change for North Health Centre 1 just updates its current
row, so it still has one row. Pick the columns your reports care about and leave the rest out.

Expected after batch 2: 24 rows in `dim_org_unit_history` (20 first load, 3 new versions,
1 new facility), 3 of them with `__END_AT` filled in, and 21 rows in `dim_org_unit_current`
(North Health Centre 5 shows `is_open = false`).

## Querying it

Today's version of every org unit (or just read `dim_org_unit_current`):

```sql
SELECT * FROM dim_org_unit_history WHERE __END_AT IS NULL;
```

Every version of one org unit:

```sql
SELECT org_unit_name, district, closed_date, __START_AT, __END_AT
FROM dim_org_unit_history
WHERE org_unit_uid = 'FAC402AbCdE'
ORDER BY __START_AT;
```

What org units looked like on a given date (start included, end not included):

```sql
SELECT *
FROM dim_org_unit_history
WHERE __START_AT <= TIMESTAMP'2026-03-01'
  AND (__END_AT > TIMESTAMP'2026-03-01' OR __END_AT IS NULL);
```

## Using it with your own DHIS2 data

- Key on the DHIS2 org unit UID and sequence by `lastUpdated`. The sequence column is how the
  pipeline knows which version is newer, even if files arrive late or out of order.
- The example assumes each new file holds only org units that changed. If your export lands
  the full list every time instead, look at `AUTO CDC ... FROM SNAPSHOT` (SQL, needs
  Databricks Runtime 18.3 or later) or `create_auto_cdc_from_snapshot_flow` in Python, which
  compares each full export to the last one and works out the changes for you.
- Once the history table holds real history, protect it from a full refresh, which rebuilds
  it from whatever files are still in the landing folder. Adding
  `TBLPROPERTIES ('pipelines.reset.allowed' = 'false')` to the table does that.
