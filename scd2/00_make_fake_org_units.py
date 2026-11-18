# Databricks notebook source
# MAGIC %md
# MAGIC # Step 0: make fake DHIS2-style org units that change over time
# MAGIC
# MAGIC Everything here is made up. No real facilities, districts or phone numbers.
# MAGIC Each record is one org unit (a health facility) as a DHIS2 export would describe it,
# MAGIC with its `last_updated` time. The UIDs match the facility reports in the Nov 18 demo,
# MAGIC so the two can be joined.
# MAGIC
# MAGIC - `batch = 1`: the first full load, 20 facilities, all last updated Dec 1, 2025.
# MAGIC - `batch = 2`: only what changed since then (like pulling org units with a lastUpdated filter):
# MAGIC   1. Hill Health Centre 3 is renamed (it became a district hospital)
# MAGIC   2. River Health Centre 2 moves from River Bend to Lake Shore (a boundary change)
# MAGIC   3. North Health Centre 5 closes
# MAGIC   4. North Health Centre 1 gets a new phone number (a change we do NOT keep history for)
# MAGIC   5. a new facility, River Health Centre 6, opens
# MAGIC
# MAGIC Run batch 1, run the pipeline, then run batch 2 and run the pipeline again.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "dq_demo")
dbutils.widgets.text("batch", "1")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
batch = int(dbutils.widgets.get("batch"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw")
landing = f"/Volumes/{catalog}/{schema}/raw/dhis2_org_units"
print("Landing files in:", landing)

# COMMAND ----------

import random

random.seed(7)

districts = ["North Ridge", "Lake Shore", "Hill View", "River Bend"]

def facility(d_i, f_i, district):
    return {
        "org_unit_uid": f"FAC{d_i}{f_i:02d}AbCdE",
        "org_unit_name": f"{district.split()[0]} Health Centre {f_i}",
        "district": district,
        "level": 4,
        "opening_date": f"{random.randint(2008, 2020)}-0{random.randint(1, 9)}-01",
        "closed_date": None,
        "phone_number": f"+000 555 {d_i}{f_i:02d}0",
        "last_updated": "2025-12-01T08:00:00Z",
    }

first_load = [
    facility(d_i, f_i, d)
    for d_i, d in enumerate(districts, start=1)
    for f_i in range(1, 6)
]
by_uid = {r["org_unit_uid"]: r for r in first_load}

# COMMAND ----------

if batch == 1:
    rows = first_load
else:
    def changed(uid, last_updated, **new_values):
        row = dict(by_uid[uid])
        row.update(new_values)
        row["last_updated"] = last_updated
        return row

    rows = [
        # 1. renamed
        changed("FAC203AbCdE", "2026-04-10T09:00:00Z", org_unit_name="Hill View District Hospital"),
        # 2. moved to another district
        changed("FAC402AbCdE", "2026-04-15T09:00:00Z", district="Lake Shore"),
        # 3. closed
        changed("FAC105AbCdE", "2026-04-30T17:00:00Z", closed_date="2026-04-30"),
        # 4. phone number only: updated in place, no new history row
        changed("FAC101AbCdE", "2026-04-20T09:00:00Z", phone_number="+000 555 99990"),
        # 5. brand new facility
        {
            "org_unit_uid": "FAC406AbCdE",
            "org_unit_name": "River Health Centre 6",
            "district": "River Bend",
            "level": 4,
            "opening_date": "2026-04-01",
            "closed_date": None,
            "phone_number": "+000 555 40600",
            "last_updated": "2026-04-01T08:00:00Z",
        },
    ]

# COMMAND ----------

df = spark.createDataFrame(rows, schema="""
    org_unit_uid STRING, org_unit_name STRING, district STRING, level INT,
    opening_date STRING, closed_date STRING, phone_number STRING, last_updated STRING
""")

(df.coalesce(1)
   .write.mode("append")
   .json(f"{landing}/batch_{batch}"))

print(f"Wrote {df.count()} rows to {landing}/batch_{batch}")
display(df)
