# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Step 0: make fake DHIS2-style facility reports
# MAGIC
# MAGIC Everything here is made up. No real facilities, districts or numbers.
# MAGIC Each record is one facility's monthly report: HIV tests done and HIV tests positive.
# MAGIC
# MAGIC Five problems are planted on purpose so the pipeline and DQX have something to catch:
# MAGIC 1. a report with no org unit
# MAGIC 2. a negative number of tests
# MAGIC 3. more positives than tests
# MAGIC 4. a period in the future
# MAGIC 5. the same facility and month reported twice (identical rows)
# MAGIC
# MAGIC Run it once to land the first batch, then run it again with `batch = 2` during the demo
# MAGIC to show the pipeline picking up only the new file.

# COMMAND ----------

dbutils.widgets.text("bronze_catalog", "bronze")
dbutils.widgets.text("silver_catalog", "silver")
dbutils.widgets.text("gold_catalog", "gold")
dbutils.widgets.text("schema", "dq_demo")
dbutils.widgets.text("batch", "1")

bronze_catalog = dbutils.widgets.get("bronze_catalog")
silver_catalog = dbutils.widgets.get("silver_catalog")
gold_catalog = dbutils.widgets.get("gold_catalog")
schema = dbutils.widgets.get("schema")
batch = int(dbutils.widgets.get("batch"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_catalog}.{schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_catalog}.{schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {bronze_catalog}.{schema}.raw")
landing = f"/Volumes/{bronze_catalog}/{schema}/raw/dhis2_monthly"
print("Landing files in:", landing)

# COMMAND ----------

import random
from datetime import datetime, timezone

random.seed(42 + batch)

districts = ["North Ridge", "Lake Shore", "Hill View", "River Bend"]
facilities = [
    (f"FAC{d_i}{f_i:02d}AbCdE", f"{d.split()[0]} Health Centre {f_i}", d)
    for d_i, d in enumerate(districts, start=1)
    for f_i in range(1, 6)
]

# batch 1 covers Jan to Jun 2026, batch 2 covers Jul to Sep 2026
periods = ["202601", "202602", "202603", "202604", "202605", "202606"] if batch == 1 \
    else ["202607", "202608", "202609"]

rows = []
for uid, name, district in facilities:
    for period in periods:
        tests = random.randint(40, 400)
        positives = int(tests * random.uniform(0.01, 0.08))
        rows.append({
            "org_unit_uid": uid,
            "org_unit_name": name,
            "district": district,
            "period": period,
            "hiv_tests_done": tests,
            "hiv_tests_positive": positives,
            "reported_at": datetime.now(timezone.utc).isoformat(),
        })

# COMMAND ----------

# MAGIC %md ## Plant the problems

# COMMAND ----------

p = periods[-1]
rows += [
    # 1. no org unit
    {"org_unit_uid": None, "org_unit_name": "Unknown", "district": "Lake Shore",
     "period": p, "hiv_tests_done": 120, "hiv_tests_positive": 4, "reported_at": None},
    # 2. negative tests (a typo in the form)
    {"org_unit_uid": "FAC203AbCdE", "org_unit_name": "Hill Health Centre 3", "district": "Hill View",
     "period": p, "hiv_tests_done": -35, "hiv_tests_positive": 2, "reported_at": None},
    # 3. more positives than tests (columns swapped)
    {"org_unit_uid": "FAC402AbCdE", "org_unit_name": "River Health Centre 2", "district": "River Bend",
     "period": p, "hiv_tests_done": 9, "hiv_tests_positive": 90, "reported_at": None},
    # 4. a period in the future
    {"org_unit_uid": "FAC105AbCdE", "org_unit_name": "North Health Centre 5", "district": "North Ridge",
     "period": "202712", "hiv_tests_done": 80, "hiv_tests_positive": 3, "reported_at": None},
]
# 5. same facility and month twice, identical values
rows.append(dict(rows[0]))

# COMMAND ----------

df = spark.createDataFrame(rows, schema="""
    org_unit_uid STRING, org_unit_name STRING, district STRING, period STRING,
    hiv_tests_done INT, hiv_tests_positive INT, reported_at STRING
""")

(df.coalesce(1)
   .write.mode("append")
   .json(f"{landing}/batch_{batch}"))

print(f"Wrote {df.count()} rows to {landing}/batch_{batch}")
display(df)