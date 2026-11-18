# Databricks notebook source
# MAGIC %md
# MAGIC # Step 2: the same rules with DQX
# MAGIC
# MAGIC DQX is a free Databricks Labs library (not an official, supported product).
# MAGIC It works on any Spark DataFrame, inside or outside a pipeline.
# MAGIC
# MAGIC What it adds on top of expectations:
# MAGIC - the reason a row failed, written on the row itself
# MAGIC - error vs warning levels per rule
# MAGIC - rules kept in YAML (or a table), so one set of rules can serve every country
# MAGIC - a profiler that suggests starting rules from your data
# MAGIC
# MAGIC Checked against the DQX docs on Nov 17, 2026 (docs show v0.17). Run the install cell first.

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "dq_demo")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

dq_engine = DQEngine(WorkspaceClient())

# COMMAND ----------

# MAGIC %md ## The rules, in plain YAML
# MAGIC `error` rows go to quarantine. `warn` rows stay in, flagged.

# COMMAND ----------

checks = yaml.safe_load("""
- name: has_org_unit
  criticality: error
  check:
    function: is_not_null
    arguments:
      column: org_unit_uid

- name: tests_not_negative
  criticality: error
  check:
    function: is_not_less_than
    arguments:
      column: hiv_tests_done
      limit: 0

- name: positives_not_negative
  criticality: error
  check:
    function: is_not_less_than
    arguments:
      column: hiv_tests_positive
      limit: 0

- criticality: error
  check:
    function: sql_expression
    arguments:
      expression: hiv_tests_positive <= hiv_tests_done
      msg: more positives than tests
      name: positives_within_tests

- criticality: warn
  check:
    function: sql_expression
    arguments:
      expression: to_date(period, 'yyyyMM') <= current_date()
      msg: reporting period is in the future
      name: period_not_in_future
""")

# Catch typos in the rules before running them; any problem shows up here
print(DQEngine.validate_checks(checks))

# COMMAND ----------

# MAGIC %md ## Run the rules and split good rows from bad rows

# COMMAND ----------

reports = spark.read.table(f"{catalog}.{schema}.dhis2_monthly_bronze")

valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(reports, checks)

print("valid rows:", valid_df.count(), "| quarantined rows:", quarantine_df.count())

# Each quarantined row carries the reason it failed. Check the column names on your
# version (the docs show _errors and _warnings).
display(quarantine_df)

# COMMAND ----------

(valid_df.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.dhis2_monthly_dqx_valid"))
(quarantine_df.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.dhis2_monthly_dqx_quarantine"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: one call that reads, checks and saves
# MAGIC Handy in a scheduled job. Same rules, no extra code.

# COMMAND ----------

from databricks.labs.dqx.config import InputConfig, OutputConfig

dq_engine.apply_checks_by_metadata_and_save_in_table(
    checks=checks,
    input_config=InputConfig(location=f"{catalog}.{schema}.dhis2_monthly_bronze"),
    output_config=OutputConfig(location=f"{catalog}.{schema}.dhis2_monthly_dqx_valid", mode="overwrite"),
    quarantine_config=OutputConfig(location=f"{catalog}.{schema}.dhis2_monthly_dqx_quarantine", mode="overwrite"),
)
