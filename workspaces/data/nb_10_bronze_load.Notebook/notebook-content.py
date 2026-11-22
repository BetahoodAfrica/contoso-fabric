# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# CELL ********************

# --- Parameters cell (toggle "parameter cell"; the pipeline overrides these) ---
load_date = "2026-11-21"
tables = ("Sales.SalesOrderHeader,Sales.SalesOrderDetail,Sales.Customer,Person.Person,Sales.Store,"
          "Production.Product,Production.ProductSubcategory,Production.ProductCategory,"
          "Sales.SalesTerritory,Person.Address,Person.StateProvince,Person.CountryRegion")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re
from pyspark.sql import functions as F

# The workspace the notebook runs in (feature, Dev, Test or Prod): no config needed
WS = notebookutils.runtime.context["currentWorkspaceName"]
# Per-environment settings (full_reload, alert_email...) come from the variable library
vl = notebookutils.variableLibrary.getLibrary("vl_config")

def onelake(lakehouse: str, sub: str) -> str:
    return f"abfss://{WS}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse/{sub}"

def snake(name: str) -> str:
    # SalesOrderHeader -> sales_order_header, SalesOrderID -> sales_order_id
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "_", name).lower()

for item in [x.strip() for x in tables.split(",")]:
    schema, table = item.split(".")
    src = onelake("lh_bronze", f"Files/raw/{schema}/{table}/load_date={load_date}")
    df = (spark.read.parquet(src)
            .withColumn("_load_date", F.lit(load_date).cast("date"))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path")))
    (df.write.format("delta").mode("append")
        .option("mergeSchema", "true")      # tolerate new source columns
        .save(onelake("lh_bronze", f"Tables/dbo/{snake(table)}")))
    print(f"{item}: {df.count():,} rows appended")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

