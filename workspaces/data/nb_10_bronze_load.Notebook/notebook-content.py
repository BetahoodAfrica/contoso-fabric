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

load_date = "latest"      # or an exact date such as "2026-11-22"; the pipeline will pass one
tables = ("Sales.SalesOrderHeader,Sales.SalesOrderDetail,Sales.Customer,Person.Person,Sales.Store,"
          "Production.Product,Production.ProductSubcategory,Production.ProductCategory,"
          "Sales.SalesTerritory,Person.Address,Person.StateProvince,Person.CountryRegion")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_00_common

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

def latest_load_date(schema: str, table: str) -> str:
    # Newest load_date=YYYY-MM-DD folder for this table
    folders = [f.name.rstrip("/") for f in notebookutils.fs.ls(onelake("lh_bronze", f"Files/raw/{schema}/{table}"))
               if f.name.startswith("load_date=")]
    return max(folders).split("=")[1]          # ISO dates sort correctly as text

for item in [x.strip() for x in tables.split(",")]:
    schema, table = item.split(".")
    run_date = load_date if load_date != "latest" else latest_load_date(schema, table)
    src = onelake("lh_bronze", f"Files/raw/{schema}/{table}/load_date={run_date}")
    df = (spark.read.parquet(src)
            .withColumn("_load_date", F.lit(run_date).cast("date"))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path")))
    (df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_load_date = '{run_date}'")   # replace only this day's rows
        .option("mergeSchema", "true")
        .save(onelake("lh_bronze", f"Tables/dbo/{snake(table)}")))
    print(f"{item}: load {run_date}, {df.count():,} rows written")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
