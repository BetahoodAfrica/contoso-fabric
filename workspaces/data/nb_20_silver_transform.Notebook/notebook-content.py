# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
load_date = "latest"              # the bronze load to process
tables = "all"         # one table to start; "all" for everything

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

# src/transforms/silver.py
import re
from pyspark.sql import DataFrame, Window, functions as F

def snake(name: str) -> str:
    # SalesOrderID -> sales_order_id, ProductSubcategoryID -> product_subcategory_id
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "_", name).lower()

def to_snake_columns(df: DataFrame) -> DataFrame:
    return df.toDF(*[c if c.startswith("_") else snake(c) for c in df.columns])

def latest_per_key(df: DataFrame, keys: list[str]) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

def split_valid(df: DataFrame, keys: list[str]) -> tuple[DataFrame, DataFrame]:
    bad = " OR ".join(f"{k} IS NULL" for k in keys)
    return df.filter(f"NOT ({bad})"), df.filter(bad)

# Per-table cleaning, applied before the MERGE
def clean_sales_order_header(df):
    return (df.withColumn("order_date", F.to_date("order_date"))
              .withColumn("order_date_key", F.date_format("order_date", "yyyyMMdd").cast("int")))

def clean_person(df):
    return (df.withColumn("first_name", F.trim("first_name"))
              .withColumn("last_name", F.trim("last_name"))
              .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name")))

def clean_sales_territory(df):
    return df.withColumnRenamed("group", "territory_group")

CLEANERS = {"sales_order_header": clean_sales_order_header,
            "person": clean_person,
            "sales_territory": clean_sales_territory}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import functions as F
# assumes onelake() and vl_config set-up from nb_10, plus the functions above

CONFIG = {
    "sales_order_header": ["sales_order_id"],
    "sales_order_detail": ["sales_order_id", "sales_order_detail_id"],
    "customer": ["customer_id"],
    "person": ["business_entity_id"],
    "store": ["business_entity_id"],
    "product": ["product_id"],
    "product_subcategory": ["product_subcategory_id"],
    "product_category": ["product_category_id"],
    "sales_territory": ["territory_id"],
    "address": ["address_id"],
    "state_province": ["state_province_id"],
    "country_region": ["country_region_code"],
}

run_list = CONFIG if tables == "all" else {t.strip(): CONFIG[t.strip()] for t in tables.split(",")}
for table, keys in run_list.items():
    bronze = spark.read.format("delta").load(onelake("lh_bronze", f"Tables/dbo/{table}"))
    run_date = load_date if load_date != "latest" else bronze.agg(F.max("_load_date")).first()[0]
    src = to_snake_columns(bronze).filter(F.col("_load_date") == run_date)
    print(f"{table}: processing load {run_date}")
    src = latest_per_key(src, keys)
    src = CLEANERS.get(table, lambda d: d)(src)
    good, bad = split_valid(src, keys)

    if bad.limit(1).count():
        (bad.withColumn("_table", F.lit(table))
            .select("_table", F.to_json(F.struct(*bad.columns)).alias("payload"), "_ingested_at")
            .write.format("delta").mode("append")
            .save(onelake("lh_silver", "Tables/dbo/quarantine")))

    good = good.drop("_source_file").withColumn("_updated_at", F.current_timestamp())
    target_path = onelake("lh_silver", f"Tables/dbo/{table}")

    if not DeltaTable.isDeltaTable(spark, target_path):
        good.write.format("delta").save(target_path)
        continue

    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (DeltaTable.forPath(spark, target_path).alias("t")
        .merge(good.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for t in ["sales_order_header", "sales_order_detail", "customer", "person", "store", "product",
          "product_subcategory", "product_category", "sales_territory", "address",
          "state_province", "country_region"]:
    n = spark.read.format("delta").load(onelake("lh_silver", f"Tables/dbo/{t}")).count()
    print(f"{t:<25}{n:>10,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
