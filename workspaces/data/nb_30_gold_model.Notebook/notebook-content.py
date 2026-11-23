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
load_date = "latest"

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

from delta.tables import DeltaTable
from pyspark.sql import Window, functions as F

for t in ["sales_order_header", "sales_order_detail", "customer", "person", "store", "product",
          "product_subcategory", "product_category", "sales_territory", "address",
          "state_province", "country_region"]:
    spark.read.format("delta").load(onelake("lh_silver", f"Tables/dbo/{t}")).createOrReplaceTempView(f"s_{t}")
    
scd_date = load_date if load_date != "latest" else str(spark.table("s_product").agg(F.max("_load_date")).first()[0])
print("Modelling as of:", scd_date)

def write_gold(df, name):
    # V-Order speeds up Direct Lake reads: enable it for the session first,
    # e.g. spark.conf.set("spark.sql.parquet.vorder.default", "true") (confirm the name for your runtime)
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .save(onelake("lh_gold", f"Tables/dbo/{name}")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_customer = spark.sql("""
  SELECT c.customer_id AS customer_key, c.account_number,
         CASE WHEN c.store_id IS NOT NULL THEN 'Store' ELSE 'Individual' END AS customer_type,
         COALESCE(s.name, p.full_name, 'Unknown') AS customer_name
  FROM s_customer c
  LEFT JOIN s_person p ON c.person_id = p.business_entity_id
  LEFT JOIN s_store  s ON c.store_id  = s.business_entity_id
""")

dim_sales_territory = spark.sql("""
  SELECT territory_id AS sales_territory_key, name AS region,
         country_region_code, territory_group
  FROM s_sales_territory
""")

dim_geography = spark.sql("""
  SELECT a.address_id AS geography_key, a.city, a.postal_code,
         sp.name AS state_province, cr.name AS country
  FROM s_address a
  JOIN s_state_province sp ON a.state_province_id   = sp.state_province_id
  JOIN s_country_region cr ON sp.country_region_code = cr.country_region_code
""")

b = spark.sql("SELECT MIN(order_date) lo, MAX(order_date) hi FROM s_sales_order_header").first()
dim_date = (spark.sql(f"""SELECT explode(sequence(to_date('{b.lo.year}-01-01'),
                                                  to_date('{b.hi.year}-12-31'), interval 1 day)) AS date""")
    .select(F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"), "date",
            F.year("date").alias("year"), F.quarter("date").alias("quarter"),
            F.month("date").alias("month_number"), F.date_format("date", "MMMM").alias("month_name"),
            F.date_format("date", "yyyy-MM").alias("year_month"),
            F.date_format("date", "EEEE").alias("day_name")))

for name, df in {"dim_customer": dim_customer, "dim_sales_territory": dim_sales_territory,
                 "dim_geography": dim_geography, "dim_date": dim_date}.items():
    write_gold(df, name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

src = spark.sql("""
  SELECT p.product_id, p.product_number AS product_code, p.name AS product_name, p.color,
         COALESCE(sc.name, 'Unknown') AS subcategory, COALESCE(c.name, 'Unknown') AS category,
         p.list_price, p.standard_cost
  FROM s_product p
  LEFT JOIN s_product_subcategory sc ON p.product_subcategory_id = sc.product_subcategory_id
  LEFT JOIN s_product_category  c  ON sc.product_category_id   = c.product_category_id
""")
TRACKED = ["product_name", "subcategory", "category", "list_price", "standard_cost"]
src = src.withColumn("row_hash", F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in TRACKED]), 256))

path = onelake("lh_gold", "Tables/dbo/dim_product")
open_end = F.lit("9999-12-31").cast("date")

if not DeltaTable.isDeltaTable(spark, path):
    # First load: one version per product, valid for all history
    (src.withColumn("product_key", F.row_number().over(Window.orderBy("product_id")))
        .withColumn("valid_from", F.lit("1900-01-01").cast("date"))
        .withColumn("valid_to", open_end).withColumn("is_current", F.lit(True))
        .write.format("delta").save(path))
else:
    dim = DeltaTable.forPath(spark, path)
    current = dim.toDF().filter("is_current")
    changed = (src.alias("s").join(current.alias("d"), "product_id", "left")
                  .filter("d.product_id IS NULL OR d.row_hash <> s.row_hash")
                  .select("s.*")).localCheckpoint()        # freeze before the dimension changes
    run_date = F.lit(scd_date).cast("date")

    # a) close the current version of every changed product
    (dim.alias("d").merge(changed.alias("s"), "d.product_id = s.product_id AND d.is_current")
        .whenMatchedUpdate(set={"valid_to": F.date_sub(run_date, 1), "is_current": F.lit(False)})
        .execute())

    # b) add the new versions with new surrogate keys
    max_key = dim.toDF().agg(F.max("product_key")).first()[0]
    (changed.withColumn("product_key", F.row_number().over(Window.orderBy("product_id")) + F.lit(max_key))
            .withColumn("valid_from", run_date).withColumn("valid_to", open_end)
            .withColumn("is_current", F.lit(True))
            .write.format("delta").mode("append").save(path))

spark.read.format("delta").load(path).createOrReplaceTempView("g_dim_product")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_sales = spark.sql("""
  WITH lines AS (
    SELECT d.sales_order_id, d.sales_order_detail_id, h.sales_order_number,
           h.order_date, h.order_date_key, h.customer_id, h.territory_id, h.ship_to_address_id,
           CASE WHEN h.online_order_flag THEN 'Internet' ELSE 'Reseller' END AS sales_channel,
           d.product_id, d.order_qty, d.unit_price, d.unit_price_discount, d.line_total,
           h.sub_total, h.tax_amt, h.freight
    FROM s_sales_order_detail d
    JOIN s_sales_order_header h ON d.sales_order_id = h.sales_order_id
  )
  SELECT l.sales_order_id, l.sales_order_detail_id, l.sales_order_number, l.sales_channel,
         l.order_date_key, l.customer_id AS customer_key, l.territory_id AS sales_territory_key,
         l.ship_to_address_id AS geography_key, COALESCE(p.product_key, -1) AS product_key,
         l.order_qty, l.unit_price, l.unit_price_discount,
         CAST(l.line_total AS DECIMAL(19,4))                          AS sales_amount,
         CAST(l.order_qty * p.standard_cost AS DECIMAL(19,4))         AS total_product_cost,
         CAST(l.tax_amt * l.line_total / l.sub_total AS DECIMAL(19,4)) AS tax_amount,
         CAST(l.freight * l.line_total / l.sub_total AS DECIMAL(19,4)) AS freight_amount
  FROM lines l
  LEFT JOIN g_dim_product p
    ON l.product_id = p.product_id AND l.order_date BETWEEN p.valid_from AND p.valid_to
""")
write_gold(fact_sales, "fact_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fs = spark.read.format("delta").load(onelake("lh_gold", "Tables/dbo/fact_sales"))
dp = spark.read.format("delta").load(onelake("lh_gold", "Tables/dbo/dim_product"))
checks = {
    "fact_sales has rows": fs.count() > 0,
    "rows = silver order lines": fs.count() == spark.table("s_sales_order_detail").count(),
    "no duplicate order lines": fs.groupBy("sales_order_id", "sales_order_detail_id").count().filter("count > 1").count() == 0,
    "every sale found its product version": fs.filter("product_key = -1").count() == 0,
    "every product_key exists in dim_product": fs.join(dp, "product_key", "left_anti").count() == 0,
}
for name, ok in checks.items():
    print(("PASS  " if ok else "FAIL  ") + name)
assert all(checks.values()), "Gold data quality failed"

fs.groupBy("sales_channel").agg(F.count("*").alias("lines"), F.round(F.sum("sales_amount"), 2).alias("sales")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
