# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "55426c38-4479-4246-a001-878ef1830557",
# META       "default_lakehouse_name": "retail_lakehouse",
# META       "default_lakehouse_workspace_id": "20706a0b-f5cf-407c-8c0c-39a3c7e901d9",
# META       "known_lakehouses": [
# META         {
# META           "id": "55426c38-4479-4246-a001-878ef1830557"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

customers = spark.table("silver_customers")
products  = spark.table("silver_products")
sales     = spark.table("silver_sales")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_customer = (
    customers
    .select("customer_id", "name", "gender_clean", "city")
    .dropDuplicates(["customer_id"])
)

w = Window.orderBy("customer_id")
dim_customer = dim_customer.withColumn("customer_sk", F.row_number().over(w))

dim_customer.createOrReplaceTempView("dim_customer_vw")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_product = (
    products
    .select("product_id", "product_name", "category", "price")
    .dropDuplicates(["product_id"])
)

w2 = Window.orderBy("product_id")
dim_product = dim_product.withColumn("product_sk", F.row_number().over(w2))

dim_product.createOrReplaceTempView("dim_product_vw")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_date = (
    sales.select(F.col("sale_date").alias("date"))
    .dropna()
    .dropDuplicates()
    .withColumn("date_sk", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("day", F.dayofmonth("date"))
)

dim_date.createOrReplaceTempView("dim_date_vw")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_sales = (
    sales.alias("s")
    .join(dim_customer.alias("c"), F.col("s.customer_id") == F.col("c.customer_id"), "inner")
    .join(dim_product.alias("p"), F.col("s.product_id") == F.col("p.product_id"), "inner")
    .join(dim_date.alias("d"), F.col("s.sale_date") == F.col("d.date"), "inner")
    .select(
        F.col("s.sale_id"),
        F.col("c.customer_sk"),
        F.col("p.product_sk"),
        F.col("d.date_sk"),
        F.col("s.quantity").cast("int").alias("quantity"),
        F.col("s.total_amount").cast("double").alias("total_amount")
    )
)

fact_sales.createOrReplaceTempView("fact_sales_vw")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DROP TABLE IF EXISTS dim_product")
spark.sql("DROP TABLE IF EXISTS dim_customer")
spark.sql("DROP TABLE IF EXISTS dim_date")
spark.sql("DROP TABLE IF EXISTS fact_sales")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_customer.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("dim_customer")
dim_product.write.mode("overwrite").format("delta").saveAsTable("dim_product")
dim_date.write.mode("overwrite").format("delta").saveAsTable("dim_date")
fact_sales.write.mode("overwrite").format("delta").saveAsTable("fact_sales")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
