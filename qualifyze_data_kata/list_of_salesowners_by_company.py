from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    collect_list,
    array_distinct,
    array_sort,
    concat_ws,
    split,
    col,
    regexp_replace,
    trim,
    lower,
    explode,
)

# Crear una sesión de Spark
spark = (
    SparkSession.builder.appName("CompanySalesowners").master("local[*]").getOrCreate()
)

# Cargar el archivo CSV de orders
orders_df = spark.read.csv(
    "./resources/orders.csv", header=True, inferSchema=True, sep=";", escape='"'
)

orders_df = orders_df.withColumn(
    "company_name_clean",
    lower(
        regexp_replace(
            trim(regexp_replace(col("company_name"), r"(S.A.|SA|\s+)", " ")),
            " ",
            "_",
        )
    ),
)

orders_df.createOrReplaceTempView("orders")

consolidated_companies_df = spark.sql(
    """
    SELECT
        min(company_id) as company_id,
        company_name_clean,
        collect_set(company_name) as company_names
    FROM orders
    GROUP BY company_name_clean
"""
)

orders_df = (
    orders_df.withColumn(
        "salesowners", split(regexp_replace(col("salesowners"), r"\s*,\s*", ","), ",")
    )
    .withColumn("salesowner", explode(col("salesowners")))
    .groupBy("company_name_clean")
    .agg(
        array_sort(array_distinct(collect_list("salesowner"))).alias(
            "sorted_salesowners"
        )
    )
)

final_df = (
    consolidated_companies_df.join(orders_df, "company_name_clean")
    .withColumn("list_salesowners", concat_ws(",", col("sorted_salesowners")))
    .select(
        col("company_id"),
        col("company_name_clean").alias("company_name"),
        col("list_salesowners"),
    )
)

final_df.show(truncate=False)

spark.stop()
