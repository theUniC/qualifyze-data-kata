from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    when,
    col,
    from_json,
    concat,
    lit,
    regexp_replace,
)
from pyspark.sql.types import StructType, StructField, StringType

spark = (
    SparkSession.builder.appName("DataEngineeringAssesment")
    .master("local[*]")
    .getOrCreate()
)

contact_data_schema = StructType(
    [
        StructField("auditor_name", StringType(), nullable=False),
        StructField("auditor_surname", StringType(), nullable=False),
        StructField("city", StringType(), nullable=False),
        StructField("cp", StringType()),
    ]
)

orders_df = spark.read.csv(
    "./resources/orders.csv", header=True, inferSchema=True, sep=";", escape='"'
)

final_df = (
    orders_df.withColumn(
        "contact_data", regexp_replace(col("contact_data"), r"[\[\]]", "")
    )
    .withColumn(
        "contact_data_parsed",
        when(
            col("contact_data").isNotNull(),
            from_json(
                col("contact_data"),
                contact_data_schema,
            ),
        ).otherwise(None),
    )
    .withColumn(
        "auditor_full_name",
        when(
            col("contact_data_parsed").isNotNull(),
            concat(
                col("contact_data_parsed.auditor_name"),
                lit(" "),
                col("contact_data_parsed.auditor_surname"),
            ),
        ).otherwise("Hans Zimmermann"),
    )
    .select("order_id", "auditor_full_name")
)
final_df.show(truncate=False)

spark.stop()
