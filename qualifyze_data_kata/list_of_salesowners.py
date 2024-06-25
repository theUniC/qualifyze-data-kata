from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    try_divide,
    lit,
    try_add,
    split,
    regexp_replace,
    posexplode,
    udf,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    DecimalType,
    DoubleType,
)


def calculate_commission(order_position: int, net_value: float) -> float:
    if order_position == 0:
        return net_value * 0.06
    elif order_position == 1:
        return net_value * 0.025
    elif order_position == 2:
        return net_value * 0.0095
    else:
        return 0.0


spark = (
    SparkSession.builder.appName("DataEngineeringAssesment")
    .master("local[*]")
    .getOrCreate()
)

calculate_commission_udf = udf(calculate_commission, DoubleType())

invoicing_data_schema = StructType(
    [
        StructField(
            "data",
            StructType(
                [
                    StructField(
                        "invoices",
                        ArrayType(
                            StructType(
                                [
                                    StructField("id", StringType(), nullable=False),
                                    StructField(
                                        "orderId", StringType(), nullable=False
                                    ),
                                    StructField(
                                        "companyId", StringType(), nullable=False
                                    ),
                                    StructField(
                                        "grossValue", StringType(), nullable=False
                                    ),
                                    StructField("vat", StringType(), nullable=False),
                                ],
                            )
                        ),
                        nullable=False,
                    )
                ]
            ),
            nullable=False,
        )
    ]
)

orders_df = spark.read.csv(
    "./resources/orders.csv", header=True, inferSchema=True, sep=";", escape='"'
)

invoices_df = (
    spark.read.schema(invoicing_data_schema)
    .json("./resources/invoicing_data.json", invoicing_data_schema, multiLine=True)
    .select(explode(col("data.invoices")).alias("invoice"))
    .select(
        col("invoice.id").alias("id"),
        col("invoice.orderId").alias("order_id"),
        col("invoice.companyId").alias("company_id"),
        col("invoice.grossValue").alias("gross_value"),
        col("invoice.vat").alias("vat"),
    )
    .withColumn("gross_value", col("gross_value").cast(DecimalType(10, 2)))
)

salesowners_and_commissions_df = (
    orders_df.join(invoices_df, "order_id")
    .withColumn(
        "net_value",
        try_divide(
            col("gross_value"),
            try_add(lit(1), try_divide(col("vat"), lit(100))),
        ),
    )
    .withColumn(
        "salesowners", split(regexp_replace(col("salesowners"), r"\s*,\s*", ","), ",")
    )
    .withColumn("salesowner", explode(col("salesowners")))
    .select("*", posexplode(col("salesowners")).alias("order_position", "salesowner_2"))
    .withColumn(
        "commission", calculate_commission_udf(col("order_position"), col("net_value"))
    )
    .groupBy("salesowner_2")
    .sum("commission")
    .withColumnRenamed("sum(commission)", "total_commission")
    .orderBy(col("total_commission").desc())
    .withColumn("total_commission", col("total_commission") / 100)
    .withColumn("total_commission", col("total_commission").cast(DecimalType(10, 2)))
)
salesowners_and_commissions_df.show(truncate=False)

spark.stop()
