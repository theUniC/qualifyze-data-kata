from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.builder.getOrCreate()

# A CSV dataset is pointed to by path.
# The path can be either a single CSV file or a directory of CSV files
path = "./resources/orders.csv"

df = spark.read.csv(path, header=True, inferSchema=True, sep=";", escape='"')
df.show()

df_by_audit_type = (
    df.groupBy("company_id", "audit_type")
    .agg(count("*").alias("by_audit_type"))
    .orderBy("company_id")
)

df_by_audit_type.show()

spark.stop()
