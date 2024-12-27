from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from jobs.python.fashion_campus_common import bronze_path, silver_path


def normalize(spark, input_path, output_path):
    customer_df = spark.read.csv(input_path, header=True, inferSchema=True)

    normalized_customer_df = customer_df.select(
        col("customer_id").alias("customer_id"),
        col("gender"),
        col("home_location").alias("city"),
        col("home_country").alias("country"),
        col("birthdate"),
        col("first_join_date"),
        col("device_type")
    )

    normalized_customer_df.write.mode("overwrite").format("csv").option("header", "true").save(output_path)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Silver Zone Customer").getOrCreate()
    normalize(spark, f"{bronze_path}customer/*.csv", f"{silver_path}customer")
    spark.stop()
