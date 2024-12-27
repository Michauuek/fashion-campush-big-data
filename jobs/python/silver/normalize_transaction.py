from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from jobs.python.fashion_campus_common import bronze_path, silver_path


def normalize(spark, input_path, output_path):
    transaction_df = spark.read.csv(input_path, header=True, inferSchema=True)

    normalized_transaction_df = transaction_df.select(
        col("created_at").alias("transaction_date"),
        col("customer_id"),
        col("payment_method"),
        col("payment_status"),
        col("session_id"),
        col("quantity"),
        col("shipment_location_lat").alias("shipment_lat"),
        col("shipment_location_long").alias("shipment_long"),
        col("booking_id"),
        col("item_price"),
        col("total_amount"),
        col("product_id"),
    )

    normalized_transaction_df.write.mode("overwrite").format("csv").option("header", "true").save(output_path)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Silver Zone Transaction").getOrCreate()
    normalize(spark, f"{bronze_path}transaction_new/*.csv", f"{silver_path}transaction")
    spark.stop()
