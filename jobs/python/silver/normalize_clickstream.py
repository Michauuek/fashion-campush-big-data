from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from jobs.python.fashion_campus_common import bronze_path, silver_path


def normalize(spark, input_path, output_path):
    clickstream_df = spark.read.csv(input_path, header=True, inferSchema=True)

    normalized_clickstream_df = clickstream_df.select(
        col("session_id"),
        col("event_name"),
        col("event_time"),
        col("event_id"),
        col("traffic_source"),
        col("product_id"),
        col("quantity").alias("cart_quantity"),
        col("item_price"),
        col("payment_status"),
        col("search_keywords"),
        col("promo_code"),
        col("promo_amount")
    )

    normalized_clickstream_df.write.mode("overwrite").format("csv").option("header", "true").save(output_path)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Silver Zone Click Stream").getOrCreate()
    normalize(spark, f"{bronze_path}click_stream_new/*.csv",  f"{silver_path}click_stream")
    spark.stop()
