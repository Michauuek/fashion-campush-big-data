from pyspark.sql import SparkSession

from jobs.python.fashion_campus_common import silver_path, gold_path
from pyspark.sql.functions import col, count, avg, sum, min, max, when


def load(spark, clickstream_path, transaction_path, customer_path, output_path):
    clickstream_df = spark.read.csv(clickstream_path, header=True, inferSchema=True)
    transaction_df = spark.read.csv(transaction_path, header=True, inferSchema=True)
    customer_df = spark.read.csv(customer_path, header=True, inferSchema=True)

    clickstream_df = clickstream_df.withColumn("event_time", col("event_time").cast("timestamp"))

    session_duration_df = clickstream_df.groupBy("session_id").agg(
        (max("event_time").cast("long") - min("event_time").cast("long")).alias("session_duration")
    )

    clickstream_transaction = transaction_df.join(
        clickstream_df,
        transaction_df["session_id"] == clickstream_df["session_id"],
        "inner"
    ).drop(transaction_df["session_id"])

    clickstream_transaction_customer = clickstream_transaction.join(
        customer_df,
        clickstream_transaction["customer_id"] == customer_df["customer_id"],
        "inner"
    ).drop(clickstream_transaction["customer_id"])

    enriched_data = clickstream_transaction_customer.join(
        session_duration_df,
        "session_id",
        "inner"
    )

    mobile_clickstream = enriched_data.filter(col("traffic_source") == "MOBILE")

    result = mobile_clickstream.groupBy("customer_id").agg(
        count("session_id").alias("total_sessions"),
        avg("session_duration").alias("avg_session_time_seconds"),
        count(when(col("event_name") == "CLICK", 1)).alias("total_clicks"),
        avg("cart_quantity").alias("avg_quantity_per_session"),
        sum(when(col("event_name") == "ADD_TO_CART", col("cart_quantity")).otherwise(0)).alias("total_added_to_cart"),
        sum(when(col("event_name") == "BOOKING", col("cart_quantity")).otherwise(0)).alias("total_booked"),
    ).withColumn("abandoned_carts", col("total_added_to_cart") - col("total_booked"))

    result.write.mode("overwrite").format("csv").option("header", "true").save(output_path)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Gold Zone Click Stream").getOrCreate()

    load(
        spark,
        f"{silver_path}click_stream/*.csv",
        f"{silver_path}transaction/*.csv",
        f"{silver_path}customer/*.csv",
        f"{gold_path}user_stream_click"
    )

    spark.stop()
