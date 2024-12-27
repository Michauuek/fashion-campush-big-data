import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum, min, max, when

from jobs.python.fashion_campus_common import silver_path, gold_path


def load(spark, clickstream_path, transaction_path, customer_path,  output_path):
    clickstream_df = spark.read.csv(clickstream_path, header=True, inferSchema=True)
    transaction_df = spark.read.csv(transaction_path, header=True, inferSchema=True)
    customer_df = spark.read.csv(customer_path, header=True, inferSchema=True)

    clickstream_transaction = transaction_df.join(
        clickstream_df,
        transaction_df["session_id"] == clickstream_df["session_id"],
        "inner"
    ).drop(transaction_df["session_id"])

    clickstream_transaction_customer = clickstream_transaction.join(
        customer_df,
        clickstream_transaction["customer_id"] == customer_df["customer_id"],
        "inner"
    ).drop("customer_id").drop("quantity")

    mobile_clickstream = clickstream_transaction_customer.filter(col("traffic_source") == "MOBILE")

    result = mobile_clickstream.groupBy("customer_id").agg(
        count("session_id").alias("total_sessions"),
        avg("session_duration").alias("avg_session_time"),
        count(when(col("event_name"), "click")).alias("total_clicks"),
        avg("quantity").alias("avg_quantity_per_session"),
        (sum("quantity") / count("session_id")).alias("conversion_rate")
    )

    result.write.mode("overwrite").csv(output_path)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Gold Zone Click Stream").getOrCreate()

    # args = sys.argv[1:]
    # silver_path = args[0]
    # gold_path = int(args[1])

    load(
        spark,
        f"{silver_path}click_stream/*.csv",
        f"{silver_path}transaction/*.csv",
        f"{silver_path}customer/*.csv",
        f"{gold_path}user_stream_click"
    )

    spark.stop()
