import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum, min, max, when

from jobs.python.fashion_campus_common import silver_path, gold_path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, min, max


def load(spark, customer_path, transaction_path, output_path):
    # Read the input data
    customer_df = spark.read.csv(customer_path, header=True, inferSchema=True)
    transaction_df = spark.read.csv(transaction_path, header=True, inferSchema=True)

    # Join the datasets
    user_transaction = transaction_df.join(
        customer_df,
        transaction_df["customer_id"] == customer_df["customer_id"],
        "inner"
    ).drop(customer_df["customer_id"])  # Remove the duplicate `customer_id` column from customer_df

    # Perform aggregation
    result = user_transaction.groupBy("customer_id").agg(
        count("*").alias("total_orders"),
        sum("total_amount").alias("total_spent"),
        min("transaction_date").alias("first_order_date"),
        max("transaction_date").alias("last_order_date")
    )

    # Calculate shortest order interval
    result = result.withColumn(
        "shortest_order_interval",
        (col("last_order_date").cast("long") - col("first_order_date").cast("long")) / col("total_orders")
    )

    # Write the result to the output path
    result.write.mode("overwrite").csv(output_path)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Gold Zone User Transaction").getOrCreate()

    # args = sys.argv[1:]
    # silver_path = args[0]
    # gold_path = int(args[1])

    load(
        spark,
        f"{silver_path}customer/*.csv",
        f"{silver_path}transaction/*.csv",
        f"{gold_path}user_transaction_product"
    )

    spark.stop()
