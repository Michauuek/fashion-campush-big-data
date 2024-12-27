from jobs.python.fashion_campus_common import silver_path, gold_path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, min, max


def load(spark, customer_path, transaction_path, output_path):
    customer_df = spark.read.csv(customer_path, header=True, inferSchema=True)
    transaction_df = spark.read.csv(transaction_path, header=True, inferSchema=True)

    user_transaction = transaction_df.join(
        customer_df,
        transaction_df["customer_id"] == customer_df["customer_id"],
        "inner"
    ).drop(customer_df["customer_id"])

    result = user_transaction.groupBy("customer_id").agg(
        count("*").alias("total_orders"),
        sum("total_amount").alias("total_spent"),
        min("transaction_date").alias("first_order_date"),
        max("transaction_date").alias("last_order_date")
    )

    result = result.withColumn(
        "shortest_order_interval",
        (col("last_order_date").cast("long") - col("first_order_date").cast("long")) / col("total_orders")
    )

    result.write.mode("overwrite").csv(output_path).format("csv").option("header", "true").save(output_path)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Gold Zone User Transaction").getOrCreate()

    load(
        spark,
        f"{silver_path}customer/*.csv",
        f"{silver_path}transaction/*.csv",
        f"{gold_path}user_transaction_product"
    )

    spark.stop()
