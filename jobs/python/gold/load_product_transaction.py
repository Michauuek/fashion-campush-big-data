from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum, min, max, when, first

from jobs.python.fashion_campus_common import silver_path, gold_path


def load(spark, product_path, transaction_path, output_path):
    product_df = spark.read.csv(product_path, header=True, inferSchema=True)
    transaction_df = spark.read.csv(transaction_path, header=True, inferSchema=True)

    summer_products = product_df.filter(col("season") == "SUMMER")

    product_transaction = transaction_df.join(
        summer_products,
        transaction_df.product_id == summer_products.product_id,
        "inner"
    ).drop(transaction_df["product_id"])

    result = product_transaction.groupBy("product_id").agg(
        count("*").alias("total_sales_count"),
        avg("total_amount").alias("avg_order_value"),
        sum("total_amount").alias("total_revenue"),
        avg("quantity").alias("avg_quantity"),
        first("master_category").alias("master_category")
    )

    result.write.mode("overwrite").format("csv").option("header", "true").save(output_path)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Gold Zone Product Transaction").getOrCreate()

    # args = sys.argv[1:]
    # silver_path = args[0]
    # gold_path = int(args[1])

    load(
        spark,
        f"{silver_path}product/*.csv",
        f"{silver_path}transaction/*.csv",
        f"{gold_path}product_transaction"
    )

    spark.stop()
