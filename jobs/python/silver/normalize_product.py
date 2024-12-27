from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from jobs.python.fashion_campus_common import bronze_path, silver_path


def normalize(spark, input_path, output_path):
    product_df = spark.read.csv(input_path, header=True, inferSchema=True)

    normalized_product_df = product_df.select(
        col("id").alias("product_id"),
        col("gender"),
        col("masterCategory").alias("master_category"),
        col("subCategory").alias("sub_category"),
        col("articleType").alias("article_type"),
        col("baseColour").alias("base_color"),
        col("season").alias("season"),
        col("year").alias("year"),
        col("productDisplayName").alias("product_display_name"),
    )

    normalized_product_df.write.mode("overwrite").format("csv").option("header", "true").save(output_path)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Silver Zone Product").getOrCreate()
    normalize(spark, f"{bronze_path}product/*.csv", f"{silver_path}product")
    spark.stop()
