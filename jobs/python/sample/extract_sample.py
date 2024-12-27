import sys

from pyspark.sql import SparkSession


def extract(spark, input_path, output_path, fraction=0.1):
    large_df = spark.read.csv(input_path, header=True, inferSchema=True)
    sampled_df = large_df.sample(fraction=fraction, seed=42)
    sampled_df.write.csv(output_path, header=True)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("CSV Sampling").getOrCreate()
    # args = sys.argv[1:]
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    fraction = float(sys.argv[3]) if len(sys.argv) > 3 else 0.1
    extract(spark, input_path, output_path, fraction)
    spark.stop()

