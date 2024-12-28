import os
import shutil
import sys

from pyspark.sql import SparkSession


def extract(spark, input_path, output_path, fraction=0.1):
    large_df = spark.read.csv(input_path, header=True, inferSchema=True)
    sampled_df = large_df.sample(fraction=fraction, seed=42)
    sampled_df = sampled_df.coalesce(1)
    rename_and_save(sampled_df, output_path)


def rename_and_save(sampled_df, output_path):
    temp_dir = "/opt/data/source/temp"
    sampled_df.write.csv(temp_dir, header=True, mode="overwrite")
    part_file = [f for f in os.listdir(temp_dir) if f.startswith("part-")][0]
    os.rename(f"{temp_dir}/{part_file}", output_path)
    shutil.rmtree(temp_dir)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("CSV Sampling").getOrCreate()
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    fraction = float(sys.argv[3])
    extract(spark, input_path, output_path, fraction)
    spark.stop()
