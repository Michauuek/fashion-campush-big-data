from pyspark.sql import SparkSession
import sys


def main(args):
    if len(args) < 3:
        print("Usage: csv_to_bronze.py <input_path> <partition_size> <output_path>")
        sys.exit(1)

    input_csv_path = args[0]
    partition_size = int(args[1])
    bronze_stage_path = args[2]

    spark = SparkSession.builder \
        .appName("CSV to Bronze Stage") \
        .getOrCreate()

    df = spark.read.csv(input_csv_path, header=True, inferSchema=True)

    repartitioned_df = df.repartition(partition_size)

    repartitioned_df.write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(bronze_stage_path)

    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
