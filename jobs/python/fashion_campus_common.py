import os

bronze_path = "/opt/data/bronze/"
silver_path = "/opt/data/silver/"
gold_path = "/opt/data/gold/"


def get_csv_files_from_folder(folder_path):
    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"The folder {folder_path} does not exist.")

    csv_files = []

    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".csv"):
                full_path = os.path.join(root, file)
                csv_files.append(full_path)

    return csv_files


# def main():
    # spark = SparkSession.builder.appName("Silver Zone ETL").getOrCreate()
    # products_csv_bronze = get_csv_files_from_folder(bronze_path + "product/")
    #
    # for product_csv in products_csv_bronze:
    #     normalize_product_data(spark, product_csv, silver_path + "product/")

    # normalize_transaction_data(spark, f"{bronze_path}transaction_new", f"{silver_path}transaction")
    # normalize_customer_data(spark, f"{bronze_path}customer", f"{silver_path}customer")
    # normalize_clickstream_data(spark, f"{bronze_path}click_stream_new", f"{silver_path}click_stream")
