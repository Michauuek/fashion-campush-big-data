import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Michal",
    "start_date": airflow.utils.dates.days_ago(1),
    "retries": 1
}

dag = DAG(
    dag_id="fashion_silver",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

start_task = PythonOperator(
    task_id="start_task",
    python_callable=lambda: print("Starting Silver Pipeline"),
    dag=dag
)

normalize_product_task = SparkSubmitOperator(
    task_id="normalize_product_task",
    conn_id="spark-conn",
    application="jobs/python/silver/normalize_product.py",
    application_args=["/opt/data/bronze/product", "/opt/data/silver/product"],
    name="NormalizeProductData",
    dag=dag
)

normalize_transaction_task = SparkSubmitOperator(
    task_id="normalize_transaction_task",
    conn_id="spark-conn",
    application="jobs/python/silver/normalize_transaction.py",
    application_args=["/opt/data/bronze/transaction_new", "/opt/data/silver/transaction"],
    name="NormalizeTransactionData",
    dag=dag
)

normalize_customer_task = SparkSubmitOperator(
    task_id="normalize_customer_task",
    conn_id="spark-conn",
    application="jobs/python/silver/normalize_customer.py",
    application_args=["/opt/data/bronze/customer", "/opt/data/silver/customer"],
    name="NormalizeCustomerData",
    dag=dag
)

normalize_clickstream_task = SparkSubmitOperator(
    task_id="normalize_clickstream_task",
    conn_id="spark-conn",
    application="jobs/python/silver/normalize_clickstream.py",
    application_args=["/opt/data/bronze/click_stream_new", "/opt/data/silver/click_stream"],
    name="NormalizeClickStreamData",
    dag=dag
)

end_task = PythonOperator(
    task_id="end_task",
    python_callable=lambda: print("Silver ETL Pipeline Completed"),
    dag=dag
)

# start_task >> [normalize_product_task] >> end_task
start_task >> [normalize_product_task, normalize_transaction_task, normalize_customer_task, normalize_clickstream_task] >> end_task
