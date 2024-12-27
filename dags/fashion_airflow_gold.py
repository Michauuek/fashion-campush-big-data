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
    dag_id="fashion_gold",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

start_task = PythonOperator(
    task_id="start_task",
    python_callable=lambda: print("Starting Gold Pipeline"),
    dag=dag
)

load_product_transaction_task = SparkSubmitOperator(
    task_id="load_product_transaction_task",
    conn_id="spark-conn",
    application="jobs/python/gold/load_product_transaction.py",
    application_args=["/opt/data/silver/", "/opt/data/gold/"],
    name="LoadProductTransaction",
    dag=dag
)

load_user_clickstream_task = SparkSubmitOperator(
    task_id="load_user_clickstream_task",
    conn_id="spark-conn",
    application="jobs/python/gold/load_user_clickstream.py",
    application_args=["/opt/data/silver/", "/opt/data/gold/"],
    name="LoadUserClickstream",
    dag=dag
)

load_user_transaction_task = SparkSubmitOperator(
    task_id="load_user_transaction_task",
    conn_id="spark-conn",
    application="jobs/python/gold/load_user_transaction.py",
    application_args=["/opt/data/silver/", "/opt/data/gold/"],
    name="LoadUserTransaction",
    dag=dag
)

end_task = PythonOperator(
    task_id="end_task",
    python_callable=lambda: print("Silver ETL Pipeline Completed"),
    dag=dag
)

start_task >> [load_product_transaction_task, load_user_clickstream_task, load_user_transaction_task] >> end_task
