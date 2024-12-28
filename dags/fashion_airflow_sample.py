import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id="fashion_sample",
    default_args={
        "owner": "Michal",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval="@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag
)

sampling_job_product = SparkSubmitOperator(
    task_id="sampling_job_product",
    conn_id="spark-conn",
    application="jobs/python/sample/extract_sample.py",
    application_args=["/opt/data/source/full/product.csv", "/opt/data/source/sample/product.csv", "1"],
    dag=dag
)

sampling_job_customer = SparkSubmitOperator(
    task_id="sampling_job_customer",
    conn_id="spark-conn",
    application="jobs/python/sample/extract_sample.py",
    application_args=["/opt/data/source/full/customer.csv", "/opt/data/source/sample/customer.csv", "1"],
    dag=dag
)

sampling_job_click_stream_new = SparkSubmitOperator(
    task_id="sampling_job_click_stream_new",
    conn_id="spark-conn",
    application="jobs/python/sample/extract_sample.py",
    application_args=["/opt/data/source/full/click_stream_new.csv", "/opt/data/source/sample/click_stream_new.csv", "0.01"],
    dag=dag
)

sampling_job_transaction_new = SparkSubmitOperator(
    task_id="sampling_job_transaction_new",
    conn_id="spark-conn",
    application="jobs/python/sample/extract_sample.py",
    application_args=["/opt/data/source/full/transaction_new.csv", "/opt/data/source/sample/transaction_new.csv", "0.1"],
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> sampling_job_product >> sampling_job_customer >> sampling_job_click_stream_new >> sampling_job_transaction_new >> end
