import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id = "fashion_flow",
    default_args = {
        "owner": "Michal Rz",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

repartitioning_job_product = SparkSubmitOperator(
    task_id="repartitioning_job_product",
    conn_id="spark-conn",
    application="jobs/python/fashion_campus_repartitioning.py",
    application_args=["/opt/data/source/sample/product.csv", "1", "/opt/data/bronze/product"],
    dag=dag
)

repartitioning_job_customer = SparkSubmitOperator(
    task_id="repartitioning_job_customer",
    conn_id="spark-conn",
    application="jobs/python/fashion_campus_repartitioning.py",
    application_args=["/opt/data/source/sample/customer.csv", "4", "/opt/data/bronze/customer"],
    dag=dag
)

repartitioning_job_click_stream_new = SparkSubmitOperator(
    task_id="repartitioning_job_click_stream_new",
    conn_id="spark-conn",
    application="jobs/python/fashion_campus_repartitioning.py",
    application_args=["/opt/data/source/sample/click_stream_new.csv", "4", "/opt/data/bronze/click_stream_new"],
    dag=dag
)

repartitioning_job_transaction_new = SparkSubmitOperator(
    task_id="repartitioning_job_transaction_new",
    conn_id="spark-conn",
    application="jobs/python/fashion_campus_repartitioning.py",
    application_args=["/opt/data/source/sample/transaction_new.csv", "4", "/opt/data/bronze/transaction_new"],
    dag=dag
)

python_job = SparkSubmitOperator(
    task_id="python_job",
    conn_id="spark-conn",
    application="jobs/python/wordcountjob.py",
    dag=dag
)


end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> python_job >> repartitioning_job_product >> repartitioning_job_customer >> repartitioning_job_click_stream_new >> repartitioning_job_transaction_new >> end