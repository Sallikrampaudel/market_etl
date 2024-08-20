from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from market_etl import run_market_etl


default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2024, 8, 20),
    'email' : ['airflow@example.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries': 0,
    'retry_delay' : timedelta(minutes=1)
}

dag= DAG(
    'market_dag',
    default_args=default_args,
    description = 'First_etl_code'
)

start = PythonOperator(
    task_id = 'start',
    python_callable = run_market_etl,
    dag = dag,
)

run_etl = PythonOperator(
    task_id = 'api_to_s3',
    python_callable = run_market_etl,
    dag = dag,
)

start >> run_etl
