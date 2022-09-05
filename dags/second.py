import os
import datetime as dt 
import pandas as pd 
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from utils import *

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 7, 14),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
    dag_id='titanic_pivot_2',
    schedule_interval=None,
    default_args=args,
) as dag:
    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
        dag=dag,
    )

    create_titanic_dataset = PythonOperator(
        task_id='create_titanic_dataset',
        python_callable=download_titanic_dataset,
        provide_context=True,
        dag=dag,
    )

    pivot_titanic_dataset = PythonOperator(
        task_id='pivot_titanic_dataset',
        python_callable=pivot_dataset,
        provide_context=True,
        dag=dag,
    )

    mean_fares_titanic_dataset = PythonOperator(
        task_id='mean_fares_titanic_dataset',
        python_callable=mean_fare_per_class,
        provide_context=True,
        dag=dag,
    )

    last_task = BashOperator(
        task_id='last_task',
        bash_command='echo "Pipeline finished! Execution date is {{ execution_date  }}"',
        dag=dag,
    )

first_task >> create_titanic_dataset >> pivot_titanic_dataset >> last_task
create_titanic_dataset >> mean_fares_titanic_dataset >> last_task
