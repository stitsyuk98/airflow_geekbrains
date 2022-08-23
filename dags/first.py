import os
import datetime as dt 
import pandas as pd 
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 7, 14),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)

def download_titanic_dataset():
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    df.to_csv(get_path('titanic.csv'), encoding='utf-8')

def pivot_dataset():
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()
    df.to_csv(get_path('titanic_pivot.csv'))

def mean_fare_per_class():
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    print(titanic_df)
    #расчитывает среднюю арифметическую цену билета (Fare) для каждого класса (Pclass)
    titanic_mean_df = titanic_df.groupby(['Pclass']).mean()
    print(titanic_mean_df)
    titanic_mean_df.to_csv(get_path('titanic_mean_fares.csv'))

with DAG(
    dag_id='titanic_pivot',
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
        dag=dag,
    )

    pivot_titanic_dataset = PythonOperator(
        task_id='pivot_titanic_dataset',
        python_callable=pivot_dataset,
        dag=dag,
    )

    mean_fares_titanic_dataset = PythonOperator(
        task_id='mean_fares_titanic_dataset',
        python_callable=mean_fare_per_class,
        dag=dag,
    )

    last_task = BashOperator(
        task_id='last_task',
        bash_command='echo "Pipeline finished! Execution date is {{ execution_date  }}"',
        dag=dag,
    )

first_task >> create_titanic_dataset >> pivot_titanic_dataset >> last_task
create_titanic_dataset >> mean_fares_titanic_dataset >> last_task
