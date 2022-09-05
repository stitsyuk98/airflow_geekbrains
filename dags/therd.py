import os
import datetime as dt 
import pandas as pd 
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable


def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)

@dag(schedule_interval=None, start_date=dt.datetime(2022, 7, 14))
def titanic_pivot_3():

    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"'
    )

    @task(task_id='download_titanic_dataset')
    def download_titanic_dataset():
        url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
        df = pd.read_csv(url)
        result = df.to_json(orient="index")
        return result

    @task(multiple_outputs=True)
    def pivot_dataset(tatan: dict):
        titanic_df = pd.read_json(tatan, orient='index')
        df = titanic_df.pivot_table(index=['Sex'],
                                    columns=['Pclass'],
                                    values='Name',
                                    aggfunc='count').reset_index()
        # df.to_csv(get_path('titanic_pivot.csv'))
        print(df.dtypes)
        print(df)

        table_name = Variable.get('pivot_dataset')
        pg = PostgresHook(conn='postgres_default')
        pg.run(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            sex varchar(50),
            first integer,
            second integer,
            therd integer
        );
        """)
        rows = list(df.itertuples(index=False, name=None))
        pg.insert_rows(table=table_name, rows=rows)
        return {}

    @task(multiple_outputs=True)
    def mean_fare_per_class(tatan: dict):
        titanic_df = pd.read_json(tatan, orient='index')
        print(titanic_df)
        #расчитывает среднюю арифметическую цену билета (Fare) для каждого класса (Pclass)
        titanic_mean_df = titanic_df.groupby(['Pclass']).mean()
        print(titanic_mean_df)
        print(titanic_mean_df.dtypes)

        table_name = Variable.get('mean_fare_per_class')
        # titanic_mean_df.to_csv(get_path('titanic_mean_fares.csv'))
        pg = PostgresHook(conn='postgres_default')
        pg.run(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            survived float,
            age float,
            siblings    float,
            parents    float,
            fare float
        );
        """)
        rows = list(titanic_mean_df.itertuples(index=False, name=None))
        pg.insert_rows(table=table_name, rows=rows)
        return {}

    last_task = BashOperator(
        task_id='last_task',
        bash_command='echo "Pipeline finished! Execution date is {{ execution_date  }}"'
    )

    titanic_dataset = download_titanic_dataset()
    first_task >> titanic_dataset
    [pivot_dataset(titanic_dataset), mean_fare_per_class(titanic_dataset)] >> last_task

dag = titanic_pivot_3()