from airflow.providers.postgres.hooks.postgres import PostgresHook
import os, json
import datetime as dt 
import pandas as pd 
from airflow.models import Variable

def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)

def download_titanic_dataset(ti, **kwargs):
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    # df.to_csv(get_path('titanic.csv'), encoding='utf-8')
    result = df.to_json(orient="index")
    parsed = json.loads(result)
    ti.xcom_push(value=parsed, key='titanic')

def pivot_dataset(ti, **kwargs):
    # titanic_df = pd.read_csv(get_path('titanic.csv'))
    tit = ti.xcom_pull(key='titanic')
    tatan = json.dumps(tit)
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


def mean_fare_per_class(ti, **kwargs):
    # titanic_df = pd.read_csv(get_path('titanic.csv'))
    tit = ti.xcom_pull(key='titanic')
    tatan = json.dumps(tit)
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
