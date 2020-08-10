import datetime
import logging

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

dag = DAG(
    'redshift_create_copy',
    start_date=datetime.datetime.now()
)

create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql="""
    drop table stores;
    CREATE TABLE IF NOT EXISTS stores (
Store_Number int,
Store_Name varchar,
Address varchar,
City varchar,
Zip_Code int,
Median_income int,
Population int
);
"""
)

copy_task = PostgresOperator(
    task_id="insert_data",
    dag=dag,
    postgres_conn_id="redshift",
    sql="""
    COPY stores
    from 's3://dend-buket-fujimoto2/capstone/pandas_stores.csv'
    iam_role 'arn:aws:iam::945626161665:role/dwhRole'
    csv;
"""
)  


create_table >> copy_task