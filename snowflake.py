from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'snowflake_data_warehouse_dag',
    default_args=default_args,
    schedule_interval='@once',  # Run the DAG only once
)

# Snowflake connection ID in Airflow
snowflake_conn_id = 'snowflake_conn'

with dag:

    # Step 1: Create Data Warehouse
    create_dw_task = SnowflakeOperator(
        task_id='create_data_warehouse',
        sql='CREATE WAREHOUSE IF NOT EXISTS my_warehouse;',
        snowflake_conn_id=snowflake_conn_id,
    )

    # Step 2: Create Database
    create_db_task = SnowflakeOperator(
        task_id='create_database',
        sql='CREATE DATABASE IF NOT EXISTS my_database;',
        snowflake_conn_id=snowflake_conn_id,
    )

    # Step 3: Create Table
    create_table_task = SnowflakeOperator(
        task_id='create_table',
        sql='''CREATE TABLE IF NOT EXISTS my_database.public.students_subjects (
                student_id INTEGER,
                student_name STRING,
                subject STRING,
                score INTEGER
            );''',
        snowflake_conn_id=snowflake_conn_id,
    )

    # Step 4: Load Data
    load_data_task = SnowflakeOperator(
        task_id='load_data',
        sql='''INSERT INTO my_database.public.students_subjects
                VALUES (1, 'Alice', 'Math', 90),
                       (2, 'Bob', 'Math', 85),
                       (3, 'Charlie', 'Science', 78);''',
        snowflake_conn_id=snowflake_conn_id,
    )

    # Set up task dependencies
    create_dw_task >> create_db_task >> create_table_task >> load_data_task
