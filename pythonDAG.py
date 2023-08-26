from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def test_function(name,age):
    print(f"My name is manoj,and i am 23 years old")

my_dag = DAG(
    'python_dag',
    start_date=datetime(2023, 7, 14),
    schedule_interval='@daily'
)

python_task = PythonOperator(
    task_id='python_task',
    python_callable=test_function,
    op_args=['jack', 32],
    dag=my_dag
)