from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Define default_args for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Create the DAG instance
dag = DAG(
    "weekday_tasks",
    default_args=default_args,
    schedule_interval="0 0 * * 1-3",  # Run on Monday, Tuesday, and Wednesday at midnight
    catchup=False,
)

# Define functions for the tasks
def monday_task():
    print("Monday task running!")

def tuesday_task():
    print("Tuesday task running!")

def wednesday_task():
    print("Wednesday task running!")

# Create task instances
start_task = DummyOperator(task_id="start_task", dag=dag)

monday_task = PythonOperator(
    task_id="monday_task",
    python_callable=monday_task,
    dag=dag,
)

tuesday_task = PythonOperator(
    task_id="tuesday_task",
    python_callable=tuesday_task,
    dag=dag,
)

wednesday_task = PythonOperator(
    task_id="wednesday_task",
    python_callable=wednesday_task,
    dag=dag,
)

# Set task dependencies
start_task >> [monday_task, tuesday_task, wednesday_task]
