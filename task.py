from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime

def task_1(**kwargs):
    try:
         print("Running Task 1")
        #raise Exception("Task 1 Failed")  # Simulating a task failure
    except Exception as e:
        ti = kwargs['ti']
        ti.xcom_push(key='task_1_status', value='failed')
        raise e

def decide_next_task(**kwargs):
    ti = kwargs['ti']
    task_1_status = ti.xcom_pull(task_ids='task_1', key='task_1_status')
    print("2222222222222;",task_1_status)
    if task_1_status == 'failed':
        return 'task_2'
    else:
        return 'task_3'

def task_2():
    print("Running Task 2")

def task_3():
    print("Running Task 3")

def task_4():
    print("Running Task 4")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

dag = DAG(
    'sample_dag',
    default_args=default_args,
    schedule_interval=None,
)

task_1_result = PythonOperator(
    task_id='task_1',
    python_callable=task_1,
    provide_context=True,
    dag=dag,
)

branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=decide_next_task,
    provide_context=True,
    trigger_rule='all_done',
    dag=dag,
)

task_2_result = PythonOperator(
    task_id='task_2',
    python_callable=task_2,
    dag=dag,
)

task_3_result = PythonOperator(
    task_id='task_3',
    python_callable=task_3,
    dag=dag,
)

task_4_result = PythonOperator(
    task_id='task_4',
    python_callable=task_4,
    dag=dag,
)

# Define task dependencies based on your logic
task_1_result >> branch_task
branch_task >> task_2_result
branch_task >> task_3_result >> task_4_result
