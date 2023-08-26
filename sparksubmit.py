from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_scala_job',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Schedule the DAG to run daily
)

# Define the spark-submit command to run locally
# Use the appropriate path to your Spark installation
spark_submit_cmd = (
    '/opt/homebrew/bin/spark-submit'
    '--class dataframe'  # Replace with your main class name
    '/Users/manojreddy/Downloads/spark/src/main/scala/dataframe.scala'  # Replace with the path to your Scala script
)

# Use the BashOperator to execute the spark-submit command
submit_spark_job = BashOperator(
    task_id='submit_spark_job',
    bash_command=spark_submit_cmd,
    dag=dag,
)



