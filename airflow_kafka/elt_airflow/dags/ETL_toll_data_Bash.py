# import libraries
from datetime import datetime, timedelta
from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# define arguments of DAG
default_args={
    'owner':'Dang Nhat',
    'start_date': days_ago(0),
    'email':['dangnhatsimon@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries':1,
    'retry_delay': timedelta(minutes=5)
}

# define DAG
dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1)
)

# define task
extract_transform_load=BashOperator(
    task_id='extract_transform_load',
    bash_command='chmod +x $AIRFLOW_HOME/dags/Extract_Transform_data.sh; /$AIRFLOW_HOME/dags/Extract_Transform_data.sh',
    dag=dag
)

extract_transform_load