# import libraries
from datetime import datetime, timedelta
from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import re
import tarfile

# Define the DAG arguments
default_args={
    'owner':'Dang Nhat',
    'start_date': days_ago(0),
    'email':['dangnhatsimon@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries':1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    dag_id='process_web_log',
    default_args=default_args,
    description='Data Pipelines Using Apache AirFlow',
    schedule_interval='* * * * *'
)

# Create a task to extract data
access_log_file = '/home/project/airflow/dags/capstone/accesslog.txt'
extracted_data_file = '/home/project/airflow/dags/capstone/extracted_data.txt'
pattern = r'(?:\d{1,3}\.){3}\d{1,3}'
def extract_data(input_file, output_file):
    """
    Extracts IP addresses from the input file and writes them to the output file.

    :param input_file: Input file path
    :param output_file: Output file path
    """
    try:
        with open(input_file,'r') as in_f,\
            open(output_file, 'w') as out_f:
            for line in in_f.readlines():
                match = re.match(pattern,line)
                if match:
                    out_f.write(match.group(0)+ '\n')
    except FileNotFoundError:
        print(f"Error: File not found - {input_file}")
    except Exception as e:
        print(f"Error: {str(e)}")

extract_data_task = PythonOperator(
    task_id = 'extract_data',
    python_callable=extract_data,
    provide_context=True,
    op_kwargs={'input_file':access_log_file, 'output_file':extracted_data_file},
    dag=dag
)

# Create a task to transform the data in the txt file
transform_data_file = '/home/project/airflow/dags/capstone/transformed_data.txt'
def transform_data(extract_file, transform_file):
    """
    Transforms data from the extract file and writes the result to the transform file.

    :param extract_file: Extracted data file path
    :param transform_file: Transformed data file path
    """
    try:
        with open(extract_file,'r') as ex_f, \
            open (transform_file,'w') as tr_f:
            for line in ex_f.readlines():
                match = re.search('198.46.149.143', line)
                if match:
                    tr_f.write(match.group(0)+ '\n')
    except FileNotFoundError:
        print(f"Error: File not found - {extract_file}")
    except Exception as e:
        print(f"Error: {str(e)}")

transform_data_task = PythonOperator(
    task_id = 'transform_data',
    python_callable=transform_data,
    provide_context=True,
    op_kwargs={'extract_file':extracted_data_file, 'transform_file':transform_data_file},
    dag=dag
)

# Create a task to load the data
archive_data_file = '/home/project/airflow/dags/capstone/weblog.tar'
def load_data(file_load,tar_file):
    """
    Archives a file and creates a tar.gz file.

    :param file_load: File to be archived
    :param tar_file: Output tar.gz file path
    """
    try:
        with tarfile.open(tar_file, 'w|gz') as tar:
            tar.add(file_load)
    except FileNotFoundError:
        print(f"Error: File not found - {file_load}")
    except Exception as e:
        print(f"Error: {str(e)}")

load_data_task = PythonOperator(
    task_id = 'load_data',
    python_callable=load_data,
    op_kwargs={'file_load':transform_data_file, 'tar_file':archive_data_file},
    dag=dag
)

# Define the task pipeline
extract_data_task >> transform_data_task >> load_data_task