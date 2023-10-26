# import libraries
from datetime import datetime, timedelta
from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd

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

# define tasks to unzip data
tolldata='/home/project/airflow/dags/finalassignment/tolldata.tgz'
unzip_data=BashOperator(
    task_id='unzip_data',
    bash_command=f'tar -xvzf {tolldata}',
    dag=dag
)

# define task to extract data from csv file
in_csv_file='/home/project/airflow/dags/finalassignment/vehicle-data.csv'
out_csv_file='/home/project/airflow/dags/finalassignment/csv_data.csv'

def extract_csv(csv_file):
    col_name=[
        'Rowid', 'Timestamp', 'Anonymized Vehicle number','Vehicle type',
        'Number of axles','Vehicle code'
    ]
    custom_date_parser = lambda x: datetime.strptime(x, "%a %b %d %H:%M:%S %Y")
    col_type={
        'Rowid':'int', 'Timestamp':'str', 'Anonymized Vehicle number':'int',
        'Vehicle type':'str', 'Number of axles':'int','Vehicle code':'str'
    }
    df=pd.read_csv(
        csv_file,
        header=None, 
        names=col_name,
        sep=',',
        parse_dates=['Timestamp'],
        date_parser=custom_date_parser,
        dtype=col_type
    )
    csv_data=df.loc[:,['Rowid', 'Timestamp', 'Anonymized Vehicle number','Vehicle type']].copy()
    csv_data.to_csv(out_csv_file,header=True, index=False)

extract_data_from_csv=PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_csv,
    op_kwargs={'csv_file':in_csv_file},
    dag=dag
)

# define task to extract data from tsv file
in_tsv_file='/home/project/airflow/dags/finalassignment/tollplaza-data.tsv'
out_tsv_file='/home/project/airflow/dags/finalassignment/tsv_data.tsv'

def extract_tsv(tsv_file):
    col_name=[
        'Rowid', 'Timestamp', 'Anonymized Vehicle number','Vehicle type',
        'Number of axles','Tollplaza id','Tollplaza code'
    ]
    custom_date_parser = lambda x: datetime.strptime(x, "%a %b %d %H:%M:%S %Y")
    col_type={
        'Rowid':'int', 'Timestamp':'str', 'Anonymized Vehicle number':'int',
        'Vehicle type':'str', 'Number of axles':'int',
        'Tollplaza id':'int','Tollplaza code':'str'
    }
    df=pd.read_table(
        tsv_file,
        header=None, 
        names=col_name,
        sep='\t',
        parse_dates=['Timestamp'],
        date_parser=custom_date_parser,
        dtype=col_type
    )
    tsv_data=df.loc[:,['Number of axles','Tollplaza id','Tollplaza code']].copy()
    tsv_data.to_csv(out_tsv_file,header=True, index=False)

extract_data_from_tsv=PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_tsv,
    op_kwargs={'tsv_file':in_tsv_file},
    dag=dag
)

# define task to extract data from fixed width file
in_fwf_file='/home/project/airflow/dags/finalassignment/payment-data.txt'
out_fwf_file='/home/project/airflow/dags/finalassignment/fixed_width_data.csv'

def extract_data_from_fixed_width(fwf_file):
    col_name=[
        'Rowid', 'dayofweek','month','day','time' ,'year', 'Anonymized Vehicle number',
        'Tollplaza id','Tollplaza code','Type of Payment code','Vehicle Code'
    ]
    col_type={
        'Rowid':'int', 'dayofweek':'str','month':'str','day':'str','time':'str' ,'year':'str', 
        'Anonymized Vehicle number':'int','Tollplaza id':'int','Tollplaza code':'str',
        'Type of Payment code':'str','Vehicle Code':'str'
    }
    df=pd.read_fwf(
        fwf_file,
        header=None,
        names=col_name,
        dtype=col_type
    )
    df.insert(
        loc=1,
        column='Timestamp',
        value=df.iloc[:,1:6].astype(str).apply(lambda x: " ".join(x), axis=1) # df.iloc[:,1:6].agg(" ".join, axis=1)
    )
    df['Timestamp']=pd.to_date(df['Timestamp'],format='%a %b %d %H:%M:%S %Y')
    df.drop(df.loc[:,'dayofweek':'year'], axis=1, inplace=True)
    fwf_data=df.loc[:,['Type of Payment code','Vehicle Code']].copy()
    fwf_data.to_csv(out_fwf_file,header=True, index=False)

extract_data_from_fixed_width=PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    op_kwargs={'fwf_file':in_fwf_file},
    dag=dag
)

# define task to consolidate data extracted
extracted_data='/home/project/airflow/dags/finalassignment/extracted_data.csv'
consolidate_data=BashOperator(
    task_id='consolidate_data',
    bash_command=f'paste -d "," {out_csv_file} {out_tsv_file} {out_fwf_file} > {extracted_data}',
    dag=dag
)

# transform and load the data
out_data='/home/project/airflow/dags/finalassignment/staging/transformed_data.csv'
transform_data=BashOperator(
    task_id='transform_data',
    bash_command=f"awk -F',' \
        '{{ if (NR!=1) {{ print $1','$2','$3','toupper($4)','$5','$6','$7','$8','$9; }} }}' \
            < {extracted_data} > {out_data}",
    dag=dag
)

# define the task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> \
extract_data_from_fixed_width >> consolidate_data >> transform_data