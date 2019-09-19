import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook


def list_keys():
    """
    list_keys function is developed to list all keys in S3. 
    Key is a unique indentifer for an object in a bucket.
    
    Parameters:
    - aws_conn_id: AWS connection id. It is stored in Airflow UI at Admin ==> Connections
    - s3_bucket: A container for objects stored in S3. 
    - s3_prefix: S3 prefixes relating to S3 bucket. 
    """
    hook = S3Hook(aws_conn_id='aws_credentials')
    bucket = 'udacity-dend'
    log_prefix = 'log_data'
    song_prefix = 'song_data'
    
    logging.info(f"Listing Keys from {bucket}/{log_prefix}")
    log_keys = hook.list_keys(bucket, prefix=log_prefix)
    for log_key in log_keys:
        logging.info(f"- s3://{bucket}/{log_key}")
        
    logging.info(f"Listing Keys from {bucket}/{song_prefix}")
    song_keys = hook.list_keys(bucket, prefix=song_prefix)
    for song_key in song_keys:
        logging.info(f"- s3://{bucket}/{song_key}")


default_args = {
    'owner': 'khoa_nguyen',
    'start_date' : datetime.datetime.now(),
     # Do not email on retry
    'email': ['khoa.nguyen.acb@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}        
        
dag = DAG('List_key_dag',
          default_args=default_args,
          description='Listing all keys in S3 bucket')
          #schedule_interval='@daily')

list_task = PythonOperator(
    task_id="list_keys_task",
    python_callable=list_keys,
    dag=dag
)

