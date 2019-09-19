from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

# Custom plugin operator to create tables in Redshift
from operators.create_tables import CreateTablesOperator
from helpers import SqlQueries

# Run list_key_dags to figure out 
# ==>  log data is only available for 2018/11
# ==>  select 2018/11/01 as backfill date

# backfill_date = datetime(2018, 11, 1) 

default_args = {
    'owner': 'khoa_nguyen',
    'start_date' : datetime.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
     # Do not email on retry
    'email': ['khoa.nguyen.acb@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}   

dag = DAG("Sparkify_data_pipeline",
          default_args=default_args,
          description="Data pipeline from S3 to Redshift with Airflow",
          schedule_interval="@daily",
          max_active_runs=3
        )

start_operator = DummyOperator(task_id="Begin_execution",  dag=dag)

create_redshift_tables = CreateTablesOperator(
    task_id="Create_tables",
    dag=dag,
    redshift_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket='udacity-dend',
    s3_key="log_data/",
    extra_params="format as json 's3://udacity-dend/log_json_path.json'",
    # s3_key="log_data/{year}/{month}/", # Data Partition
    # provide_context=True,
    # execution_date = backfill_date,
    # year=execution_date.year,
    # month=execution_date.month
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket='udacity-dend',
    s3_key="song_data/A/A/A",
    extra_params="json 'auto' compupdate off region 'us-west-2'",
    task_id="Stage_songs",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id="load_songplays_fact_table",
    redshift_conn_id="redshift",
    table="songplays",
    sql_source=SqlQueries.songplay_table_insert,
    dag=dag
)


load_user_dimension_table = LoadDimensionOperator(
    task_id="load_user_dim_table",
    redshift_conn_id="redshift",
    table="users",
    sql_source=SqlQueries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="load_song_dim_table",
    redshift_conn_id="redshift",
    table="songs",
    sql_source=SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="load_artist_dim_table",
    redshift_conn_id="redshift",
    table="artists",
    sql_source=SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="load_time_dim_table",
    redshift_conn_id="redshift",
    table="time",
    sql_source=SqlQueries.time_table_insert,
    dag=dag
)

run_data_quality_checks = DataQualityOperator(
    task_id="run_data_quality_checks",
    redshift_conn_id="redshift",
    table="time",
    dag=dag
)

end_operator = DummyOperator(task_id="stop_execution",  dag=dag)


# Dag view
start_operator >> create_redshift_tables 

create_redshift_tables >> stage_events_to_redshift
create_redshift_tables >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_data_quality_checks
load_song_dimension_table >> run_data_quality_checks
load_artist_dimension_table >> run_data_quality_checks
load_time_dimension_table >> run_data_quality_checks

run_data_quality_checks >> end_operator
