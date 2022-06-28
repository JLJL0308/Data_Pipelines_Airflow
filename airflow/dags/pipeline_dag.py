from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from helpers import SqlQueries

default_args = {
    'owner': 'jljl0308',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': 300,
    'email_on_retry': False
}

dag = DAG('pipeline_dag',
          default_args=default_args,
          description='Pipeline to extract load and transform raw data from S3 to Redshift',
          schedule_interval='@hourly',
          catchup=False
          )

start_dummy = DummyOperator(task_id='begin_execution', dag=dag)

create_staging_events_table = PostgresOperator(
    task_id='create_staging_events_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_table_staging_events
)

create_staging_songs_table = PostgresOperator(
    task_id='create_staging_songs_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_table_staging_songs
)

create_songplays_table = PostgresOperator(
    task_id='create_songplays_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_table_songplays
)

create_artists_table = PostgresOperator(
    task_id='create_artists_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_table_artists
)

create_songs_table = PostgresOperator(
    task_id='create_songs_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_table_songs
)

create_users_table = PostgresOperator(
    task_id='create_users_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_table_users
)

create_time_table = PostgresOperator(
    task_id='create_time_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_table_time
)

tables_created = DummyOperator(task_id='tables_created', dag=dag)

load_staging_events_table = StageToRedshiftOperator(
    task_id='load_staging_events_table',
    dag=dag,
    s3_bucket='udacity-dend',
    s3_prefix='log_data',
    table='staging_events',
    copy_options="JSON 's3://udacity-dend/log_json_path.json'"
)

load_staging_songs_table = StageToRedshiftOperator(
    task_id='load_staging_songs_table',
    dag=dag,
    s3_bucket='udacity-dend',
    s3_prefix='song_data',
    table='staging_songs',
    copy_options="FORMAT AS JSON 'auto'"
)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    dag=dag,
    table='songplays',
    select_sql=SqlQueries.insert_songplays_table
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='load_users_dim_table',
    dag=dag,
    table='users',
    select_sql=SqlQueries.insert_users_table,
    mode='truncate'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_songs_dim_table',
    dag=dag, table='songs',
    select_sql=SqlQueries.insert_songs_table,
    mode='truncate'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artists_dim_table',
    dag=dag,
    table='artists',
    select_sql=SqlQueries.insert_artists_table,
    mode='truncate'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dim_table',
    dag=dag,
    table='time',
    select_sql=SqlQueries.insert_time_table,
    mode='truncate'
)

check_data_quality = DataQualityOperator(
    task_id='check_data_quality',
    dag=dag,
    check_stmts=[
        {
            'sql': 'SELECT COUNT(*) FROM songplays;',
            'op': 'gt',
            'val': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM songplays WHERE songid IS NULL;',
            'op': 'eq',
            'val': 0
        }
    ]
)

stop_dummy = DummyOperator(task_id='end_execution', dag=dag)

# DAG dependencies
start_dummy >> create_staging_songs_table
start_dummy >> create_staging_events_table
start_dummy >> create_songplays_table
start_dummy >> create_artists_table
start_dummy >> create_songs_table
start_dummy >> create_users_table
start_dummy >> create_time_table

create_staging_songs_table >> tables_created
create_staging_events_table >> tables_created
create_songplays_table >> tables_created
create_artists_table >> tables_created
create_songs_table >> tables_created
create_users_table >> tables_created
create_time_table >> tables_created

tables_created >> load_staging_events_table
tables_created >> load_staging_songs_table

load_staging_events_table >> load_songplays_table
load_staging_songs_table >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> check_data_quality
load_user_dimension_table >> check_data_quality
load_artist_dimension_table >> check_data_quality
load_time_dimension_table >> check_data_quality

check_data_quality >> stop_dummy