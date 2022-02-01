from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.providers.postgres.operators.postgres import PostgresOperator

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'chrizza',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'catchup': False,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 0 * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_operator = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table_name='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log-data',
    json_path='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table_name='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song-data/A/A',
    json_path='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='songplays',
    sql=SqlQueries.songplay_table_insert,
    truncate=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='users',
    sql=SqlQueries.user_table_insert,
    truncate=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='songs',
    sql=SqlQueries.song_table_insert,
    truncate=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='artists',
    sql=SqlQueries.artist_table_insert,
    truncate=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='time',
    sql=SqlQueries.time_table_insert,
    truncate=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    dq_checks=[
        {'check_sql': 'SELECT COUNT(*) FROM artists', 'expected_result': 0, 'compare': '>'},
        {'check_sql': 'SELECT COUNT(*) FROM songplays', 'expected_result': 0, 'compare': '>'},
        {'check_sql': 'SELECT COUNT(*) FROM songs', 'expected_result': 0, 'compare': '>'},
        {'check_sql': 'SELECT COUNT(*) FROM time', 'expected_result': 0, 'compare': '>'},
        {'check_sql': 'SELECT COUNT(*) FROM users', 'expected_result': 0, 'compare': '>'},
        {'check_sql': 'SELECT COUNT(*) FROM artists WHERE artist_id is null', 'expected_result': 0, 'compare': '='},
        {'check_sql': 'SELECT COUNT(*) FROM songplays WHERE playid is null', 'expected_result': 0, 'compare': '='},
        {'check_sql': 'SELECT COUNT(*) FROM songs WHERE songid is null', 'expected_result': 0, 'compare': '='},
        {'check_sql': 'SELECT COUNT(*) FROM time WHERE start_time is null', 'expected_result': 0, 'compare': '='},
        {'check_sql': 'SELECT COUNT(*) FROM users WHERE userid is null', 'expected_result': 0, 'compare': '='},
        
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_operator
create_tables_operator >> [stage_events_to_redshift,stage_songs_to_redshift]
[stage_events_to_redshift,stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_song_dimension_table,load_user_dimension_table,load_artist_dimension_table,load_time_dimension_table]
[load_song_dimension_table,load_user_dimension_table,load_artist_dimension_table,load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator