from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
# from airflow.providers.amazon.aws.operators.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
# from helpers.SqlQueries import *
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'catchup': False
}

sql_filepath = '../create_tables.sql'

dq_schema = {"artists" : ["name"],
             "songplays": ["songid", "artistid", "sessionid"],
             "songs": ["title"],
             "staging_events": ["artist", "firstname", "lastname", "sessionid", "song", "userid"],
             "staging_songs": ["artist_id", "artist_name", "song_id", "title"],
              "time": [],
              "users": ["first_name", "last_name"]
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id='Create_tables',
    dag=dag,
    sql=sql_filepath,
    postgres_conn_id='redshift'
)

# stage_events_to_redshift = S3ToRedshiftOperator(
#     task_id='Stage_events',
#     dag=dag,
#     schema='public',
#     table='staging_events',
#     s3_bucket='s3://udacity-dend',
#     s3_key='log_data',
#     redshift_conn_id=redshift_conn_id # WIP
# )

# stage_songs_to_redshift = S3ToRedshiftOperator(
#     task_id='Stage_songs',
#     dag=dag,
#     schema='public',
#     table='staging_songs',
#     s3_bucket='s3://udacity-dend',
#     s3_key='song_data',
#     redshift_conn_id=redshift_conn_id # WIP
# )

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    s3_bucket='s3://udacity-dend',
    s3_key='log_data',
    ignore_headers=1,
#     delimiter=';',
    aws_credentials_id='aws_credentials', 
    redshift_conn_id='redshift'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    schema='public',
    table='staging_songs',
    s3_bucket='s3://udacity-dend',
    s3_key='song_data',
    ignore_headers=1,
#     delimiter=';',
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="songplays",
    select_sql=SqlQueries.songplay_table_insert,
    redshift_conn_id='redshift'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    select_sql=SqlQueries.user_table_insert,
    truncate_table=True,
    redshift_conn_id='redshift'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    select_sql=SqlQueries.song_table_insert,
    truncate_table=True,
    redshift_conn_id='redshift'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    select_sql=SqlQueries.artist_table_insert,
    truncate_table=True,
    redshift_conn_id='redshift'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    select_sql=SqlQueries.time_table_insert,
    truncate_table=True,
    redshift_conn_id='redshift'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    # provide_context=True,
    params={'schema': dq_schema}
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Dependencies
start_operator \
    >> create_tables \
    >> [stage_songs_to_redshift, stage_events_to_redshift] \
    >> load_songplays_table \
    >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] \
    >> run_quality_checks \
    >> end_operator
