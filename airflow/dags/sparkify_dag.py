from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from operators.data_quality import DataQualityOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.stage_redshift import StageToRedshiftOperator
from helpers.sql_queries import SqlQueries


default_args = {
    "owner": "sparkify",
    "start_date": datetime(2021, 12, 11),
    "depends_on_past": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "sparkify_pipeline",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
    catchup=False
)

start_operator = DummyOperator(task_id="Begin_execution",  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket=Variable.get("s3_bucket"),
    s3_datakey=Variable.get("s3_log_data"),
    s3_pathkey=Variable.get("s3_log_data_path")
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket=Variable.get("s3_bucket"),
    s3_datakey=Variable.get("s3_song_data")
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    insert_qry=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    insert_qry=SqlQueries.user_table_insert,
    truncate=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    insert_qry=SqlQueries.song_table_insert,
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    insert_qry=SqlQueries.artist_table_insert,
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    insert_qry=SqlQueries.time_table_insert,
    truncate=True
)

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks_1",
    dag=dag,
    redshift_conn_id="redshift",
    tables=["staging_events", "staging_songs", "songplays", "songs", "users", "artists", "time"],
    test_query = "SELECT COUNT(*) FROM {};"
)

run_quality_checks_2 = DataQualityOperator(
    task_id="Run_data_quality_checks_2",
    dag=dag,
    redshift_conn_id="redshift",
    tables=["songplays"],
    test_query = "SELECT COUNT(*) FROM {} WHERE songid IS NOT NULL AND artistid IS NOT NULL;;"
)

end_operator = DummyOperator(task_id="Stop_execution",  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> \
[load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> \
run_quality_checks >> run_quality_checks_2 >> end_operator
