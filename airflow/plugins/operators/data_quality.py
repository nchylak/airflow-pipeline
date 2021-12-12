from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 test_query = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.test_query = test_query

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for table in self.tables:
            qry = self.test_query.format(table)
            records = redshift_hook.get_records(qry)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. The query on table {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. The query on table {table} returned 0 rows")
            logging.info(f"Data quality on table {table} passed with {records[0][0]} records")
