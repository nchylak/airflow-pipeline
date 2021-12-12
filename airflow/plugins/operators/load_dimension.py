from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
        INSERT INTO {} 
            ({});
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_qry="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_qry = insert_qry
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate == True:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run(f"TRUNCATE TABLE {self.table}")

        self.log.info("Inserting data to Redshift")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.insert_qry
        )
        redshift.run(formatted_sql)
