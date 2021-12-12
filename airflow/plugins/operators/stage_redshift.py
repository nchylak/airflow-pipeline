from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION 'us-west-2'
        FORMAT as JSON '{}'
        TIMEFORMAT as 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_datakey="",
                 s3_pathkey="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_datakey = s3_datakey
        self.s3_pathkey = s3_pathkey


    def execute(self, context):
        aws_base_hook = AwsBaseHook(self.aws_credentials_id, client_type='s3')
        credentials = aws_base_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"TRUNCATE TABLE {self.table}")

        self.log.info("Copying data from S3 to Redshift")
        s3_datapath = f"s3://{self.s3_bucket}/{self.s3_datakey}"
        if self.s3_pathkey == "":
            s3_jsonpath = "auto"
        else:
            s3_jsonpath = f"s3://{self.s3_bucket}/{self.s3_pathkey}"
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_datapath,
            credentials.access_key,
            credentials.secret_key,
            s3_jsonpath
        )
        redshift.run(formatted_sql)
