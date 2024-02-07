from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}/{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON 's3://udacity-dend/log_json_path.json'
        --DELIMITER '{}'
        IGNOREHEADER {}
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
#                  delimeter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
#         self.delimeter = delimeter
        self.ignore_headers = ignore_headers

    def execute(self, context):
        self.log.info(f"Obtaining credentials to AWS and Redshift")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postrgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Successfully connected to Redshift")
        
        # copy data into staging table
        self.log.info(f"Copying data to {self.table} staging table")
        redshift_hook.run(StageToRedshiftOperator.copy_sql.format(
            self.table, 
            self.s3_bucket, 
            self.s3_key, 
            credentials.access_key, 
            credentials.secret_key, 
#            self.delimiter,
            self.ignore_header, 
            )) 
        self.log.info(f"Copy to {self.table} completed")







