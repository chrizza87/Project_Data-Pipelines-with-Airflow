from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        SESSION_TOKEN '{}'
        REGION '{}'
        JSON '{}'
    """
    
    truncate_sql = 'TRUNCATE TABLE {}'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 aws_credentials_id,
                 table_name,
                 s3_bucket,
                 s3_key,
                 json_path='auto',
                 aws_region='us-west-2',
                 *args, **kwargs):
        
        """
        Initializes the StageToRedshiftOperator

        Parameters
        ----------
        redshift_conn_id: id (in airflow) of the redshift connection
        aws_credentials_id: id (in airflow) of the aws credentials
        table_name: name of the table to insert data
        s3_bucket: s3 bucket where the json data is located
        s3_key: path inside the s3 bucket to the json files
        json_path: optional path to json_path file
        aws_region: optional aws region string (when not in us-west-2)
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table_name =  table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.aws_region = aws_region

    def execute(self, context):
        self.log.info('StageToRedshiftOperator: Start process')
        s3_hook = S3Hook(self.aws_credentials_id)
        aws_credentials = s3_hook.get_credentials()
        
        redshift = PostgresHook(self.redshift_conn_id)
        
        s3_key_updated = self.s3_key.format(**context)
        s3_path= f's3://{self.s3_bucket}/{s3_key_updated}'
        
        self.log.info('Redshift: Truncate all data from {self.table_name}')
        redshift.run(StageToRedshiftOperator.truncate_sql.format(self.table_name))

        sql = StageToRedshiftOperator.copy_sql.format(
            self.table_name,
            s3_path,
            aws_credentials.access_key,
            aws_credentials.secret_key,
            aws_credentials.token,
            self.aws_region,
            self.json_path
        )
        
        self.log.info('Redshift: Copy all data from S3 to {self.table_name}')
        redshift.run(sql)
        