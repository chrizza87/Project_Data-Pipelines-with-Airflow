from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    insert_sql = 'INSERT INTO {} {}'
    truncate_sql = 'TRUNCATE TABLE {}'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table_name,
                 sql,
                 truncate=True,
                 *args, **kwargs):

        """
        Initializes the LoadFactOperator

        Parameters
        ----------
        redshift_conn_id: id (in airflow) of the redshift connection
        table_name: name of the table to insert data
        sql: sql query select for data
        truncate: optional truncate bool, when true the data inside the given table will be truncated
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table_name=table_name
        self.sql=sql
        self.truncate=truncate

    def execute(self, context):
        self.log.info('LoadFactOperator: Start process')
        redshift = PostgresHook(self.redshift_conn_id)
        
        if (self.truncate):
            self.log.info('Redshift: Truncate all data from {self.table_name}')
            redshift.run(LoadFactOperator.truncate_sql.format(self.table_name))
        
        self.log.info('Redshift: Insert data into fact table {self.table_name}')
        redshift.run(LoadFactOperator.insert_sql.format(self.table_name, self.sql))
