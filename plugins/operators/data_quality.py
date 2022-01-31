from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    count_sql = 'SELECT COUNT(*) FROM {}'
    count_null_values_sql = 'SELECT COUNT(*) FROM {} WHERE {} IS NULL'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 tables_names,
                 primary_keys,
                 *args, **kwargs):
        """
        Initializes the DataQualityOperator

        Parameters
        ----------
        redshift_conn_id: id (in airflow) of the redshift connection
        table_names: names of the tables to check
        primary_keys: values of each primary key for the given table_names
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables_names = tables_names
        self.primary_keys = primary_keys

    def execute(self, context):
        self.log.info('DataQualityOperator: Start process')
        redshift = PostgresHook(self.redshift_conn_id)
        
        #1 Check if tables have records
        for table_name in self.tables_names:
            self.log.info('DataQualityOperator: Checking if table {table_name} contains any records')
            result = redshift.get_records(DataQualityOperator.count_sql.format(table_name))
            
            
            if len(result) < 1 or len(result[0]) < 1:
                raise ValueError('DataQualityOperator: Data quality check failed! Table {table_name} returned no results')

            if result[0][0] < 1:
                raise ValueError('DataQualityOperator: Quality check failed! Table {table_name} contains no rows')
            
            self.log.info('DataQualityOperator: Quality check passed for table {table_name}! Table contains {result[0][0]} records')
            
        #2 Check if there are no null values in primary keys in the tables
        for index, table_name in enumerate(self.tables_names):
            primary_key = self.primary_keys[index]
            self.log.info('DataQualityOperator: Checking if no values in {primary_key} in table {table_name} are null')
            result = redshift.get_records(DataQualityOperator.count_null_values_sql.format(table_name, primary_key))

            if len(result) > 0 :
                raise ValueError('DataQualityOperator: Data quality check failed! There are {len(result)} records in {primary_key} in table {table_name} null')
            
            self.log.info('DataQualityOperator: Quality check passed for primary key {primary_key} in table {table_name}! No null values')
            
        self.log.info('DataQualityOperator: All quality checks passed. End process')
