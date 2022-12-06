from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 dest_table = "",
                 sql_to_load = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id 
        self.dest_table = dest_table
        self.sql_to_load = sql_to_load

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id )
        self.log.info('Clearing Data from destination dimension table')
        redshift.run("DELETE FROM {}".format(self.dest_table))
        self.log.info('Copying data to destination dimension table from Staging table')
        #SQL
        sql = 'INSERT INTO %s %s' % (self.dest_table, self.sql_to_load)
        redshift.run(sql)
        self.log.info('Dimension table %s load finished' % self.dest_table)
