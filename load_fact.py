from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
  
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 dest_table = "",
                 sql_to_load = "",
                 provide_context = True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id 
        self.dest_table = dest_table  
        self.sql_to_load = sql_to_load
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id )
        self.log.info('Clearing Data from destination staging table')
        redshift.run("DELETE FROM {}".format(self.dest_table))
        self.log.info('Copying data to destination staging table from Source table')
        #sql = ''' INSERT INTO public.{dest_table} {sql_to_load} '''
        sql = 'INSERT INTO %s %s' % (self.dest_table, self.sql_to_load)
        redshift.run(sql)
        self.log.info('Fact table %s load finished' % self.dest_table)
