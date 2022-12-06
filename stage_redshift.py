class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
    COPY {} 
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}' 
 """

    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id= "",
                 table = "",
                 s3_bucket ="",
                 s3_key="",
                 json_format = "",
                 *args, **kwargs):

        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key= s3_key   
        self.json_format= json_format

    
    def execute(self, context):
        aws_hook = AwsHook(aws_conn_id=self.aws_credentials_id, verify=None)
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id )
        
        self.log.info(f'Clearing records from {self.table}')
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info('Copying records from S3 to redshift')
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json
        )
        
        redshift.run(sql)
