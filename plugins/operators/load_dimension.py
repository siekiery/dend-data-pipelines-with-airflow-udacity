from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    trunc_sql = "TRUNCATE TABLE {}"
    insert_sql = """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 table="",
                 select_sql="",
                 truncate_table=True,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.select_sql = select_sql
        self.truncate_table = truncate_table
        self.redshift_conn_id = redshift_conn_id


    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_table:
            self.log.info(f"Truncating {self.table} dimension table")
            redshift_hook.run(LoadDimensionOperator.trunc_sql.format(self.table))

        self.log.info(f"Inserting data into {self.table} dimension table")
        redshift_hook.run(LoadDimensionOperator.insert_sql.format(self.table, self.select_sql))