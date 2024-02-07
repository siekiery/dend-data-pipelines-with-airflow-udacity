from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    artists_check_sql = "SELECT COUNT(*) FROM artists WHERE name IS NULL;"

    @apply_defaults
    def __init__(self,
                 checklist=None,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.checklist = checklist
        self.redshift_conn_id = redshift_conn_id
        self.dq_schema = kwargs["params"]["schema"]

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # records check all tables
        for table in self.dq_schema.keys():
            self.check_records(table, redshift_hook)

        # artist table name col  name not null in 
        # songplays table songid, artistid, sessionid not null
        # songs table title col not null
        # staging_events table artist, firstname, lastname, sessionid, song, userid not null
        # staging songs table artist_id, artist_name, song_id, title not null
        # users table first_name, last_name not null, level
        for table, columns in dq_schema.items():
            if len(columns) == 0:
                continue
            for col in columns:
                self.check_nulls(table, col, redshift_hook)

        # time table hour <= 24, day <= 31
        self.check_values("time", "hour", expr="<=", val=24)
        self.check_values("time", "day", expr="<=", val=31)

        self.log.info('Data Quality checks completed successfully.')

    @staticmethod
    def check_records(table, redshift_hook):
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")

        if  records is  None or len(records[0]) < 1:
            raise ValueError("No records in {table} table")

        logging.info(f"Data quality - {table} table - records check passed with {records[0][0]} records.")


    @staticmethod
    def check_nulls(table, col, redshift_hook):
    # artist table name col  name not null in 
    # songplays table songid, artistid, sessionid not null
    # songs table title col not null
    # staging_events table artist, firstname, lastname, sessionid, song, userid not null
    # staging songs table artist_id, artist_name, song_id, title not null
    # users table first_name, last_name not null, level
        result = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
        
        if result[0][0] > 0:
            raise ValueError("Null value in {table} table {col} column")

        logging.info(f"Data quality - {table} table - null values check passed on {col} column.")

    @staticmethod
    def check_values(table, col, expr, val, redshift_hook):
        # time table hour <= 24, day <= 31
        SQL = "SELECT COUNT(*) FROM {} WHERE {} {} {}"
        result = redshift_hook.get_records(SQL.format(table, col, expr, val))
                                        
        if result[0][0] > 0:
            raise ValueError("{table} table {col} has records with values out of bounds.")

        logging.info(f"Data quality - {table} table - values out of bounds in {col} column.")
