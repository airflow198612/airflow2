from airflow.hooks.S3_hook import S3Hook
from airflow.plugins_manager import AirflowPlugin
# Importing base classes that we need to derive
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
from airflow.hooks.dbapi_hook import DbApiHook


# Class and function to get the snowflake connection details.
class SnowFlakeHook(DbApiHook):
    conn_name_attr = 'conn_id'

    def get_conn(self):
        """
        Returns a snowflake connection object
        """
        conn_config = self.get_connection(self.conn_id)
        return conn_config


# Will show up under airflow.operators.PluginOperator
class SnowFlakeOperator(BaseOperator):
    """
    CREATE BY : Jerry Cheruvathoor & Bharani Manavalan
    Executes sql code in a specific SnowFlake database
    :param conn_id: reference to a specific Snowflake Connection
    :param sql_file: Sql File which has snowflake queries seperated by ;
    :param parameters: JSON/dictionary input of key value pair to be replaced in sql_file
    """

    template_fields = ('sql_file', 'parameters')
    ui_color = '#ededef'

    @apply_defaults
    def __init__(self, sql_file, conn_id='snowflake', parameters={}, *args, **kwargs):
        super(SnowFlakeOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        if not self.conn_id:
            raise ValueError("Must provide reference to Snowflake connection in airflow!")
        self.sql_file = sql_file
        self.parameters = parameters
        self.hook = None
        self.s3_hook = None
        self.cur = None
        self.conn = None

    def execute(self, context):
        logging.info('Executing SQL File: ' + str(self.sql_file))
        self.get_cursor()
        sql_file_path = str(self.sql_file).strip()
        try:
            self.snowflake_query_exec(self.cur, self.conn.schema, sql_file_path)
        finally:
            self.cur.close()
            logging.info("Closed connection")

    def get_snowflake_connection(self):
        import snowflake.connector
        if self.conn.extra_dejson.get('authenticator') == "https://nike.okta.com":
            logging.info("snowflake_account_name={0}, database={1}, username={2}, warehouse={3}, role={4}, authenticator={5}"
                         .format(self.conn.host, self.conn.schema, self.conn.login
                                 , self.conn.extra_dejson.get('warehouse', "SAMPLE")
                                 , self.conn.extra_dejson.get('role', "SAMPLE")
                                 , self.conn.extra_dejson.get('authenticator', "https://nike.okta.com")))
            ctx = snowflake.connector.connect(
              user=self.conn.login,
              password=self.conn.password,
              account=self.conn.host,
              warehouse=self.conn.extra_dejson.get('warehouse', "SAMPLE"),
              database=self.conn.schema,
              role=self.conn.extra_dejson.get('role', "SAMPLE"),
              authenticator=self.conn.extra_dejson.get('authenticator', "https://nike.okta.com")
            )
        else:
            logging.info("snowflake_account_name={0}, database={1}, username={2}, warehouse={3}, role={4}"
                             .format(self.conn.host, self.conn.schema, self.conn.login
                                     , self.conn.extra_dejson.get('warehouse', "SAMPLE")
                                     , self.conn.extra_dejson.get('role', "SAMPLE")))
            self.parameters.update({'account_name': self.conn.host, 'database': self.conn.schema,
                                        'username': self.conn.login,
                                        'warehouse': self.conn.extra_dejson.get('warehouse', "SAMPLE"),
                                        'role': self.conn.extra_dejson.get('role', "SAMPLE")})
            ctx = snowflake.connector.connect(
                user=self.conn.login,
                password=self.conn.password,
                account=self.conn.host,
                warehouse=self.conn.extra_dejson.get('warehouse', "SAMPLE"),
                database=self.conn.schema,
                role=self.conn.extra_dejson.get('role', "SAMPLE")
            )
        return ctx

    def get_cursor(self):
        import snowflake.connector
        self.hook = SnowFlakeHook(conn_id=self.conn_id)
        try:
            logging.info('getting connection for conn id {conn_id}'.format(conn_id=self.conn_id))
            self.conn = self.hook.get_conn()
            ctx = self.get_snowflake_connection()
        except snowflake.connector.errors.ProgrammingError as e:
            # default error message
            logging.info(e)
            # customer error message
            raise ValueError('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
        self.cur = ctx.cursor()

    def snowflake_query_exec(self, cur, database, sql_file_path):
        import snowflake.connector
        file_string = open(sql_file_path, 'r').read()

        for key, value in self.parameters.iteritems():
            file_string = file_string.replace('$' + key, value)

        sql_statements = file_string.split(';')
        access_key, secret_key = self.s3_connection()
        logging.info("SQL file has been parsed out")
        cur.execute("use " + database)
        logging.info("Database Connected is {0}".format(database))
        for sql_qry in sql_statements:
            logging.info("Query to be executed is {0}".format(sql_qry))
            if sql_qry is not None and len(sql_qry) > 1:
                try:
                    sql_qry_formatted = sql_qry.replace('$aws_s3_key', access_key).replace('$aws_s3_secret_key'
                                                                                           , secret_key)
                    cur.execute(sql_qry_formatted)
                except snowflake.connector.errors.ProgrammingError as e:
                    # default error message
                    logging.info(e)
                    # customer error message
                    raise ValueError('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
            logging.info("Query completed successfully")
        logging.info("All Query/Queries completed successfully")
    def s3_connection(self):
        self.s3_hook = S3Hook(s3_conn_id='s3_default')
        logging.info('getting connection for s3 conn id {s3_conn_id}'.format(s3_conn_id='s3_default'))
        access_key = self.s3_hook._a_key
        secret_key = self.s3_hook._s_key
        return access_key, secret_key


class SnowFlakePlugin(AirflowPlugin):
    name = "snowflake_plugin"
    operators = [SnowFlakeOperator]
