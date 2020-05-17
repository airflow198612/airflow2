from airflow.hooks.S3_hook import S3Hook
import boto
from airflow.plugins_manager import AirflowPlugin
# Importing base classes that we need to derive
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
from airflow.hooks.dbapi_hook import DbApiHook
import time
from datetime import datetime, timedelta
import datetime as dt
from boto.s3.connection import S3Connection
import snowflake.connector
import subprocess
import pandas as pd
import numpy as np
import ast
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
import smtplib

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
class Settings():
    """
    CREATE BY : Srihari Godleti
    Executes sql code in a specific SnowFlake database
    :param sql_file: Sql File which has snowflake queries seperated by ;
    :param parameters: JSON/dictionary input of key value pair to be replaced in sql_file
    """

    template_fields = ('parameters')
    ui_color = '#ededef'

    @apply_defaults
    def __init__(self, conn_id='snowflake',parameters={}, *args, **kwargs):
        self.conn_id = conn_id
        if not self.conn_id:
            raise ValueError("Must provide reference to Snowflake connection in airflow!")
        #print 'Input argument List:', str(sys.argv)

        logger = logging.getLogger("canary_validation")

        self.params_dict = {}
        self.params_dict = parameters

        # Get the preload/postload parameters else defaults
        self.app_name = "Canary_Validation"
        self.dag_name = self.params_dict.get('dag_name')
        self.dag_exec_dt = self.params_dict.get('dag_exec_dt')
        self.validation_type = self.params_dict.get('validation_type').lower()
        self.fromaddr = self.params_dict.get('fromaddr')
        self.toaddr = self._eval('toaddr', [])
        self.env = self.params_dict.get('airflow_env').lower()
        self.airflow_cluster_name = self.params_dict.get('airflow_cluster_name')
        self.s3_canary_file = self.params_dict.get('s3_canary_file')
        self.retention_period = self._eval('retention_period', None)
        self.on_failure = self._eval('on_failure', [])
        self.on_warning = self._eval('on_warning', [])
        self.on_success = self._eval('on_success', [])

        # Derived parameters
        self.on_failure = [i.lower() for i in self.on_failure]
        self.on_warning = [i.lower() for i in self.on_warning]
        self.on_success = [i.lower() for i in self.on_success]

        # Derive the default canary file and use only when not passed from dag
        self.s3_bucket = self.s3_canary_file.replace('s3://', '').replace('s3a://','').split('/')[0]
        self.s3_prefix = '/'.join(self.s3_canary_file.replace('s3://', '').replace('s3a://','').split('/')[1:])

        # Run time parameters
        self.current_time = time.time()
        self.current_timestamp = datetime.fromtimestamp(self.current_time).strftime('%Y-%m-%d %H:%M:%S')
        self.canary_query = ""
        self.validation_status = ""
        self.canary_results_df = None
        self.canary_final_df = None
        self.logger = None
        self.results = []
        self.success_records = 0
        self.failed_records = 0
        self.warning_records = 0
        self.email_sent = 0
        self.exit_status = 0

        conn = S3Connection(host='s3.amazonaws.com')
        bucket = conn.get_bucket(self.s3_bucket)
        key = bucket.get_key(self.s3_prefix, validate=True)

        # Check the s3 canary file exists before attempting to read the file
        data = key.get_contents_as_string()
        self.canary_query = self._substitute_parameters(self._replace_special_chars(data))

        # Define the runtime attrs for debugging
        self.run_time_params = {'dag_name': self.dag_name, 'dag_exec_dt': self.dag_exec_dt,
                                'validation_type': self.validation_type, 'fromaddr': self.fromaddr,
                                'toaddr': self.toaddr, 'env': self.env,'s3_canary_file': self.s3_canary_file,'on_failure': self.on_failure, 'on_warning': self.on_warning, 'on_success': self.on_success}

    def run_bash(bash_command, logger):
        """
                Create a subprocess and run a shell command
                """

        logger.info(bash_command)
        sp = subprocess.Popen(bash_command, shell=True,stdout=subprocess.PIPE,cwd=None).stdout.read().strip('\n').split('\n')
        for line in sp:
            logger.info(line)

    def get_ctx_from_sf_connector(self,conn_id='snowflake'):
        import snowflake.connector
        self.conn_id=conn_id
        # get snowflake details:
        self.sf_conn = SnowFlakeHook(self.conn_id)

        ctx = snowflake.connector.connect(user = self.sf_conn.get_conn().login,
                                          password = self.sf_conn.get_conn().password,
                                          account=self.sf_conn.get_conn().host,
                                          warehouse=self.sf_conn.get_conn().extra_dejson.get('warehouse', "SAMPLE"),
                                          database=self.sf_conn.get_conn().schema,
                                          role=self.sf_conn.get_conn().extra_dejson.get('role', "SAMPLE"))
        return ctx


    def get_df(self, q, ctx):
        # query snowflake table and get data into dataframe.
        # q = """select count(*) cnt from forecast_input.bm_forecast_base"""
        base_df = pd.read_sql(q, ctx)
        return base_df

    def _eval(self, key, default=None):
        """
            Function to convert the string to list/dictionary
        """
        try:
            return eval(self.params_dict.get(key, default))
        except:
            return self.params_dict.get(key, default)

    def _replace_special_chars(self, input_string):
        """
            Function to replace special characters from the s3 file
            :param input_string for replacing special characters
            :return: replaced string
            """

        replace_chars = ['\r', '\n', '\t', ';']
        replaced_string = input_string
        for replace_char in replace_chars:
            replaced_string = replaced_string.replace(replace_char, ' ')
        return replaced_string

    def _substitute_parameters(self, query):
        """
            Function to substitute query parameters
            :param query: Query with parameters
            :return: Query with parameters substituted
            """

        for (key, value) in self.params_dict.iteritems():
            if isinstance(value, basestring) | isinstance(value, int):
                query = query.replace('{' + key + '}', value)
        return query

    def send_email(self):
        """
            Function to send email notifications
            """

        content = '<html><body>Hi Team, <br>Canary ' + self.validation_type \
                  + ' validation report for the DAG: ' + self.dag_name \
                  + ' for the execution date ' + self.dag_exec_dt \
                  + '<br><br>Airflow Cluster: ' + self.airflow_cluster_name \
                  + '<br><br><br>' \
                  + '<table width=60% height=40% border=1><tr><td bgcolor=lightgrey>dag_name</td> <td bgcolor="lightgrey">dag_exec_dt</td> <td bgcolor="lightgrey">validation_type</td> <td bgcolor="lightgrey">table_name</td> <td bgcolor="lightgrey">region_cd</td> <td bgcolor="lightgrey">validation_name</td> <td bgcolor="lightgrey">actual_metrics</td> <td bgcolor="lightgrey">expected_metrics</td> <td bgcolor="lightgrey">validation_desc</td> <td bgcolor="lightgrey">validation_status</tr>'

        for (index, result) in self.result_df.iterrows():
            content = content + '<tr><td bgcolor=white>' \
                      + str(result['DAG_NAME']) + '</td> <td bgcolor="white">' \
                      + str(result['DAG_EXEC_DT']) + '</td> <td bgcolor="white">' \
                      + str(result['VALIDATION_TYPE']) \
                      + '</td> <td bgcolor="white">' + str(result['TABLE_NAME']) \
                      + '</td> <td bgcolor="white">' + str(result['REGION_CD']) \
                      + '</td> <td bgcolor="white">' \
                      + str(result['VALIDATION_NAME']) \
                      + '</td> <td bgcolor="white">' + str(result['ACTUAL_METRICS'
                                                           ]) + '</td> <td bgcolor="white">' \
                      + str(result['EXPECTED_METRICS']) \
                      + '</td> <td bgcolor="white">' \
                      + str(result['VALIDATION_DESC'])
            if result['VALIDATION_STATUS'].lower() == 'success':
                content = content + '<td bgcolor="lightgreen">' \
                          + str(result['VALIDATION_STATUS']) + '</tr>'
            elif result['VALIDATION_STATUS'].lower() == 'warning':
                content = content + '<td bgcolor="yellow">' \
                          + str(result['VALIDATION_STATUS']) + '</tr>'
            else:
                content = content + '<td bgcolor="red">' \
                          + str(result['VALIDATION_STATUS']) + '</tr>'

        content += '</td></table></body></html>'

        # Setup subject for the email

        subject = self.env.upper() + ': Canary ' + self.validation_type \
                  + ' Validation ' + self.validation_status + ' for ' \
                  + str(self.dag_name) + ' - ' + str(self.dag_exec_dt)

        # Email configuration

        msg = MIMEMultipart()
        msg['From'] = self.fromaddr
        #msg['To'] = ', '.join(self.toaddr)
        msg['Subject'] = subject
        content = content
        msg.attach(MIMEText(content, 'html'))
        text = msg.as_string()
        # connect to SES
        connection = boto.connect_ses()
        # and send the message
        result = connection.send_raw_email(text, source=msg['From'], destinations=self.toaddr)

    def exception_email(self, exception):
        """
            Function to send email notifications
            """

        content = 'Hi Team, Canary Validation failed!!! Error message: ' \
                  + str(exception)

        # Setup subject for the email

        subject = self.env.upper() + ': Canary ' + self.validation_type \
                  + ' Validation Failed for ' + str(self.dag_name) + ' - ' \
                  + str(self.dag_exec_dt)

        # Email configuration

        msg = MIMEMultipart()
        msg['From'] = self.fromaddr
        #msg['To'] = ', '.join(self.toaddr)
        msg['Subject'] = subject
        content = content
        msg.attach(MIMEText(content, 'html'))
        text = msg.as_string()
        # connect to SES
        connection = boto.connect_ses()
        # and send the message
        result = connection.send_raw_email(text, source=msg['From'], destinations=self.toaddr)


# Will show up under airflow.operators.PluginOperator
class SnowFlakeCanaryOperator(BaseOperator):

        def __init__(self,
                 parameters,
                 job_name=None,
                 job_type='spark',
                 env=None,
                 conn_id='snowflake',
                 *args, **kwargs):
            super(SnowFlakeCanaryOperator, self).__init__(*args, **kwargs)
            self.canary = Settings(conn_id='snowflake', parameters=parameters, *args, **kwargs)

        def execute(self, context):

            # main(self)
            # Setup spark configs
            ctx = self.canary.get_ctx_from_sf_connector()

            logger = logging.getLogger("canary_validation")

            logger.info('Canary query to be executed: ' + self.canary.canary_query)

            query = self.canary.canary_query
            self.canary.result_df = self.canary.get_df(self.canary.canary_query, ctx)

            # Printing the runtime parameters

            logger.info('Canary run-time Parameters: ')
            logger.info(['{0}: {1}'.format(key, value) for (key, value) in self.canary.run_time_params.iteritems()])
            try:

                # Collect canary results in a python list

                self.canary.results = self.canary.result_df
                logger.info('Canary validation results are: '
                            + str(self.canary.result_df.values.T.tolist()))
            except Exception, error:

                logger.info('Error executing the canary query!!! ERROR: '
                            + str(error))
                self.canary.exception_email(error)
                raise Exception('Error executing the canary query!!!')

                # Loop through the list to collect success/warning/failure records

            logger.info('Identifying success, warning and failure records...')
            for (index, row) in self.canary.result_df.iterrows():
                if row['VALIDATION_STATUS'].lower() == 'success':
                    self.canary.success_records += 1

                    # print self.canary.success_records

                if row['VALIDATION_STATUS'].lower() == 'failed':
                    self.canary.failed_records += 1
                if row['VALIDATION_STATUS'].lower() == 'warning':
                    self.canary.warning_records += 1

                    # Send email only if it is setup in the job configuration and we've that scenario

            if 'email' in self.canary.on_failure and self.canary.failed_records > 0:
                self.canary.validation_status = 'Failed'
                logger.info('Sending failure email notification...')
                self.canary.send_email()
                self.canary.email_sent = 1

            if 'email' in self.canary.on_warning and self.canary.email_sent == 0 \
                    and self.canary.warning_records > 0:
                self.canary.validation_status = 'Warning'
                logger.info('Sending warning email notification...')
                self.canary.send_email()
                self.canary.email_sent = 1

            if 'email' in self.canary.on_success and self.canary.email_sent == 0 \
                    and self.canary.success_records > 0:
                self.canary.validation_status = 'Success'
                logger.info('Sending success email notification...')
                self.canary.send_email()
                self.canary.email_sent = 1

                # Fail only if it is setup in the job configuration and we've that scenario

            if 'fail' in self.canary.on_failure and self.canary.failed_records > 0 or 'fail' \
                    in self.canary.on_warning and self.canary.warning_records > 0 or 'fail' \
                    in self.canary.on_success and self.canary.success_records > 0:
                logger.info('Failing the job as configured!!! ')
                raise Exception('Canary {0} validation Failed! Please refer the email/stage table for more details...'.format(self.canary.validation_type))


class SnowFlakeCanaryPlugin(AirflowPlugin):
    name = "snowflakecanary_plugin"
    operators = [SnowFlakeCanaryOperator]