from airflow.hooks.base_hook import BaseHook
from airflow.plugins_manager import AirflowPlugin
# Importing base classes that we need to derive
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
from airflow.hooks.dbapi_hook import DbApiHook
import snowflake.connector
import time
import sys
import os
import re
from multiprocessing.pool import ThreadPool as Pool
from boto.s3.connection import S3Connection
import psycopg2
import json
import datetime
import boto3
import ast
import requests
import random


# Class and function to get the snowflake connection details.
class DtfHook(DbApiHook):
    conn_name_attr = 'conn_id'

    def get_conn(self):
        """
        Returns a snowflake connection object
        """
        conn_config = self.get_connection(self.conn_id)
        return conn_config


# Will show up under airflow.operators.PluginOperator
class DtfOperator(BaseOperator):
    """
    CREATE BY : Peter Loh & Vivek for delete framework project.
    Executes sql code in a specific SnowFlake database
    :param conn_id: reference to a specific Snowflake Connection
    :param parameters: JSON/dictionary input of key value pair to be replaced in sql
    """

    template_fields = ('sql_file', 'parameters')
    ui_color = '#ededef'

    @apply_defaults
    def __init__(self, sql_file, conn_id='snowflake', parameters={}, *args, **kwargs):
        super(DtfOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        if not self.conn_id:
            raise ValueError("Must provide reference to Snowflake connection in airflow!")
        self.sql_file = sql_file
        self.parameters = parameters
        self.hook = None
        self.cur = None
        self.conn = None

    def execute(self, context):
        logging.info('Executing DTF Process.')
        self.get_cursor()
        try:
            self.snowflake_query_exec(self.cur)
        finally:
            self.cur.close()
            logging.info("Closed connection")

    def get_snowflake_connection(self):
        import snowflake.connector
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
        self.hook = DtfHook(conn_id=self.conn_id)
        try:
            self.conn = self.hook.get_conn()
            ctx = self.get_snowflake_connection()
            logging.info('getting connection for conn id {conn_id}'.format(conn_id=self.conn_id))
        except snowflake.connector.errors.ProgrammingError as e:
            # default error message
            logging.info(e)
            # customer error message
            raise ValueError('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
        self.cur = ctx.cursor()

    def get_cursor_by_conn_id(self, conn_id):
        import snowflake.connector
        self.hook = DtfHook(conn_id=conn_id)
        try:
            self.conn = self.hook.get_conn()
            ctx = self.get_snowflake_connection()
            logging.info('getting connection for conn id {conn_id}'.format(conn_id=conn_id))
        except snowflake.connector.errors.ProgrammingError as e:
            # default error message
            logging.info(e)
            # customer error message
            raise ValueError('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
        return ctx.cursor()

    def log_call_back_result(self, result):
        logging.info("Call back Success ..")

    def executeSql(self, cmd):
        logging.info("SQL cmd: " +
                     re.sub("(\(aws_key_id.*?\))",
                            "(aws_key_id='#####################' aws_secret_key='#####################')", cmd))
        try:
            return self.cur.execute(cmd)
        except snowflake.connector.errors.ProgrammingError as e:
            logging.info(e)
            raise ValueError('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))

    def execute_sql_by_cursor(self, cmd, cursor):
        logging.info("SQL cmd: " +
                     re.sub("(\(aws_key_id.*?\))",
                            "(aws_key_id='#####################' aws_secret_key='#####################')", cmd))
        try:
            if cursor is None:
                return self.executeSql(cmd)
            return cursor.execute(cmd)
        except snowflake.connector.errors.ProgrammingError as e:
            logging.info(e)
            raise ValueError('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))

    def audit_entry_exception(self, treatment_table, event, start_time, audit_obj, pbd_api_url, message):
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        if audit_obj is None:
            audit_obj = {
                "response": "error",
                "message": message,
                "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S")
            }
        else:
            audit_obj['message'] = message
            audit_obj['response'] = 'error'
            audit_obj['end_time'] = end_time.strftime("%Y-%m-%d %H:%M:%S")
        self.audit_entry(treatment_table, event, start_time, end_time, audit_obj, duration, 'error', pbd_api_url)
        self.parameters['is_exception_logged'] = "True"
        logging.info("audit entry exception logged successfully.")

    def audit_entry(self, treatment_table, event, start_time, end_time, audit_obj, duration, status, pbd_api_url):
        audit_obj['branch_name'] = self.parameterCheck("branch_name")
        audit_obj['dag_name'] = self.parameterCheck("dag_name")
        audit_obj['conn_id'] = self.conn_id
        audit_obj['unload_val_conn_id'] = self.parameterCheck('unload_validation_conn_id')
        if self.conn is not None:
            audit_obj['warehouse_name'] = self.conn.extra_dejson.get('warehouse', "SAMPLE")
        start_time_formatted = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time_formatted = end_time.strftime("%Y-%m-%d %H:%M:%S")
        duration_formatted = str(datetime.timedelta(seconds=round(duration.total_seconds())))
        audit_entry_record = {
            "transactionType": "Treatment Request",
            "transactionItem": treatment_table,
            "event": event,
            "startTime": start_time_formatted,
            "endTime": end_time_formatted,
            "log": audit_obj,
            "duration": duration_formatted,
            "status": status,
            "createdAt": end_time_formatted,
            "createdBy": "SYSTEM"
        }
        audit_entry_record_json = json.dumps(audit_entry_record)
        logging.info(audit_entry_record_json)
        self.post_pdp_api(pbd_api_url, audit_entry_record_json)

    def post_pdp_api(self, pbd_api_url, audit_entry_record_json):
        return requests.post(pbd_api_url, data=audit_entry_record_json, headers={"Content-Type": "application/json"},
                             verify=False)

    def parameterIsTrue(self, attr):
        if attr in self.parameters and "{}".format(self.parameters[attr]) == "True":
            return True
        return False

    def parameterCheck(self, attr):
        if attr in self.parameters:
            return self.parameters[attr]
        return None

    def snowflake_query_exec(self, cur):
        import snowflake.connector

        # Check replacement parameters
        for key, value in self.parameters.items():
            if value is not None:
                logging.info("Replacing parameter %s with %s " % (key, value))

        # Check valid parameters
        # Set Booleans
        do_delete_process = self.parameterIsTrue("is_process_delete")
        do_unload_process = self.parameterIsTrue("is_process_unload")
        do_migration_process = self.parameterIsTrue("is_process_migration")

        is_deidentify = self.parameterIsTrue("is_deidentify")

        original_table = "{}.{}.{}".format(self.parameterCheck("treatment_database"),
                                           self.parameterCheck("treatment_schema"),
                                           self.parameterCheck("treatment_table"))

        blacklist_ids_table = "{}.{}.{}".format(self.parameterCheck('blacklist_database'),
                                                self.parameterCheck('blacklist_schema'),
                                                self.parameterCheck('blacklist_table'))

        deidentify_db_schema = "{}.{}".format(self.parameterCheck("deidentify_database"),
                                              self.parameterCheck("deidentify_schema"))

        final_deidentify_table = "{}.{}".format(deidentify_db_schema, self.parameterCheck("treatment_table"))
        tmp_deidentify_table = "{}_TMP".format(final_deidentify_table)

        is_s3_copy_partitions = self.parameterIsTrue("is_s3_copy_partitions")

        treatment_range_json = self.parameterCheck("treatment_range")

        unload_range_json = self.parameterCheck("unload_range")

        is_s3_treatment_process = self.parameterIsTrue("is_s3_treatment_process")

        snowflake_metadata_url = self.parameterCheck("snowflake_metadata_url")

        migration_metadata_url = self.parameterCheck("migration_metadata_url")

        try:
            ################
            start_time = datetime.datetime.now()

            audit_obj = None

            if do_delete_process:
                event = 'Delete Process'
            elif do_unload_process:
                event = 'Unload Process'
            else:
                event = 'Migration Process'

            treatment_table = self.parameterCheck("treatment_table")

            ignore_partitions_hashing = self.parameterIsTrue("ignore_partitions_hashing")

            s3_copy_partition_fields = self.parameterCheck("s3_copy_partition_fields")

            pbd_api_url = self.parameterCheck("pbd_api_url")
            if pbd_api_url is None or pbd_api_url == '':
                raise ValueError('Utility API endpoint is either Empty or None, cannot record monitor logs.')

            and_conjunction = ''
            and_partition_range = ''
            if is_s3_copy_partitions and treatment_range_json is not None:
                treatment_range = self.partition_treatment_range(treatment_range_json)
                if treatment_range is not None:
                    and_conjunction = ' AND '
                    and_partition_range = '(' + treatment_range + ')'

            if is_s3_treatment_process and do_unload_process:
                parameter = {
                    "viewName": self.parameterCheck("view")
                }
                parameter_json = json.dumps(parameter)
                response = self.post_pdp_api(migration_metadata_url, parameter_json)
                if 200 == response.status_code:
                    response_json = json.loads(response.content)['response']
                    if response_json['status'] == 'Success':
                        hive_compatibility_fields = response_json['hiveFields']
                    else:
                        message = response_json['msg']
                        self.audit_entry_exception(treatment_table, "Unload Process", start_time, None, pbd_api_url,
                                                   message)
                        raise ValueError(message)
                else:
                    message = migration_metadata_url + " => response code is " + str(response.status_code)
                    self.audit_entry_exception(treatment_table, "Unload Process", start_time, None, pbd_api_url,
                                               message)
                    raise ValueError(message)
            else:
                hive_compatibility_fields = self.parameterCheck("hive_compatibility_fields")

            hive_compatibility_dict = {}
            hive_schema_dict = {}
            if hive_compatibility_fields is not None:
                for hcf in hive_compatibility_fields:
                    if hcf['type'] != 'string':
                        hive_compatibility_dict[hcf['col']] = hcf['type']
                    hive_schema_dict[hcf['col'].lower()] = hcf['col']

            if do_delete_process or do_unload_process:

                table_columns, table_columns_orig, hashing_columns = self.getTableColumns(is_deidentify, original_table,
                                                                                          hive_compatibility_dict,
                                                                                          hive_schema_dict,
                                                                                          ignore_partitions_hashing,
                                                                                          s3_copy_partition_fields)
            if do_delete_process:
                blacklist_field = self.parameterCheck("blacklist_field")
                if is_s3_treatment_process:
                    parameter = {
                        "treatmentDb": original_table,
                        "treatmentField": self.parameterCheck("treatment_field"),
                        "blacklistDb": blacklist_ids_table,
                        "blacklistField": blacklist_field
                    }
                    parameter_json = json.dumps(parameter)
                    response = self.post_pdp_api(snowflake_metadata_url, parameter_json)
                    if 200 == response.status_code:
                        response_json = json.loads(response.content)['response']
                        if response_json['status'] == 'Success':
                            is_join_field_string = response_json['isJoinFieldString']
                        else:
                            message = response_json['msg']
                            self.audit_entry_exception(treatment_table, "Delete Process", start_time, None, pbd_api_url,
                                                       message)
                            raise ValueError(message)
                    else:
                        message = snowflake_metadata_url + " => response code is " + str(response.status_code)
                        self.audit_entry_exception(treatment_table, "Delete Process", start_time, None, pbd_api_url,
                                                   message)
                        raise ValueError(message)
                else:
                    is_join_field_string = self.parameterIsTrue("is_join_field_string")
                if is_join_field_string:
                    treatment_field = 'LOWER('+self.parameterCheck("treatment_field")+')'
                    blacklist_field_lower = 'LOWER('+blacklist_field+')'
                    blacklist_field_empty = blacklist_field + ' !=\'\' AND ' + blacklist_field + ' IS NOT NULL '
                else:
                    treatment_field = self.parameterCheck("treatment_field")
                    blacklist_field_lower = blacklist_field
                    blacklist_field_empty = blacklist_field + ' IS NOT NULL '
                where_blacklist_query = 'WHERE {treatment_field} IN ' \
                                        '(SELECT DISTINCT {blacklist_field_lower} FROM {blacklist_ids_table} ' \
                                        'WHERE {blacklist_field_empty}) ' \
                                        '{and_conjunction} {and_partition_range}' \
                    .format(treatment_field=treatment_field,
                            blacklist_field_lower=blacklist_field_lower,
                            blacklist_field_empty=blacklist_field_empty,
                            blacklist_ids_table=blacklist_ids_table,
                            and_conjunction=and_conjunction,
                            and_partition_range=and_partition_range)
                delete_count_query = 'SELECT COUNT(*) FROM {original_table}  {where_blacklist_query}' \
                    .format(original_table=original_table, where_blacklist_query=where_blacklist_query)
                logging.info("SQL: " + delete_count_query)
                delete_count = self.getOneSqlResponse(delete_count_query)
                if delete_count == 0 and not self.parameterIsTrue("ignore_delete_count"):
                    message = 'No Records found to delete matching the treatment criteria ...'
                    self.audit_entry_exception(treatment_table, "Delete Process", start_time, None, pbd_api_url,
                                               message)
                    raise ValueError(message)
                self.clone_process(original_table, pbd_api_url, is_deidentify, deidentify_db_schema, treatment_table)
                self.delete_process(original_table, where_blacklist_query, is_deidentify, tmp_deidentify_table,
                                    final_deidentify_table, pbd_api_url, delete_count, treatment_table, table_columns)

            if do_unload_process:

                run_date = self.parameterCheck("run_date")
                if run_date is None:
                    run_date = time.strftime('%Y%m%d')

                # s3_treated_location = '{}_dtf_{}/'.format(self.parameterCheck('s3_location')[:-1], run_date)
                # s3_deidentified_location = '{}dtf{}/{}/'.format(self.parameterCheck('s3_location'),
                #                                                self.parameterCheck('s3_suffix_deidentified'),
                #                                                run_date)

                s3_treated_location = self.parameterCheck("s3_treated_location")
                s3_deidentified_location = self.parameterCheck("s3_deidentified_location")

                file_format = self.parameterCheck("file_format")

                if self.parameterCheck("unload_max_retries") is None:
                    max_retry = 2
                else:
                    max_retry = int(self.parameterCheck("unload_max_retries"))

                pool_size = self.parameterCheck("pool_size")
                if pool_size is None or pool_size == '':
                    pool_size = 8  # your "parallelism"
                else:
                    pool_size = int(self.parameterCheck("pool_size"))

                pool_size_to_move = self.parameterCheck("pool_size_to_move")
                if pool_size_to_move is None or pool_size_to_move == '':
                    pool_size_to_move = 8  # your "parallelism"
                else:
                    pool_size_to_move = int(self.parameterCheck("pool_size_to_move"))

                access_key, secret_key = self.s3_connection()

                skip_unload_validation = self.parameterIsTrue("skip_unload_validation")

                skip_hashing_validation = self.parameterIsTrue("skip_hashing_validation")

                hashing_field = self.parameterCheck("hashing_partition_field")

                name = ""
                order_by_field = ""
                selection_group = []
                if hashing_field is None:
                    hashing_field = ""

                hashing_partitions = self.parameterCheck("hashing_partitions")
                if hashing_partitions is None:
                    hashing_partitions = 1
                else:
                    hashing_partitions = int(hashing_partitions)
                hashing_records = self.parameterCheck("hashing_records")
                if hashing_records is None:
                    hashing_records = 10
                else:
                    hashing_records = int(hashing_records)

                old_s3_location = self.parameterCheck("old_s3_location")

                if file_format is None or file_format == '':
                    raise ValueError('Unload File Format is either Empty or None, cannot proceed with unload process.')

                is_unload_by_range = is_s3_copy_partitions and self.parameterIsTrue("is_partition_range") and \
                                     not self.parameterIsTrue("is_treatment_range")

                cursor = self.get_cursor_for_unload_validation_conn_id()

                # Copy deleted and de-identified records to S3
                if is_deidentify:
                    self.unload_deidentify(is_s3_copy_partitions, s3_copy_partition_fields, final_deidentify_table,
                                           treatment_table, s3_deidentified_location, table_columns_orig, file_format,
                                           pbd_api_url, access_key, secret_key, max_retry, pool_size,
                                           skip_unload_validation)

                if is_unload_by_range:
                    if unload_range_json is not None and unload_range_json['selectionGroup'] is not None:
                        selection_group = unload_range_json['selectionGroup']
                        name = unload_range_json['selectionGroup'][0]['name']
                        order_by_field = unload_range_json['selectionGroup'][0]['field']
                    unload_range = "".join(self.partition_selection_group(selection_group))
                    treatment_table_name = treatment_table + '_' + name
                    original_table_name = self.create_sf_table_partition_range(original_table, name, unload_range,
                                                                               order_by_field, cursor,
                                                                               treatment_table_name, pbd_api_url)
                    s3_treated_location_name = '{}{}/'.format(s3_treated_location, name)
                else:
                    s3_treated_location_name = s3_treated_location
                    treatment_table_name = treatment_table
                    original_table_name = original_table

                self.unload_process(is_s3_copy_partitions, s3_copy_partition_fields, original_table_name,
                                    treatment_table_name, s3_treated_location_name, table_columns_orig, file_format,
                                    pbd_api_url, access_key, secret_key, max_retry, pool_size,
                                    skip_unload_validation, and_partition_range)

                if not skip_hashing_validation:
                    if "CSV" in file_format or "csv" in file_format:
                        hashing_columns = []
                        hashing_columns.append('value')
                    columns = " sha2(concat(" + ",".join(hashing_columns) + ")) "
                    if is_s3_copy_partitions:
                        self.hashing_validation(old_s3_location, s3_treated_location_name, s3_copy_partition_fields,
                                                hashing_field, hashing_partitions, hashing_records, original_table_name,
                                                treatment_table_name, pbd_api_url, access_key, secret_key, file_format,
                                                columns)
                    else:
                        self.hashing_validation_non_partition(old_s3_location, s3_treated_location_name,
                                                              hashing_records, original_table_name,
                                                              treatment_table_name, pbd_api_url,
                                                              access_key, secret_key, file_format, columns)
                if is_unload_by_range:
                    self.drop_sf_table_partition_range(original_table_name, cursor)

                if is_unload_by_range and not self.parameterIsTrue("skip_move_to_s3"):
                    if not self.parameterIsTrue("is_single_move"):
                        self.move_to_original_s3(s3_treated_location_name, s3_treated_location, treatment_table,
                                                 pbd_api_url, access_key, secret_key, s3_copy_partition_fields,
                                                 pool_size_to_move, max_retry, name)
                    else:
                        self.move_to_s3(s3_treated_location_name, s3_treated_location, treatment_table, pbd_api_url)

                # Control the dag to stop after this task
                treatment_table = self.parameterCheck("treatment_table")
                if self.parameterCheck("dag_name") is None:
                    dag_name = "dtf_" + treatment_table
                else:
                    dag_name = self.parameterCheck("dag_name")
                if self.parameterIsTrue("pause_after_unload"):
                    os.popen("airflow pause " + dag_name)

            if do_migration_process:
                scripts_path = self.parameterCheck("script_paths")
                access_key, secret_key = self.s3_connection()
                self.migration_process(scripts_path, treatment_table, pbd_api_url, access_key, secret_key)

        except ValueError as exp:
            message = "DTF completed with exceptions. {}".format(exp)
            logging.info(message)
            if not self.parameterIsTrue("is_exception_logged"):
                self.audit_entry_exception(treatment_table, event + ' Exception', start_time, audit_obj, pbd_api_url, message)
            raise ValueError("DTF completed with exceptions. {}".format(exp))

    def migration_process(self, s3_path, treatment_table, pbd_api_url, access_key, secret_key):
        try:
            start_time = datetime.datetime.now()
            event = 'Migration Process'
            audit_obj = None
            s3_path_alone = s3_path.replace("s3://", '')
            boto_values = s3_path_alone.split("/", 1)
            boto_bucket_name = boto_values[0]
            boto_path = boto_values[1]
            s3 = boto3.resource('s3')
            obj = s3.Object(boto_bucket_name, boto_path)
            body = obj.get()['Body'].read()
            for line in body.splitlines():
                query = line.decode('utf-8')
                if '{aws_key_id}' in query:
                    query = query.replace('{aws_key_id}', access_key)
                if '{aws_secret_key}' in query:
                    query = query.replace('{aws_secret_key}', secret_key)
                self.executeSql(query)
            end_time = datetime.datetime.now()
            duration = end_time - start_time
            message = "Migration Process is completed successfully"
            audit_obj = {
                "response": "success",
                "message": message,
                "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S")
            }
            self.audit_entry(treatment_table, event, start_time, end_time, audit_obj, duration, 'success', pbd_api_url)
            logging.info(message)

        except ValueError as exp:
            message = "DTF completed with exceptions. {}".format(exp)
            logging.info(message)
            self.audit_entry_exception(treatment_table, event, start_time, audit_obj, pbd_api_url, message)
            raise ValueError("DTF completed with exceptions. {}".format(exp))

    def aws_command_builder(self, s3_treated_location_name, name, boto_conn, s3_copy_partition_fields):
        final_bucket_paths, boto_bucket_name, boto_path = self.get_final_bucket_paths(s3_treated_location_name,
                                                                                      boto_conn,
                                                                                      s3_copy_partition_fields)
        aws_mv_commands = []
        for path in final_bucket_paths:
            dest_s3_path = path.replace(name + "/", "")
            cmd = 'aws s3 mv {} {} --recursive'.format(path, dest_s3_path)
            aws_mv_commands.append(cmd)
        return aws_mv_commands

    def move_to_original_s3(self, s3_treated_location_name, s3_treated_location, treatment_table, pbd_api_url,
                            aws_s3_key, aws_s3_secret_key, s3_copy_partition_fields, pool_size, max_retry, name):
        start_time = datetime.datetime.now()
        event = 'S3 Move Process'
        boto_conn = S3Connection(aws_s3_key, aws_s3_secret_key)
        aws_mv_commands = self.aws_command_builder(s3_treated_location_name, name, boto_conn,
                                                   s3_copy_partition_fields)
        aws_mv_commands_len = len(aws_mv_commands)
        retry = -1
        while aws_mv_commands_len > 0 and retry < max_retry:
            pool = Pool(pool_size)
            threads = []
            for cmd in aws_mv_commands:
                result = pool.apply_async(self.aws_s3_move, (cmd,), callback=self.log_call_back_result)
                threads.append(result)
            alive = [True] * aws_mv_commands_len
            try:
                while any(alive):  # wait until all threads complete
                    for rn in range(aws_mv_commands_len):
                        alive[rn] = not threads[rn].ready()
            except:  # stop threads if user presses ctrl-c
                logging.info('trying to stop threads')
                pool.terminate()
                logging.info('stopped threads')
                raise
            retry = retry + 1
            aws_mv_commands = self.aws_command_builder(s3_treated_location_name, s3_treated_location, boto_conn,
                                                       s3_copy_partition_fields)
            aws_mv_commands_len = len(aws_mv_commands)
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        message = "{} is moved successfully to {}".format(s3_treated_location_name, s3_treated_location)
        audit_obj = {
            "response": "success",
            "message": message,
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S")
        }
        self.audit_entry(treatment_table, event, start_time, end_time, audit_obj, duration, 'success', pbd_api_url)

    def aws_s3_move(self, cmd):
        logging.info(cmd)
        os.popen(cmd)

    def move_to_s3(self, s3_treated_location_name, s3_treated_location, treatment_table, pbd_api_url):
        start_time = datetime.datetime.now()
        event = 'S3 Move Process'
        cmd = 'aws s3 mv {} {} --recursive'.format(s3_treated_location_name, s3_treated_location)
        print cmd
        os.popen(cmd)
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        message = "{} is moved successfully to {}".format(s3_treated_location_name, s3_treated_location)
        audit_obj = {
            "response": "success",
            "message": message,
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S")
        }
        self.audit_entry(treatment_table, event, start_time, end_time, audit_obj, duration, 'success', pbd_api_url)

    def create_sf_table_partition_range(self, original_table, name, unload_range, order_by_field, cursor,
                                        treatment_table, pbd_api_url):
        start_time = datetime.datetime.now()
        event = 'Create Table By Range'
        self.execute_sql_by_cursor("CREATE TABLE IF NOT EXISTS {original_table}_{name} AS "
                                   "SELECT * FROM {original_table} "
                                   "WHERE {unload_range} order by {order_by_field}"
                                   .format(original_table=original_table, name=name, unload_range=unload_range,
                                           order_by_field=order_by_field), cursor)
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        audit_obj = {
            "response": "success",
            "message": "Table created successfully",
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S")
        }
        self.audit_entry(treatment_table, event, start_time, end_time, audit_obj, duration, "success",
                         pbd_api_url)
        return original_table + '_' + name

    def drop_sf_table_partition_range(self, original_table_name, cursor):
        self.execute_sql_by_cursor("DROP TABLE IF EXISTS {original_table_name}"
                                   .format(original_table_name=original_table_name), cursor)

    def clone_process(self, original_table, pbd_api_url, is_deidentify, deidentify_db_schema, treatment_table):
        try:
            start_time = datetime.datetime.now()
            event = 'Clone Process'
            audit_obj = None
            self.executeSql("CREATE OR REPLACE TABLE {original_table}_DCUCLONE CLONE {original_table}"
                            .format(original_table=original_table))

            # GRANT SELECT Permission to APPLICATION_SNOWFLAKE_PROD_ND_CDE_READ role
            self.executeSql(
                "GRANT SELECT ON TABLE {original_table}_DCUCLONE TO ROLE APPLICATION_SNOWFLAKE_PROD_ND_CDE_READ"
                    .format(original_table=original_table))

            # Introduce De-identify Clone feature
            if is_deidentify:
                result = self.getListSqlResponse("SHOW TABLES LIKE '{treatment_table}' IN {deidentify_db_schema}"
                                                 .format(deidentify_db_schema=deidentify_db_schema,
                                                         treatment_table=treatment_table), None)
                logging.info("result"+str(len(result)))
                if len(result) == 1:
                    self.executeSql("CREATE OR REPLACE TABLE {deidentify_db_schema}.{treatment_table}_DCUCLONE CLONE "
                                    "{deidentify_db_schema}.{treatment_table}"
                                    .format(deidentify_db_schema=deidentify_db_schema, treatment_table=treatment_table))
                    self.executeSql(
                        "GRANT SELECT ON TABLE {deidentify_db_schema}.{treatment_table}_DCUCLONE TO "
                        "ROLE APPLICATION_SNOWFLAKE_PROD_ND_CDE_READ"
                            .format(deidentify_db_schema=deidentify_db_schema, treatment_table=treatment_table))

            end_time = datetime.datetime.now()
            duration = end_time - start_time
            audit_obj = {
                "response": "success",
                "message": "Clone Success",
                "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S")
            }
            self.audit_entry(treatment_table, event, start_time, end_time, audit_obj, duration, 'success', pbd_api_url)

            start_time = datetime.datetime.now()
            event = "Clone Validation Process"
            original_table_count = self.getOneSqlResponse('SELECT COUNT(*) FROM ' + original_table)
            cloned_table_count = self.getOneSqlResponse('SELECT COUNT(*) FROM ' + original_table + '_DCUCLONE')
            audit_obj = {
                "response": "success",
                "message": "original table count %d, cloned table count %d" % (original_table_count,
                                                                               cloned_table_count),
                "match": "Y",
                "total_original_count": original_table_count,
                "total_original_clone_count": cloned_table_count,
                "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S")
            }
            if is_deidentify and len(result) == 1:
                original_deidentify_table_count = self. \
                    getOneSqlResponse("SELECT COUNT(*) FROM {deidentify_db_schema}.{treatment_table}"
                                      .format(deidentify_db_schema=deidentify_db_schema,
                                              treatment_table=treatment_table))
                cloned_deidentify_table_count = self. \
                    getOneSqlResponse("SELECT COUNT(*) FROM {deidentify_db_schema}.{treatment_table}_DCUCLONE"
                                      .format(deidentify_db_schema=deidentify_db_schema,
                                              treatment_table=treatment_table))
                audit_obj['total_deidentify_count'] = original_deidentify_table_count
                audit_obj['total_deidentify_clone_count'] = cloned_deidentify_table_count
            end_time = datetime.datetime.now()
            audit_obj['end_time'] = end_time.strftime("%Y-%m-%d %H:%M:%S")
            duration = end_time - start_time
            if original_table_count == cloned_table_count:
                if is_deidentify and len(result) == 1:
                    if original_deidentify_table_count == cloned_deidentify_table_count:
                        self.executeSql("COMMIT;")
                        self.audit_entry(treatment_table, event, start_time, end_time, audit_obj, duration, 'success',
                                         pbd_api_url)
                        logging.info("original table count %d, cloned table count %d, "
                                     "original deidentify table count %d, cloned deidentify table count %d, " %
                                     (original_table_count, cloned_table_count, original_deidentify_table_count,
                                      cloned_deidentify_table_count))
                    else:
                        audit_obj['match'] = 'N'
                        audit_obj['response'] = 'error'
                        self.executeSql("ROLLBACK;")
                        raise ValueError('Error Count mismatch. original deidentify table count %d, '
                                         'cloned deidentify table count %d ' %
                                         (original_deidentify_table_count, cloned_deidentify_table_count) +
                                         'Rollback of DTF clone/delete process completed.')
                else:
                    self.executeSql("COMMIT;")
                    self.audit_entry(treatment_table, event, start_time, end_time, audit_obj, duration, 'success',
                                     pbd_api_url)
                    logging.info("original table count %d, cloned table count %d, " %
                                 (original_table_count, cloned_table_count))
            else:
                audit_obj['match'] = 'N'
                audit_obj['response'] = 'error'
                self.executeSql("ROLLBACK;")
                raise ValueError('Error Count mismatch. original table count %d, cloned table count %d ' %
                                 (original_table_count, cloned_table_count) +
                                 'Rollback of DTF clone/delete process completed.')
        except ValueError as exp:
            message = "DTF completed with exceptions. {}".format(exp)
            logging.info(message)
            self.audit_entry_exception(treatment_table, event, start_time, audit_obj, pbd_api_url, message)
            raise ValueError("DTF completed with exceptions. {}".format(exp))

    def delete_process(self, original_table, where_blacklist_query, is_deidentify, tmp_deidentify_table,
                       final_deidentify_table, pbd_api_url, delete_count, treatment_table, table_columns):
        try:

            start_time = datetime.datetime.now()
            event = 'Delete Process'
            audit_obj = None
            # Check counts to verify
            before_count = self.getOneSqlResponse('SELECT COUNT(*) FROM ' + original_table)
            after_count_expected = before_count - delete_count
            logging.info("Before count %d, expected after count %d." % (before_count, after_count_expected))
            # Begin transaction - delete process
            self.executeSql("BEGIN TRANSACTION NAME {};".format(treatment_table))

            #
            #  Get table column names for this table
            if is_deidentify and "treatment_schema" in self.parameters and \
                    "treatment_table" in self.parameters:
                logging.info("GET TABLE COLS:  DESCRIBE TABLE " + original_table + ";")

                # Create de-identify table if required
                self.executeSql("CREATE TABLE IF NOT EXISTS {} LIKE {};"
                                .format(final_deidentify_table, original_table))

                deidentify_before_count = self.getOneSqlResponse('SELECT COUNT(*) FROM ' + final_deidentify_table)

                # Copy records to be deleted and de-identified  into tmp table
                # Insert into $deleted_items_table (select $table_column_list from $treatment_table where
                # $delete_field in (select distinct $delete_field from $delete_ids_table));
                self.executeSql("INSERT INTO {final_deidentify_table} (SELECT {table_columns_array} FROM "
                                "{original_table} {where_blacklist_query});"
                                .format(final_deidentify_table=final_deidentify_table,
                                        table_columns_array=", ".join(table_columns), original_table=original_table,
                                        where_blacklist_query=where_blacklist_query))

                # GRANT SELECT Permission to APPLICATION_SNOWFLAKE_PROD_ND_CDE_READ role
                self.executeSql("GRANT SELECT ON TABLE " + final_deidentify_table +
                                " TO ROLE APPLICATION_SNOWFLAKE_PROD_ND_CDE_READ;")

            # Execute Delete from treatment tables ;
            # Delete from $treatment_table where $delete_field in
            # (select distinct  $delete_field from $delete_ids_table);
            self.executeSql("DELETE FROM {original_table} {where_blacklist_query};"
                            .format(original_table=original_table, where_blacklist_query=where_blacklist_query))

            end_time = datetime.datetime.now()
            duration = end_time - start_time
            audit_obj = {
                "response": "success",
                "message": "Delete Success",
                "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S")
            }
            self.audit_entry(treatment_table, event, start_time, end_time, audit_obj, duration, 'success', pbd_api_url)
            event = 'Delete Validation Process'
            start_time = datetime.datetime.now()

            # Post delete, check counts
            after_count = self.getOneSqlResponse('SELECT COUNT(*) FROM ' + original_table)
            after_deleted_count = before_count - after_count
            audit_obj = {
                "response": "success",
                "message": "Before count %d, expected after count %d, actual after count %d, "
                           "deleted / de-identified count %d" % (before_count, after_count_expected, after_count,
                                                                 after_deleted_count),
                "match": "Y",
                "before_count": before_count,
                "after_count_expected": after_count_expected,
                "delete_count_expected": delete_count,
                "after_count": after_count,
                "delete_count": after_deleted_count,
                "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S")
            }
            if after_count_expected == after_count:
                if is_deidentify:
                    deidentify_after_count = self.getOneSqlResponse('SELECT COUNT(*) FROM ' + final_deidentify_table)

                    deidentify_count = deidentify_after_count - deidentify_before_count

                    message = "Before count %d, expected after count %d, actual after count %d, " \
                              "de-identified count %d" % (before_count, after_count_expected, after_count,
                                                          after_deleted_count)
                    end_time = datetime.datetime.now()
                    duration = end_time - start_time
                    audit_obj['end_time'] = end_time.strftime("%Y-%m-%d %H:%M:%S")
                    audit_obj['deidentified_count'] = deidentify_count
                    audit_obj['message'] = message
                    if after_deleted_count == deidentify_count:
                        self.executeSql("COMMIT;")
                        self.audit_entry(treatment_table, event, start_time, end_time, audit_obj, duration, 'success',
                                         pbd_api_url)
                        logging.info(message)
                    else:
                        audit_obj['match'] = 'N'
                        status = 'error'
                        audit_obj['response'] = status
                        message = "DTF exception in delete process. After De-identify - Expected count %d, " \
                                  "But Actual count %d, Rolling back... " % (after_deleted_count, deidentify_count)
                        audit_obj['message'] = message
                        logging.error(message)
                        self.executeSql("ROLLBACK;")
                        raise ValueError(message)
                else:
                    self.executeSql("COMMIT;")
                    message = "Before count %d, expected after count %d, actual after count %d, deleted count %d" \
                              % (before_count, after_count_expected, after_count, after_deleted_count)
                    end_time = datetime.datetime.now()
                    duration = end_time - start_time
                    audit_obj['end_time'] = end_time.strftime("%Y-%m-%d %H:%M:%S")
                    audit_obj['message'] = message
                    self.audit_entry(treatment_table, event, start_time, end_time, audit_obj, duration, 'success',
                                     pbd_api_url)
                    logging.info(message)
            else:
                message = "DTF exception in delete process. After Delete - Expected count %d, " \
                          "But Actual count %d, Rolling back..." % (after_count_expected, after_count)
                audit_obj['message'] = message
                logging.error(message)
                status = 'error'
                audit_obj['response'] = status
                self.executeSql("ROLLBACK;")
                raise ValueError(message)
        except ValueError as exp:
            message = "DTF completed with exceptions. {}".format(exp)
            logging.info(message)
            self.audit_entry_exception(treatment_table, event, start_time, audit_obj, pbd_api_url, message)
            logging.info("DTF exception. {}".format(exp))
            logging.info("DTF exception in delete process. Rolling back...")
            self.executeSql("ROLLBACK;")
            raise ValueError('Rollback of DTF delete process completed.')

    def unload_process(self, is_s3_copy_partitions, s3_copy_partition_fields, original_table,
                       treatment_table, s3_treated_location, table_columns_orig, file_format,
                       pbd_api_url, access_key, secret_key, max_retry, pool_size,
                       skip_unload_validation, and_partition_range):
        try:
            start_time = datetime.datetime.now()
            event = 'Unload Process'
            audit_obj = None

            # Now execute copy to S3 after commit
            # Begin transaction - delete process
            self.executeSql("BEGIN TRANSACTION NAME {}_UNLOAD;"
                            .format(self.parameterCheck("treatment_table")))

            # Execute copy
            where_clause = ""
            if is_s3_copy_partitions and and_partition_range != '':
                # Add where clause for history
                where_clause = "WHERE " + and_partition_range
            # Copy updated table to S3
            logging.info(
                "Copy to table %s to S3 %s" % (treatment_table, s3_treated_location))

            # Copy partitions or not
            if is_s3_copy_partitions:
                logging.info("Using partition options - fields %s " % (
                    s3_copy_partition_fields))
                # execute partition copy
                self.partition_sequence(original_table,
                                        s3_copy_partition_fields,
                                        s3_treated_location,
                                        where_clause,
                                        table_columns_orig,
                                        file_format,
                                        pbd_api_url,
                                        False,
                                        treatment_table,
                                        pool_size,
                                        max_retry,
                                        skip_unload_validation)
                message = "Unload Process for partitions completed successfully"
            else:
                retry = -1
                is_valid = False
                event = 'Unload Validation Process'
                while not is_valid and retry < max_retry:
                    # Check if there is a record in a destination s3 location
                    ls_cmd = os.popen('aws s3 ls {} | wc -l'.format(s3_treated_location))
                    s3_row_count = int(ls_cmd.read())
                    if s3_row_count == 0:
                        # Do snowflake copy
                        cmd = "COPY INTO '{}' FROM (SELECT {} FROM {} {}) " \
                              "credentials=(aws_key_id='{}' aws_secret_key='{}') {} ;" \
                            .format(s3_treated_location, ", ".join(table_columns_orig),
                                    original_table, where_clause, access_key, secret_key, file_format)
                        self.executeSql(cmd)
                    if skip_unload_validation:
                        is_valid = True
                    else:
                        retry_count = self.get_retry_count(retry)
                        is_valid = self.unload_validation_and_audit_entry(s3_treated_location, start_time, file_format,
                                                                          original_table, treatment_table, pbd_api_url,
                                                                          event + retry_count)
                    retry = retry + 1
                if not is_valid and retry < max_retry:
                    raise ValueError('Exceeded the maximum retry')
                message = "Unload Process completed successfully."

            end_time = datetime.datetime.now()
            duration = end_time - start_time
            event = 'Unload Overall Process'
            audit_obj = {
                "response": "success",
                "message": message,
                "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S")
            }

            self.audit_entry(treatment_table, event, start_time, end_time, audit_obj, duration, 'success',
                             pbd_api_url)
            logging.info(message)

        except ValueError as exp:
            message = " DTF exception. {}".format(exp)
            self.audit_entry_exception(treatment_table, event, start_time, audit_obj, pbd_api_url, message)
            logging.info(message)
            logging.info("DTF exception in unload process. Rolling back unload...")
            self.executeSql("ROLLBACK;")
            raise ValueError('Rollback of DTF unload process completed.')

    def unload_deidentify(self, is_s3_copy_partitions, s3_copy_partition_fields, final_deidentify_table,
                          treatment_table, s3_deidentified_location, table_columns_orig, file_format,
                          pbd_api_url, access_key, secret_key, max_retry, pool_size,
                          skip_unload_validation):
        try:
            start_time = datetime.datetime.now()
            event = 'Unload De-identify Process'
            audit_obj = None
            # Copy partitions or not
            if is_s3_copy_partitions:
                logging.info("Using partition options - fields %s " % (
                    s3_copy_partition_fields))
                # execute partition copy
                self.partition_sequence(final_deidentify_table,
                                        s3_copy_partition_fields,
                                        s3_deidentified_location,
                                        "",
                                        table_columns_orig,
                                        file_format,
                                        pbd_api_url,
                                        True,
                                        treatment_table,
                                        pool_size,
                                        max_retry,
                                        skip_unload_validation)
                logging.info("Unload processes for partitions completed successfully.")
            else:
                retry = -1
                is_valid = False
                event = 'Unload De-identify Validation Process'
                while not is_valid and retry < max_retry:
                    # Check if there is a record in a destination s3 location
                    ls_cmd = os.popen('aws s3 ls {} | wc -l'.format(s3_deidentified_location))
                    s3_row_count = int(ls_cmd.read())
                    if s3_row_count == 0:
                        # Call SQL to do snowflake copy - no need to pass columns list
                        cmd = "COPY INTO '{}' FROM (SELECT {} FROM {}) " \
                              "CREDENTIALS=(aws_key_id='{}' aws_secret_key='{}') {} ; " \
                            .format(s3_deidentified_location, ", ".join(table_columns_orig),
                                    final_deidentify_table, access_key, secret_key, file_format)
                        self.executeSql(cmd)
                        self.executeSql("COMMIT;")
                    if skip_unload_validation:
                        is_valid = True
                    else:
                        retry_count = self.get_retry_count(retry)
                        is_valid = self.unload_validation_and_audit_entry(s3_deidentified_location, start_time,
                                                                          file_format, final_deidentify_table,
                                                                          treatment_table, pbd_api_url,
                                                                          event + retry_count)
                    retry = retry + 1
                if not is_valid and retry < max_retry:
                    raise ValueError('Exceeded the maximum retry')
            end_time = datetime.datetime.now()
            duration = end_time - start_time
            event = 'Unload De-identify Overall Process'
            audit_obj = {
                "response": "success",
                "message": "Unload and De-identified Process completed successfully.",
                "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S")
            }
            self.audit_entry(treatment_table, event, start_time, end_time, audit_obj, duration, 'success',
                             pbd_api_url)

            logging.info("Unload and De-identified processes completed successfully.")
        except ValueError as exp:
            message = " DTF exception. {}".format(exp)
            self.audit_entry_exception(treatment_table, event, start_time, audit_obj, pbd_api_url, message)
            logging.info(message)
            logging.info("DTF exception in unload de-identify process. Rolling back unload...")
            self.executeSql("ROLLBACK;")
            raise ValueError('Rollback of DTF unload de-identify process completed.')

    def unload_validation_and_audit_entry(self, s3_path, start_time, file_format, original_table, treatment_table,
                                          pbd_api_url, event):
        logging.info(s3_path + " already exists !! ")
        s3_path_alone = s3_path.replace("s3://", '')
        boto_values = s3_path_alone.split("/", 1)
        boto_bucket_name = boto_values[0]
        boto_path = boto_values[1]
        audit_obj = {
            "response": 'success',
            "message": "Unload Validation Process completed successfully.",
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "total_s3_count": 0,
            "total_snowflake_count": 0
        }
        type = self.get_type_by_file_format(file_format)
        aws_s3_key, aws_s3_secret_key = self.s3_connection()

        cursor = self.get_cursor_for_unload_validation_conn_id()

        self.create_stage(original_table + '_file_format', type, original_table + '_stage',
                          s3_path, aws_s3_key, aws_s3_secret_key, cursor)

        select_ext_table_query = self.create_external_table(original_table, cursor, [])
        is_valid = self.unload_validation_non_partition(original_table, audit_obj, boto_bucket_name, boto_path, cursor,
                                                        select_ext_table_query)
        self.drop_stage(original_table, cursor)
        self.drop_external_table(original_table, cursor)
        if is_valid:
            status = 'success'
        else:
            status = 'error'
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        audit_obj['response'] = status
        audit_obj['end_time'] = end_time.strftime("%Y-%m-%d %H:%M:%S")
        self.audit_entry(treatment_table, event, start_time, end_time, audit_obj, duration, status,
                         pbd_api_url)
        return is_valid

    def get_cursor_for_unload_validation_conn_id(self):
        unload_validation_conn_id = self.parameterCheck("unload_validation_conn_id")

        if unload_validation_conn_id is None or unload_validation_conn_id == '':
            cursor = None
        else:
            cursor = self.get_cursor_by_conn_id(unload_validation_conn_id)
        return cursor

    # Get S3 credentials from Airflow S3 connection ID
    def s3_connection(self):
        access_key = None
        secret_key = None
        logging.info('getting connection for s3 conn id {s3_conn_id}'.format(s3_conn_id='s3_default'))
        conn = BaseHook.get_connection('s3_default')
        for arg_name, arg_val in conn.extra_dejson.items():
            if arg_name in ['aws_access_key_id']:
                access_key = arg_val
            if arg_name in ['aws_secret_access_key']:
                secret_key = arg_val
        return access_key, secret_key

    def getTableColumns(self, is_deidentify, original_table, hive_compatibility_dict, hive_schema_dict,
                        ignore_partitions_hashing, s3_copy_partition_fields):
        table_columns = []
        table_columns_orig = []
        hashing_columns = []
        cs = self.executeSql("describe table " + original_table + ";")
        ret = cs.fetchmany(10)
        while len(ret) > 0:
            for row in ret:
                if row[0].lower() in hive_schema_dict:
                    field_alias_name = hive_schema_dict[row[0].lower()]
                else:
                    field_alias_name = row[0].lower()
                if ignore_partitions_hashing:
                    if row[0].lower() not in s3_copy_partition_fields:
                        hashing_columns.append("nvl(value:%s,'NULL')" % field_alias_name)
                else:
                    hashing_columns.append("nvl(value:%s,'NULL')" % field_alias_name)
                if hive_compatibility_dict is not None and field_alias_name in hive_compatibility_dict:
                    type = hive_compatibility_dict[field_alias_name]
                    table_columns_orig.append("CAST(%s as %s) AS \"%s\"" % (row[0], type, field_alias_name))
                else:
                    table_columns_orig.append("%s AS \"%s\"" % (row[0], field_alias_name))
                if is_deidentify:
                    deId = False
                    for item in self.parameterCheck("deidentify_fields"):
                        if row[0].lower() == item["field"].lower():
                            fnCall = item["functionName"] + '(' + item["field"].lower() + ')'
                            table_columns.append(fnCall)
                            deId = True
                    if not deId:
                        table_columns.append("%s AS \"%s\"" % (row[0], row[0].lower()))
            ret = cs.fetchmany(3)
        return table_columns, table_columns_orig, hashing_columns

    # Execute one sql command and return response if any
    def getOneSqlResponse(self, sql):
        try:
            logging.info("SQL cmd: " + sql)
            self.cur.execute(sql)
            one_row = self.cur.fetchone()
            resp = one_row[0]
        except snowflake.connector.errors.ProgrammingError as e:
            raise ValueError('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
        return resp

    # Execute one sql command and return response if any
    def get_one_sql_response(self, sql, cursor):
        try:
            logging.info("SQL cmd: " + sql)
            if cursor is not None:
                cursor.execute(sql)
                one_row = cursor.fetchone()
            else:
                self.cur.execute(sql)
                one_row = self.cur.fetchone()
            resp = one_row[0]
        except snowflake.connector.errors.ProgrammingError as e:
            raise ValueError('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
        return resp

    def getListSqlResponse(self, sql, cursor):
        try:
            logging.info("SQL cmd: " + sql)
            if cursor is not None:
                cursor.execute(sql)
                list_row = cursor.fetchall()
            else:
                self.cur.execute(sql)
                list_row = self.cur.fetchall()
            resp = list_row
        except snowflake.connector.errors.ProgrammingError as e:
            raise ValueError('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
        return resp

    def get_s3_dict(self, select_ext_table_query, partitions, cursor):
        s3_dict = {}
        partition_len = len(partitions)
        s3_count_list = self.getListSqlResponse(select_ext_table_query, cursor)

        for s3_count in s3_count_list:
            key = []
            for i in range(0, partition_len):
                key.append('{}={}'.format(partitions[i], s3_count[i]))
            s3_dict['/'.join(key)] = s3_count[partition_len]
        return s3_dict

    def get_sf_dict(self, original_table, partitions, cursor, where_clause):
        sf_dict = {}
        partition_len = len(partitions)
        snowflake_count_list = self. \
            getListSqlResponse("SELECT {partition_columns}, COUNT(*) AS CNT FROM {original_table} {where_clause} "
                               "GROUP BY {partition_columns} ORDER BY {partition_columns} ASC"
                               .format(original_table=original_table, where_clause=where_clause,
                                       partition_columns=','.join(partitions)), cursor)

        for snowflake_count in snowflake_count_list:
            key = []
            for i in range(0, partition_len):
                key.append('{}={}'.format(partitions[i], snowflake_count[i]))
            sf_dict['/'.join(key)] = snowflake_count[partition_len]
        return sf_dict

    def get_final_bucket_paths(self, s3_path, boto_conn, partitions):
        s3_path_alone = s3_path.replace("s3://", '')
        boto_values = s3_path_alone.split("/", 1)
        boto_bucket_name = boto_values[0]
        boto_path = boto_values[1]
        bucket = boto_conn.get_bucket(boto_bucket_name)
        final_bucket_paths = []
        len_partitions = len(partitions)
        len_index = 1
        bucket_path = bucket.list(prefix=boto_path, delimiter='/')
        while len_index < len_partitions:
            new_bucket_path = []
            for bk_path in bucket_path:
                for key in bucket.list(prefix=bk_path.name, delimiter='/'):
                    new_bucket_path.append(key)
            len_index = len_index + 1
            bucket_path = new_bucket_path
        for bk_path in bucket_path:
            s3_full_path = 's3://{}/{}'.format(boto_bucket_name, bk_path.name)
            final_bucket_paths.append(s3_full_path)
        return final_bucket_paths, boto_bucket_name, boto_path

    def group_s3_path_by_hashing_field(self, final_bucket_paths, hashing_field):
        hashing_dict = {}
        for path in final_bucket_paths:
            result = re.findall(r""+hashing_field+"=(.*?)/", path)
            # if '-' not in result[0]:
            #    raise ValueError('hashing_field expect to be in this format YYYY-MM-DD but actual is ' + hashing_field)
            key = result[0][:4]
            if key in hashing_dict:
                hashing_dict[key].append(path)
            else:
                path_list = []
                path_list.append(path)
                hashing_dict[key] = path_list
        return hashing_dict

    def hash_and_compare(self, old_external_table, new_external_table, columns, count, cursor):
        query = 'select count(*) from {new_external_table}'.format(new_external_table=new_external_table)
        new_s3_count = self.get_one_sql_response(query, cursor)
        if new_s3_count < count:
            count = new_s3_count
        query = 'select count(*) from (select distinct {columns} as hashing_field from {old_external_table} ' \
                'where hashing_field in (select distinct {columns} from {new_external_table} limit {count}))'\
            .format(old_external_table=old_external_table, new_external_table=new_external_table, columns=columns, count=count)
        hashed_matching_count = self.get_one_sql_response(query, cursor)
        return hashed_matching_count, count

    def hashing_validation_non_partition(self, old_s3_location, s3_treated_location_name,
                                         hashing_records, original_table, treatment_table, pbd_api_url,
                                         aws_s3_key, aws_s3_secret_key, file_format, columns):
        try:
            start_time = datetime.datetime.now()
            event = "Hashing Validation Process"
            audit_obj = None
            cursor = self.get_cursor_for_unload_validation_conn_id()
            type = self.get_type_by_file_format(file_format)

            message = "Hashing Validation Process completed successfully."
            audit_obj = {
                "response": "success",
                "message": message,
                "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "detailed_hashed_matching_count": [],
                "hashing_records_to_process": hashing_records,

            }
            new_s3_path = s3_treated_location_name
            new_s3_stage = original_table + '_new_s3_stage'
            new_s3_file_format_name = original_table + '_new_s3_file_format'

            new_external_table = original_table + '_new_external_table'

            # pattern_type = self.get_pattern_by_file_format(file_format)

            # pattern = '^.*.{pattern_type}.*$'.format(pattern_type=pattern_type)

            self.create_stage(new_s3_file_format_name, type, new_s3_stage, new_s3_path, aws_s3_key, aws_s3_secret_key,
                              cursor)

            self.create_external_table_without_pattern(new_external_table, new_s3_stage, new_s3_file_format_name,
                                                       cursor)

            old_s3_path = old_s3_location
            old_s3_stage = original_table + '_old_s3_stage'
            old_s3_file_format_name = original_table + '_old_s3_file_format'

            old_external_table = original_table + '_old_external_table'

            self.create_stage(old_s3_file_format_name, type, old_s3_stage, old_s3_path, aws_s3_key, aws_s3_secret_key,
                              cursor)

            self.create_external_table_without_pattern(old_external_table, old_s3_stage, old_s3_file_format_name,
                                                       cursor)

            hashed_matching_count, count = self.hash_and_compare(old_external_table, new_external_table, columns,
                                                                 hashing_records, cursor)
            hashed_json = {
                "new_s3_path": new_s3_path,
                "old_s3_path": old_s3_path,
                "matching_count": hashed_matching_count,
                "expected_count": count
            }
            audit_obj["detailed_hashed_matching_count"].append(hashed_json)
            is_valid = hashed_matching_count == count
            if not is_valid:
                message = 'Mismatch in hashing between old s3 and new s3'
                # self.audit_entry_exception(treatment_table, "Hashing Validation Process", start_time, audit_obj,
                #                           pbd_api_url, message)
                raise ValueError(message)
            self.drop_stage_by_name(old_s3_stage, old_s3_file_format_name, cursor)
            self.drop_stage_by_name(new_s3_stage, new_s3_file_format_name, cursor)
            end_time = datetime.datetime.now()
            duration = end_time - start_time
            audit_obj['end_time'] = end_time.strftime("%Y-%m-%d %H:%M:%S")
            self.audit_entry(treatment_table, "Hashing Validation Process", start_time,
                             end_time, audit_obj, duration, "success", pbd_api_url)
            logging.info(message)
        except ValueError as exp:
            message = "DTF completed with exceptions. {}".format(exp)
            logging.info(message)
            self.audit_entry_exception(treatment_table, event, start_time, audit_obj, pbd_api_url, message)
            raise ValueError("DTF completed with exceptions. {}".format(exp))

    def hashing_validation(self, s3_old_path, s3_new_path, s3_copy_partition_fields, hashing_field, hashing_partitions,
                           hashing_records, original_table, treatment_table, pbd_api_url, aws_s3_key, aws_s3_secret_key,
                           file_format, columns):
        try:
            start_time = datetime.datetime.now()
            event = "Hashing Validation Process"
            audit_obj = None
            cursor = self.get_cursor_for_unload_validation_conn_id()
            type = self.get_type_by_file_format(file_format)
            boto_conn = S3Connection(aws_s3_key, aws_s3_secret_key)
            final_bucket_paths, boto_bucket_name, boto_path = self.get_final_bucket_paths(s3_new_path, boto_conn,
                                                                                          s3_copy_partition_fields)
            hashing_dict = self.group_s3_path_by_hashing_field(final_bucket_paths, hashing_field)
            message = "Hashing Validation Process completed successfully."
            audit_obj = {
                "response": "success",
                "message": message,
                "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "detailed_hashed_matching_count": [],
                "hashing_partitions_to_process": hashing_partitions,
                "hashing_records_to_process": hashing_records,

            }
            for year in sorted(hashing_dict):
                path_list = sorted(hashing_dict[year])
                len_dict = len(path_list)
                if hashing_partitions >= len_dict:
                    validate_dict = range(1, len_dict)
                else:
                    validate_dict = random.sample(range(1, len_dict), hashing_partitions)
                for i in validate_dict:
                    new_s3_path = path_list[i]
                    new_s3_stage = original_table + '_new_s3_stage_' + str(i)
                    new_s3_file_format_name = original_table + '_new_s3_file_format_' + str(i)
                    self.create_stage(new_s3_file_format_name, type, new_s3_stage,
                                      new_s3_path, aws_s3_key, aws_s3_secret_key, cursor)

                    new_external_table = original_table + '_new_external_table'

                    # pattern_type = self.get_pattern_by_file_format(file_format)

                    # pattern = '^.*.{pattern_type}.*$'.format(pattern_type=pattern_type)

                    self.create_external_table_without_pattern(new_external_table, new_s3_stage, new_s3_file_format_name
                                                               , cursor)

                    old_s3_path = path_list[i].replace(s3_new_path, s3_old_path)

                    old_s3_stage = original_table + '_old_s3_stage_' + str(i)
                    old_s3_file_format_name = original_table + '_old_s3_file_format_' + str(i)
                    self.create_stage(old_s3_file_format_name, type, old_s3_stage, old_s3_path, aws_s3_key,
                                      aws_s3_secret_key, cursor)

                    old_external_table = original_table + '_old_external_table'

                    self.create_external_table_without_pattern(old_external_table, old_s3_stage, old_s3_file_format_name
                                                               , cursor)
                    hashed_matching_count, count = self.hash_and_compare(old_external_table, new_external_table,
                                                                         columns, hashing_records, cursor)
                    hashed_json = {
                        "new_s3_path": new_s3_path,
                        "old_s3_path": old_s3_path,
                        "matching_count": hashed_matching_count,
                        "expected_count": count
                    }
                    audit_obj["detailed_hashed_matching_count"].append(hashed_json)
                    is_valid = hashed_matching_count == count
                    if not is_valid:
                        message = 'Mismatch in hashing between old s3 and new s3'
                        # self.audit_entry_exception(treatment_table, event, start_time, audit_obj, pbd_api_url,
                        #                           message)
                        raise ValueError(message)
                    self.drop_stage_by_name(old_s3_stage, old_s3_file_format_name, cursor)
                    self.drop_stage_by_name(new_s3_stage, new_s3_file_format_name, cursor)
            end_time = datetime.datetime.now()
            duration = end_time - start_time
            audit_obj['end_time'] = end_time.strftime("%Y-%m-%d %H:%M:%S")
            self.audit_entry(treatment_table, event, start_time, end_time, audit_obj, duration, "success", pbd_api_url)
            logging.info(message)
        except ValueError as exp:
            message = "DTF completed with exceptions. {}".format(exp)
            logging.info(message)
            self.audit_entry_exception(treatment_table, event, start_time, audit_obj, pbd_api_url, message)
            raise ValueError("DTF completed with exceptions. {}".format(exp))

    def query_builder(self, partitions_val, s3_path, partitions, copy_template, original_table, where_clause,
                      aws_s3_key, aws_s3_secret_key, file_format, table_columns_orig, boto_conn, pbd_api_url,
                      retry, is_deidentify, skip_unload_validation, cursor, treatment_table, select_ext_table_query):
        start_time = datetime.datetime.now()
        final_bucket_paths, boto_bucket_name, boto_path = self.get_final_bucket_paths(s3_path, boto_conn, partitions)
        query_out = []
        status = 'success'
        total_partitions = len(partitions_val)
        audit_obj = {
            "response": status,
            "message": "Unload Validation Process completed successfully.",
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "detailed_partition_counts":
                {"matched_partitions": [], "mismatched_partitions": [], "skipped_partitions": []},
            "total_s3_count": 0,
            "total_snowflake_count": 0,
            "total_partitions": total_partitions
        }
        detailed_skipped_partitions = []
        matched_partitions = 0
        mismatched_partitions = 0

        s3_dict = {}
        sf_dict = {}

        if not skip_unload_validation:
            sf_dict = self.get_sf_dict(original_table, partitions, cursor, where_clause)
            s3_dict = self.get_s3_dict(select_ext_table_query, partitions, cursor)

        for i in partitions_val:
            partition_path = '/'.join(['{}={}'.format(partitions[ii], k) for ii, k in enumerate(i)])
            path = "{}{}/".format(s3_path, partition_path)
            build_query = True
            snowflake_count = 0
            if partition_path in sf_dict:
                snowflake_count = sf_dict[partition_path]
            s3_count = 0
            if partition_path in s3_dict:
                s3_count = s3_dict[partition_path]
            if path in final_bucket_paths:
                logging.info(path + " is processed !! ")
                if skip_unload_validation:
                    is_valid = True
                else:
                    is_valid = self.unload_validation(s3_count, snowflake_count, partition_path,
                                                      audit_obj, boto_bucket_name, boto_path)
                if is_valid:
                    matched_partitions = matched_partitions + 1
                    build_query = False
                else:
                    mismatched_partitions = mismatched_partitions + 1
                    status = 'error'
                    audit_obj['message'] = 'Count mismatch in partitions'
                    audit_obj['response'] = status
            else:
                partition_json = {
                    "partition": partition_path,
                    "s3_count": s3_count,
                    "snowflake_count": snowflake_count,
                    "skipped": "Y"
                }
                detailed_skipped_partitions.append(partition_json)
            if build_query:
                query = copy_template.format(path=path, table_columns_orig=", ".join(table_columns_orig),
                                             original_table=original_table,
                                             where=' and '.join(
                                                 ['{}=\'{}\''.format(partitions[ii], k) for ii, k in enumerate(i)]
                                             ).replace("='None'", " IS NULL"), where_clause=where_clause,
                                             aws_s3_key=aws_s3_key, aws_s3_secret_key=aws_s3_secret_key,
                                             file_format=file_format)
                query_out.append(query)

        for k, v in sf_dict.items():
            total_snowflake_count = audit_obj['total_snowflake_count'] + sf_dict[k]
            audit_obj['total_snowflake_count'] = total_snowflake_count

        end_time = datetime.datetime.now()
        duration = end_time - start_time
        audit_obj['end_time'] = end_time.strftime("%Y-%m-%d %H:%M:%S")
        skip_checkpoint_log = False
        event_type = 'Validation'
        retry_count = self.get_retry_count(retry)
        if is_deidentify:
            deidentify = 'De-identify '
        else:
            deidentify = ''
        skipped_partitions = total_partitions - matched_partitions
        if skipped_partitions != mismatched_partitions and skipped_partitions != total_partitions:
            audit_obj['skipped_partitions'] = skipped_partitions
            audit_obj['detailed_partition_counts']['skipped_partitions'] = detailed_skipped_partitions
        audit_obj['matched_partitions'] = matched_partitions
        audit_obj['mismatched_partitions'] = mismatched_partitions
        if skipped_partitions != 0:
            if matched_partitions != 0 or mismatched_partitions != 0:
                if retry == -1:
                    event_type = 'Checkpoint Validation'
                status = 'error'
                audit_obj['message'] = "{matched_partitions} of {total_partitions} partitions processed successfully" \
                    .format(total_partitions=total_partitions, matched_partitions=matched_partitions)
            else:
                skip_checkpoint_log = True
                status = 'success'
                audit_obj['message'] = "{total_partitions} partitions yet to be processed" \
                    .format(total_partitions=total_partitions)
            audit_obj['response'] = status
        event = 'Unload {deidentify}{event_type} Process{retry_count}' \
            .format(deidentify=deidentify, event_type=event_type, retry_count=retry_count)
        if skip_checkpoint_log or skip_unload_validation:
            logging.info(json.dumps(audit_obj))
        else:
            self.audit_entry(treatment_table, event, start_time, end_time, audit_obj, duration, status, pbd_api_url)
        return query_out, skipped_partitions

    def unload_validation(self, s3_count, snowflake_count, partition_path, audit_obj, boto_bucket_name, boto_path):

        total_s3_count = audit_obj['total_s3_count'] + s3_count
        audit_obj['total_s3_count'] = total_s3_count

        if s3_count == snowflake_count:
            partition_json = {
                "partition": partition_path,
                "s3_count": s3_count,
                "snowflake_count": snowflake_count,
                "match": "Y"
            }
            audit_obj['detailed_partition_counts']['matched_partitions'].append(partition_json)
            logging.info("s3 count - {s3_count} is matching with snowflake count- {snowflake_count}"
                         .format(s3_count=s3_count, snowflake_count=snowflake_count))
            return True
        else:
            logging.info("s3 count - %d is mis matching with snowflake count- %d" % (s3_count, snowflake_count))
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(boto_bucket_name)
            bucket.objects.filter(Prefix=boto_path + partition_path).delete()
            partition_json = {
                "partition": partition_path,
                "s3_count": s3_count,
                "snowflake_count": snowflake_count,
                "match": "N"
            }
            audit_obj['detailed_partition_counts']['mismatched_partitions'].append(partition_json)
            return False

    def unload_validation_non_partition(self, original_table, audit_obj, boto_bucket_name, boto_path, cursor,
                                        select_ext_table_query):

        s3_count = self.get_one_sql_response(select_ext_table_query, cursor)
        audit_obj['total_s3_count'] = s3_count
        snowflake_count = self.get_one_sql_response("SELECT COUNT(*) AS CNT FROM {original_table} "
                                                    .format(original_table=original_table), cursor)
        audit_obj['total_snowflake_count'] = snowflake_count
        if s3_count == snowflake_count:
            audit_obj['match'] = 'Y'
            logging.info("s3 count - {s3_count} is matching with snowflake count- {snowflake_count}"
                         .format(s3_count=s3_count, snowflake_count=snowflake_count))
            return True
        else:
            logging.info("s3 count - %d is mis matching with snowflake count- %d" % (s3_count, snowflake_count))
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(boto_bucket_name)
            bucket.objects.filter(Prefix=boto_path).delete()
            audit_obj['match'] = 'N'
            return False

    def create_stage(self, file_format_name, file_format_type, stage_name, s3_location, aws_key_id,
                     aws_secret_key, cursor):
        self.execute_sql_by_cursor("CREATE OR REPLACE FILE FORMAT {file_format_name} {file_format_type}"
                                   .format(file_format_name=file_format_name, file_format_type=file_format_type),
                                   cursor)

        self.execute_sql_by_cursor("CREATE OR REPLACE STAGE {stage_name} FILE_FORMAT = {file_format_name} "
                                   "URL = '{s3_location}' CREDENTIALS = (aws_key_id='{aws_key_id}' "
                                   "aws_secret_key='{aws_secret_key}')"
                                   .format(stage_name=stage_name, file_format_name=file_format_name,
                                           s3_location=s3_location, aws_key_id=aws_key_id,
                                           aws_secret_key=aws_secret_key), cursor)

    def drop_stage(self, original_table, cursor):
        stage_name = original_table + '_stage'
        file_format_name = original_table + '_file_format'
        self.drop_stage_by_name(stage_name, file_format_name, cursor)

    def drop_stage_by_name(self, stage_name, file_format_name, cursor):
        self.execute_sql_by_cursor("DROP STAGE {stage_name}".format(stage_name=stage_name), cursor)

        self.execute_sql_by_cursor("DROP FILE FORMAT {file_format_name}".format(file_format_name=file_format_name),
                                   cursor)

    def create_external_table_without_pattern(self, external_table, stage_name, file_format_name, cursor):
        self.execute_sql_by_cursor("CREATE OR REPLACE EXTERNAL TABLE {external_table} "
                                   "WITH LOCATION=@{stage_name}/ AUTO_REFRESH = true FILE_FORMAT={file_format_name} "
                                   .format(external_table=external_table, stage_name=stage_name,
                                           file_format_name=file_format_name), cursor)

    def create_external_table_using_pattern(self, external_table, stage_name, file_format_name, pattern, cursor):
        self.execute_sql_by_cursor("CREATE OR REPLACE EXTERNAL TABLE {external_table} "
                                   "WITH LOCATION=@{stage_name}/ AUTO_REFRESH = true FILE_FORMAT={file_format_name} "
                                   "PATTERN = '{pattern}' "
                                   .format(external_table=external_table, stage_name=stage_name,
                                           file_format_name=file_format_name, pattern=pattern), cursor)

    def create_external_table(self, original_table, cursor, partitions):
        external_table = original_table + '_EXT_TABLE'
        file_format_name = original_table + '_FILE_FORMAT'
        stage_name = original_table + "_STAGE"
        partition_array = []
        for i in partitions:
            field_format = "{partition_col} STRING AS " \
                           "SPLIT_PART(SPLIT_PART(metadata$filename,'/{partition_col}=',2),'/',1)" \
                .format(partition_col=i)
            partition_array.append(field_format)
        if len(partitions) == 0:
            partition_array.append("file_name STRING AS SPLIT_PART(metadata$filename,'/',-1)")
            partition_by = ""
            select_ext_table_query = "SELECT COUNT(*) AS CNT FROM {external_table} " \
                .format(external_table=external_table)
        else:
            partition_by_fields = ','.join(partitions)
            partition_by = "PARTITION BY ({partition_by_fields})".format(partition_by_fields=partition_by_fields)
            select_ext_table_query = "SELECT {partition_by_fields}, COUNT(*) AS CNT FROM {external_table} " \
                                     "GROUP BY {partition_by_fields}" \
                .format(external_table=external_table, partition_by_fields=partition_by_fields)

        self.execute_sql_by_cursor("CREATE OR REPLACE EXTERNAL TABLE {external_table} ({partition_arr}) "
                                   "{partition_by}"
                                   "LOCATION=@{stage_name}/ FILE_FORMAT={file_format_name}"
                                   .format(external_table=external_table, stage_name=stage_name,
                                           file_format_name=file_format_name, partition_arr=','.join(partition_array),
                                           partition_by=partition_by), cursor)

        self.execute_sql_by_cursor("ALTER EXTERNAL TABLE {external_table} REFRESH"
                                   .format(external_table=external_table), cursor)

        return select_ext_table_query

    def drop_external_table(self, original_table, cursor):
        external_table = original_table + '_EXT_TABLE'

        self.execute_sql_by_cursor("DROP EXTERNAL TABLE {external_table}"
                                   .format(external_table=external_table), cursor)

    def get_type_by_file_format(self, file_format):
        result = re.search('\((.*)\)', file_format)
        type = result.group(1)
        return type

    def get_pattern_by_file_format(self, file_format):
        if "AVRO" in file_format or "avro" in file_format:
            type = 'avro'
        elif "CSV" in file_format or "csv" in file_format:
            type = 'csv'
        else:
            type = 'parquet'
        return type

    def get_retry_count(self, retry):
        retry_count = ''
        if retry > 0:
            retry_count = ' - Retry ' + str(retry)
        return retry_count

    def partition_selection_group(self, selection_group_json):
        select_group_array = []
        for select_group in selection_group_json:
            field = select_group['field']
            operator = select_group['operator']
            if operator == 'BETWEEN':
                select_group_array.append(field + " between '" + "' AND '".join(select_group['value']) + "'")
            elif operator == 'IN':
                select_group_array.append(field + " IN ('" + "','".join(select_group['value']) + "')")
            else:
                logging.error('invalid operator ' + operator)
        return select_group_array

    def partition_treatment_range(self, treatment_range_json):
        treatment_range_array = []
        for select_groups in treatment_range_json:
            select_group_array = self.partition_selection_group(select_groups['selectionGroup'])
            treatment_range_array.append('(' + ' AND '.join(select_group_array) + ')')
        if len(treatment_range_array) == 0:
            where_condition = None
        else:
            where_condition = ' OR '.join(treatment_range_array)
        return where_condition

    def partition_sequence(self, original_table, partitions, s3_path, where_clause, table_columns_orig,
                           file_format, pbd_api_url, is_deidentify, treatment_table, pool_size, max_retry,
                           skip_unload_validation):
        start_time = datetime.datetime.now()
        aws_s3_key, aws_s3_secret_key = self.s3_connection()

        type = self.get_type_by_file_format(file_format)

        cursor = None
        is_external_table_created = False
        count_validation_enabled = False
        select_ext_table_query = None
        boto_conn = S3Connection(aws_s3_key, aws_s3_secret_key)
        copy_template = """COPY INTO '{path}'  FROM 
        (SELECT {table_columns_orig} FROM {original_table} WHERE {where}) 
        CREDENTIALS=(aws_key_id='{aws_s3_key}' aws_secret_key='{aws_s3_secret_key}') {file_format} ;"""
        query = "SELECT DISTINCT {} FROM {} {} ORDER BY {} ASC;".format(','.join(partitions), original_table,
                                                                        where_clause, ','.join(partitions))
        result = self.execute_sql_by_cursor(query, cursor)
        retry = -1
        if result is not None:
            partitions_val = result.fetchall()
            force_skip_unload_validation = True
            query_out, skipped_partitions = self.query_builder(partitions_val, s3_path, partitions, copy_template,
                                                               original_table, where_clause, aws_s3_key,
                                                               aws_s3_secret_key, file_format, table_columns_orig,
                                                               boto_conn, pbd_api_url, retry, is_deidentify,
                                                               force_skip_unload_validation, cursor, treatment_table,
                                                               select_ext_table_query)
            query_out_len = len(query_out)
            while (query_out_len > 0 or count_validation_enabled) and retry < max_retry:
                count_validation_enabled = False
                pool = Pool(pool_size)
                threads = []
                for cmd in query_out:
                    result = pool.apply_async(self.executeSql, (cmd,), callback=self.log_call_back_result)
                    threads.append(result)
                alive = [True] * query_out_len
                try:
                    while any(alive):  # wait until all threads complete
                        for rn in range(query_out_len):
                            alive[rn] = not threads[rn].ready()
                except:  # stop threads if user presses ctrl-c
                    logging.info('trying to stop threads')
                    pool.terminate()
                    logging.info('stopped threads')
                    raise
                if not force_skip_unload_validation and is_external_table_created and query_out_len > 0:
                    self.execute_sql_by_cursor("ALTER EXTERNAL TABLE {original_table}_EXT_TABLE REFRESH"
                                               .format(original_table=original_table), cursor)
                retry_count = self.get_retry_count(retry)
                if is_deidentify:
                    deidentify = 'De-identify '
                else:
                    deidentify = ''
                event = 'Unload {deidentify} Process{retry_count}' \
                    .format(deidentify=deidentify, retry_count=retry_count)
                end_time = datetime.datetime.now()
                duration = end_time - start_time
                message = "Unload Process for partitions completed successfully"
                audit_obj = {
                    "response": "success",
                    "message": message,
                    "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S")
                }

                if not force_skip_unload_validation and is_external_table_created:
                    self.audit_entry(treatment_table, event, start_time, end_time, audit_obj, duration, 'success',
                                     pbd_api_url)
                    start_time = datetime.datetime.now()

                skipped_partitions_previous = skipped_partitions
                query_out, skipped_partitions = self.query_builder(partitions_val, s3_path, partitions, copy_template,
                                                                   original_table, where_clause, aws_s3_key,
                                                                   aws_s3_secret_key, file_format, table_columns_orig,
                                                                   boto_conn, pbd_api_url, retry, is_deidentify,
                                                                   force_skip_unload_validation, cursor,
                                                                   treatment_table, select_ext_table_query)

                if skipped_partitions == 0 or skipped_partitions_previous <= skipped_partitions:
                    retry = retry + 1
                if skipped_partitions == 0:
                    force_skip_unload_validation = skip_unload_validation
                    if not force_skip_unload_validation and not is_external_table_created:
                        cursor = self.get_cursor_for_unload_validation_conn_id()
                        self.create_stage(original_table + '_file_format', type, original_table + '_stage', s3_path,
                                          aws_s3_key, aws_s3_secret_key, cursor)
                        select_ext_table_query = self.create_external_table(original_table, cursor, partitions)
                        is_external_table_created = True
                        count_validation_enabled = True

                query_out_len = len(query_out)
        if not skip_unload_validation and is_external_table_created:
            self.drop_stage(original_table, cursor)
            self.drop_external_table(original_table, cursor)
        if query_out_len > 0 and retry >= max_retry:
            raise ValueError('Exceeded the maximum retry')
        return


class DtfPlugin(AirflowPlugin):
    name = "dtf_plugin"
    operators = [DtfOperator]
