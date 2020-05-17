import time
import subprocess
from boto import boto
import logging
import batch_common
from datetime import datetime
import sys

# XXX Pull this from the configuration file, instead
LOCAL_PATH = '/tmp/'


class SqoopTools(object):
    def __init__(self, env, conf, exception=None, oracle=None, mysql=None):
        self.env = env
        self.batch = str(int(time.time()))
        self.pyprofile_path = conf.get('code_repo', 'profile')
        self.db_user = str(conf.get('oracle_conn', 'db_user'))
        self.db_pwd_path = str(conf.get('oracle_conn', 'db_path')) + self.env + '/.password'
        self.db_conn_opt = " --username " + self.db_user + " --password-file " + self.db_pwd_path
        self.conf = conf
        self.exception = exception
        self.oracle = oracle
        self.mysql = mysql

    def get_sqoop_properties(self, properties, load_type):
        """
        get the sqoop properties from the properties file including the constructed sqoop command into the dictionary
        :param properties:
        :param load_type:
        :return: dictionary of sqoop properties
        """

        logging.info('getting sqoop properties')
        logging.info("properties passed in file: %s", properties)

        # initialize sqoop_prop with load type
        sqoop_prop = {'LOAD_TYPE': load_type.upper()}

        # SQOOP_JOB_NAME
        if properties['SQOOP_JOB_NAME'] is None:
            raise self.exception("SQOOP_JOB_NAME is not passed in the Sqoop properties file")
        else:
            sqoop_prop['SQOOP_JOB_NAME'] = properties['SQOOP_JOB_NAME']

        # SCHEMA
        if properties['SCHEMA_NAME'] is None:
            raise self.exception("SCHEMA_NAME is not passed in the Sqoop properties file")
        else:
            sqoop_prop['SCHEMA'] = properties['SCHEMA_NAME']

        # TABLE_VIEW_NAME
        if properties['TABLE_VIEW_NAME'] is None:
            raise self.exception("TABLE_VIEW_NAME is not passed in the Sqoop properties file")
        else:
            sqoop_prop['TABLE_VIEW_NAME'] = properties['TABLE_VIEW_NAME']

        # IS_VIEW
        if properties['IS_VIEW'] is None:
            raise self.exception("IS_VIEW is not passed in the Sqoop properties file")
        else:
            sqoop_prop['VIEW_FLAG'] = properties['IS_VIEW'].upper()

        # IS_QUERY
        try:
            if properties['IS_QUERY'] is None:
                sqoop_prop['QUERY_FLAG'] = "FALSE"
            else:
                sqoop_prop['QUERY_FLAG'] = properties['IS_QUERY'].upper()
        except KeyError:
            sqoop_prop['QUERY_FLAG'] = "FALSE"

        try:
            if sqoop_prop['QUERY_FLAG'] == "TRUE":
                if properties['QUERY_STR'] is None:
                    raise self.exception("IS_QUERY is not passed in the Sqoop properties file")
                else:
                    # put $CONDITION back to the QUERY after where
                    sqoop_prop['SOURCE'] = ' --query "' + properties['QUERY_STR'].replace(' where ',
                                                                                          ' where $CONDITIONS ') + '"'
            else:
                sqoop_prop['SOURCE'] = " --table " + properties['SCHEMA_NAME'] + '.' + properties['TABLE_VIEW_NAME']
        except KeyError:
            sqoop_prop['SOURCE'] = " --table " + properties['SCHEMA_NAME'] + '.' + properties['TABLE_VIEW_NAME']

        try:
            if properties['SPLIT_BY_COLUMN'] is None:
                if sqoop_prop['QUERY_FLAG'] == "TRUE":
                    raise self.exception("QUERY_FLAG is True, but SPLIT_BY_COLUMN is not passed in property file")
                else:
                    if sqoop_prop['VIEW_FLAG'] == "TRUE":
                        raise self.exception("VIEW_FLAG is True, but SPLIT_BY_COLUMN is not passed in property file")
                    else:
                        sqoop_prop['SPLIT_BY_COLUMN'] = ""
            else:
                if sqoop_prop['QUERY_FLAG'] == "TRUE" or sqoop_prop['VIEW_FLAG'] == "TRUE":
                    sqoop_prop['SPLIT_BY_COLUMN'] = " --split-by " + properties['SPLIT_BY_COLUMN']
                else:
                    sqoop_prop['SPLIT_BY_COLUMN'] = ""
        except KeyError:
            sqoop_prop['SPLIT_BY_COLUMN'] = ""

        # DATABASE_NAME for DATABASE CONNECTION
        if properties['DATABASE_NAME'] is None:
            raise self.exception("DATABASE_NAME is not passed in the Sqoop properties file")
        else:
            sqoop_prop['DB_CONN'] = properties['DATABASE_NAME']

        # TARGET_DIRECTORY
        sqoop_prop['TARGET_IS_S3'] = properties['TARGET_IS_S3']
        if properties['TARGET_DIRECTORY'] is None:
            raise self.exception("TARGET_DIRECTORY is not passed in the Sqoop properties file")
        else:
            if properties['TARGET_IS_S3']:
                sqoop_prop['SQP_TGT_DIRECTORY'] = " --target-dir " + "s3://" + properties['TARGET_DIRECTORY']
                sqoop_prop['S3_TARGET_DIRECTORY'] = "s3://" + properties['TARGET_DIRECTORY']
                sqoop_prop['S3_TGT_FOLDER_FILE'] = "s3://" + properties['TARGET_DIRECTORY'] + "_$folder$"
                sqoop_prop['SQOOP_OP_DIR_FLAG'] = "S3"

            else:
                sqoop_prop['SQP_TGT_DIRECTORY'] = " --target-dir " + properties[
                    'TARGET_DIRECTORY'] + " --delete-target-dir "
                sqoop_prop['S3_TARGET_DIRECTORY'] = ""
                sqoop_prop['S3_TGT_FOLDER_FILE'] = ""
                sqoop_prop['SQOOP_OP_DIR_FLAG'] = "HDFS"

        # COLUMNS
        try:
            if properties['COLUMNS'] is None:
                sqoop_prop['COLUMNS'] = ""
            else:
                sqoop_prop['COLUMNS'] = " --columns " + properties['COLUMNS']
        except KeyError:
            sqoop_prop['COLUMNS'] = ""

        # COL_CONVERSION
        try:
            if properties['COL_CONVERSION'] is None:
                sqoop_prop['COL_CONVERSION'] = ""
            else:
                sqoop_prop['COL_CONVERSION'] = " --map-column-java " + properties['COL_CONVERSION']
        except KeyError:
            sqoop_prop['COL_CONVERSION'] = ""

        # MAPPER_COUNT default mapper count to 4
        try:
            if properties['MAPPER_COUNT'] is None:
                sqoop_prop['MAPPER_COUNT'] = " --num-mappers 4"
            else:
                sqoop_prop['MAPPER_COUNT'] = " --num-mappers " + properties['MAPPER_COUNT']
        except KeyError:
            sqoop_prop['MAPPER_COUNT'] = " --num-mappers 4"

        # LOG_TRACING_LEVEL default to verbose
        try:
            if properties['LOG_TRACING_LEVEL'] is None or properties['LOG_TRACING_LEVEL'] == "--verbose":
                sqoop_prop['LOG_TRACING_LEVEL'] = " --verbose "
            else:
                sqoop_prop['LOG_TRACING_LEVEL'] = ""
        except KeyError:
            sqoop_prop['LOG_TRACING_LEVEL'] = " --verbose "

        sqoop_prop['COMPRESS'] = " --compression-codec org.apache.hadoop.io.compress.SnappyCodec"

        # incremental load
        # validate incremental columns and days to extract provided for incremental load
        if load_type == "INC":
            try:
                if properties['COL_INCREMENTAL'] is None:
                    raise self.exception("Need to provide incremental column for Incremental load")
                else:
                    sqoop_prop['COL_INCREMENTAL_NAME'] = properties['COL_INCREMENTAL']
            except KeyError:
                raise self.exception("Need to provide incremental column for Incremental load")

            try:
                if properties['DAYS_TO_EXTRACT'] is None:
                    raise self.exception("Need to provide day to extract for Incremental load")
                else:
                    sqoop_prop['DAYS_TO_EXTRACT'] = properties['DAYS_TO_EXTRACT']
            except KeyError:
                raise self.exception("Need to provide day to extract for Incremental load")

        else:
            sqoop_prop['COL_INCREMENTAL_NAME'] = ""
            try:
                sqoop_prop['DAYS_TO_EXTRACT'] = properties['DAYS_TO_EXTRACT']
            except KeyError:
                sqoop_prop['DAYS_TO_EXTRACT'] = ""

        # STREAM_TOLERANCE
        try:
            if properties['STREAM_TOLERANCE'] is None:
                sqoop_prop['STREAM_TOLERANCE'] = 0
            else:
                sqoop_prop['STREAM_TOLERANCE'] = properties['STREAM_TOLERANCE']
        except KeyError:
            sqoop_prop['STREAM_TOLERANCE'] = 0

        sqoop_prop['OUTPUT_FILE_FORMAT'] = None
        # checking for different output file formats
        if 'DATA_OUTPUT_FILE_FORMAT' in properties:
            if properties['DATA_OUTPUT_FILE_FORMAT'] is not None and (
                            properties['DATA_OUTPUT_FILE_FORMAT'].upper() == "PARQUET" or properties[
                        'DATA_OUTPUT_FILE_FORMAT'].upper() == "SEQUENCE") and properties['TARGET_IS_S3']:
                raise self.exception(
                    "sqoop data output location should be HDFS not S3,"
                    "for given" + properties['DATA_OUTPUT_FILE_FORMAT'] + " sqoop doesn't support load to s3")
            elif properties['DATA_OUTPUT_FILE_FORMAT'] is not None and properties[
                'DATA_OUTPUT_FILE_FORMAT'].upper() == "PARQUET":
                sqoop_prop['OUTPUT_FILE_FORMAT'] = ' --as-parquetfile '
            elif properties['DATA_OUTPUT_FILE_FORMAT'] is not None and properties[
                'DATA_OUTPUT_FILE_FORMAT'].upper() == "SEQUENCE":
                sqoop_prop['OUTPUT_FILE_FORMAT'] = ' --as-sequencefile '
            elif properties['DATA_OUTPUT_FILE_FORMAT'] is not None and properties[
                'DATA_OUTPUT_FILE_FORMAT'].upper() == "AVRO":
                sqoop_prop['OUTPUT_FILE_FORMAT'] = ' --as-avrodatafile '

        # validate values are provided for avro file format
        try:
            is_avro = properties['IS_AVRO'].upper()
        except KeyError:
            is_avro = None
        try:
            field_delimiter = properties['FIELD_DELIMITER']
        except KeyError:
            field_delimiter = None

        if sqoop_prop['OUTPUT_FILE_FORMAT'] is None:
            if is_avro is None or is_avro == "TRUE":
                sqoop_prop["OUTPUT_FILE_FORMAT"] = " --as-avrodatafile "
            elif field_delimiter is None:
                raise self.exception("Since the file format is not avro, need to provide field delimiter")
            else:
                sqoop_prop['OUTPUT_FILE_FORMAT'] = " --fields-terminated-by ' " + properties[
                    'FIELD_DELIMITER'] + "' --hive-drop-import-delims "

        if field_delimiter is not None:
            sqoop_prop['FIELD_DELIMITER_DEFAULT_OVERRIDE'] = " --null-string '\\N' --null-non-string '\\N' "
            logging.info("preventing null values for field delimited output format : FIELD_DELIMITER_DEFAULT_OVERRIDE")
        else:
            sqoop_prop['FIELD_DELIMITER_DEFAULT_OVERRIDE'] = ""

        sqoop_prop['CompVal'] = "--compression-codec org.apache.hadoop.io.compress.SnappyCodec"

        if sqoop_prop.get('QUERY_FLAG') == "TRUE":
            pre_option = "import -D oraoop.jdbc.url.verbatim=true -D oraoop.timestamp.string=false " \
                         "-D fs.s3a.connection.timeout=200000 -D fs.s3n.multipart.uploads.enabled=true --direct --connect"
            pre_option_field_delimiter = "import -D mapred.child.java.opts=-Djava.security.egd=file:/dev/../dev/urandom" \
                                         " -D oraoop.jdbc.url.verbatim=true -D" \
                                         " sqoop.connection.factories=com.quest.oraoop.OraOopManagerFactory " \
                                         "-D fs.s3n.multipart.uploads.enabled=true -D oraoop.timestamp.string=false " \
                                         "--connect"

            post_option = sqoop_prop['SOURCE'] + " " + sqoop_prop['SQP_TGT_DIRECTORY'] + " " + sqoop_prop[
                'SPLIT_BY_COLUMN'] \
                          + " " + sqoop_prop['MAPPER_COUNT'] + " " + sqoop_prop["OUTPUT_FILE_FORMAT"] + " " + \
                          sqoop_prop['COLUMNS'] + " " + sqoop_prop['COL_CONVERSION'] + " " + \
                          sqoop_prop['LOG_TRACING_LEVEL'] + " " + sqoop_prop["CompVal"]

            if properties.get('FIELD_DELIMITER') is not None:
                sqoop_cmd = pre_option_field_delimiter + " " + sqoop_prop['DB_CONN'] + " " + self.db_conn_opt + " " + \
                            post_option + " " + sqoop_prop['FIELD_DELIMITER_DEFAULT_OVERRIDE']
            else:
                sqoop_cmd = pre_option + " " + sqoop_prop['DB_CONN'] + self.db_conn_opt + " " + post_option

        elif sqoop_prop.get('VIEW_FLAG') == 'FALSE' and load_type == 'FULL':
            pre_option = "import -D oraoop.jdbc.url.verbatim=true -D oraoop.timestamp.string=false " \
                         "-D fs.s3a.connection.timeout=200000 -D fs.s3n.multipart.uploads.enabled=true --direct " \
                         "--connect"
            pre_option_field_delimiter = "import -D mapred.child.java.opts=-Djava.security.egd=file:/dev/../dev/" \
                                         "urandom -D " \
                                         "fs.s3n.multipart.uploads.enabled=true -D oraoop.jdbc.url.verbatim=true -D " \
                                         "sqoop.connection.factories=" \
                                         "com.quest.oraoop.OraOopManagerFactory -D oraoop.timestamp.string=false" \
                                         " --connect"
            post_option = sqoop_prop['SOURCE'] + " " + sqoop_prop['SQP_TGT_DIRECTORY'] + " " + sqoop_prop[
                'MAPPER_COUNT'] \
                          + " " + sqoop_prop["OUTPUT_FILE_FORMAT"] + " " + sqoop_prop['COLUMNS'] + " " + \
                          sqoop_prop['COL_CONVERSION'] + " " + sqoop_prop['LOG_TRACING_LEVEL'] + " " + sqoop_prop[
                              "CompVal"]

            if properties.get('FIELD_DELIMITER') is not None:
                sqoop_cmd = pre_option_field_delimiter + " " + sqoop_prop['DB_CONN'] + " " + self.db_conn_opt + " " + \
                            post_option + " " + sqoop_prop['FIELD_DELIMITER_DEFAULT_OVERRIDE']
            else:
                sqoop_cmd = pre_option + " " + sqoop_prop['DB_CONN'] + self.db_conn_opt + " " + post_option

        elif sqoop_prop.get('VIEW_FLAG') == 'FALSE' and load_type == 'INC':
            pre_option = "import -D oraoop.jdbc.url.verbatim=true -D oraoop.timestamp.string=false " \
                         "-D fs.s3n.multipart.uploads.enabled=true -D fs.s3a.connection.timeout=200000" \
                         " -D oraoop.table.import.where.clause.location=SUBSPLIT --direct --connect"
            pre_option_field_delimiter = "import -D mapred.child.java.opts=-Djava.security.egd=file:/dev/../dev/" \
                                         "urandom -D fs.s3n.multipart.uploads.enabled=true -D " \
                                         "oraoop.jdbc.url.verbatim=true -D sqoop.connection.factories=" \
                                         "com.quest.oraoop.OraOopManagerFactory -D oraoop.timestamp.string=false " \
                                         "--connect "
            post_option = sqoop_prop['SOURCE'] + " --where"

            post_option_1 = sqoop_prop['SQP_TGT_DIRECTORY'] + " " + sqoop_prop['MAPPER_COUNT'] \
                            + " " + sqoop_prop["OUTPUT_FILE_FORMAT"] + " " + sqoop_prop['COLUMNS'] + " " + \
                            sqoop_prop['COL_CONVERSION'] + " " + sqoop_prop['LOG_TRACING_LEVEL'] + " " + \
                            sqoop_prop["CompVal"]

            cmd = pre_option + " " + sqoop_prop['DB_CONN'] + " " + self.db_conn_opt + " " + post_option + " " + \
                  properties['COL_INCREMENTAL'] + ">=trunc(sysdate)-" + sqoop_prop['DAYS_TO_EXTRACT'] \
                  + post_option_1
            cmd_field_delimiter = pre_option_field_delimiter + " " + sqoop_prop['DB_CONN'] + self.db_conn_opt + \
                                  " " + post_option + "'" + properties['COL_INCREMENTAL'] + \
                                  ">=trunc(sysdate)-" + sqoop_prop['DAYS_TO_EXTRACT'] + post_option_1

            if properties.get('FIELD_DELIMITER') is not None:
                sqoop_cmd = cmd_field_delimiter + " " + sqoop_prop['FIELD_DELIMITER_DEFAULT_OVERRIDE']
            else:
                sqoop_cmd = cmd

        elif sqoop_prop.get('VIEW_FLAG') == 'FALSE' and load_type == 'INC':
            pre_option = "import -D oraoop.jdbc.url.verbatim=true -D fs.s3n.multipart.uploads.enabled=true -D oraoop.timestamp.string=false " \
                         "-D fs.s3a.connection.timeout=200000 --direct --connect"
            pre_option_field_delimiter = "import -D mapred.child.java.opts=-Djava.security.egd=file:/dev/../dev/" \
                                         "urandom -D fs.s3n.multipart.uploads.enabled=true -D oraoop.jdbc.url.verbatim=true -D oraoop.timestamp.string=false" \
                                         " --connect "

            post_option = sqoop_prop['SOURCE'] + " --where " + properties['COL_INCREMENTAL'] + ">=trunc(sysdate)-" + \
                          sqoop_prop['DAYS_TO_EXTRACT'] + " " + sqoop_prop['SQP_TGT_DIRECTORY'] + " " + \
                          sqoop_prop['MAPPER_COUNT'] + " " + sqoop_prop['SPLIT_BY_COLUMN'] + \
                          sqoop_prop["OUTPUT_FILE_FORMAT"] + " " + sqoop_prop['COLUMNS'] + " " + \
                          sqoop_prop['COL_CONVERSION'] + " " + sqoop_prop['LOG_TRACING_LEVEL'] + " " \
                          + sqoop_prop["CompVal"]

            if properties.get('FIELD_DELIMITER') is not None:
                sqoop_cmd = pre_option_field_delimiter + " " + sqoop_prop['DB_CONN'] + " " + self.db_conn_opt + " " + \
                            post_option + " " + sqoop_prop['FIELD_DELIMITER_DEFAULT_OVERRIDE']
            else:
                sqoop_cmd = pre_option + " " + sqoop_prop['DB_CONN'] + self.db_conn_opt + " " + post_option

        else:
            pre_option = "import -D oraoop.jdbc.url.verbatim=true -D oraoop.timestamp.string=false " \
                         " -D fs.s3n.multipart.uploads.enabled=true -D fs.s3a.connection.timeout=200000 --direct --connect "
            pre_option_field_delimiter = "import -D mapred.child.java.opts=-Djava.security.egd=file:/dev/../dev/" \
                                         "urandom -D fs.s3n.multipart.uploads.enabled=true -D oraoop.jdbc.url.verbatim=true -D oraoop.timestamp.string=false" \
                                         " --connect"

            post_option = sqoop_prop['SOURCE'] + " " + sqoop_prop['SQP_TGT_DIRECTORY'] + " " + \
                          sqoop_prop['MAPPER_COUNT'] + " " + sqoop_prop['SPLIT_BY_COLUMN'] + " " + \
                          sqoop_prop["OUTPUT_FILE_FORMAT"] + " " + sqoop_prop['COLUMNS'] + " " + \
                          sqoop_prop['COL_CONVERSION'] + " " + sqoop_prop['LOG_TRACING_LEVEL'] + " " + \
                          sqoop_prop["CompVal"]
            if properties.get('FIELD_DELIMITER') is not None:
                sqoop_cmd = pre_option_field_delimiter + " " + sqoop_prop['DB_CONN'] + " " + self.db_conn_opt + " " + \
                            post_option + " " + sqoop_prop['FIELD_DELIMITER_DEFAULT_OVERRIDE']
            else:
                sqoop_cmd = pre_option + " " + sqoop_prop['DB_CONN'] + self.db_conn_opt + " " + post_option

        logging.info("Sqoop prop built: %s", sqoop_prop)

        sqoop_prop['SQOOP_CMD'] = sqoop_cmd

        # Table_count_query
        if sqoop_prop.get('QUERY_FLAG') == 'TRUE':
            query_str = properties.get('QUERY_STR').strip().upper().replace("\"", "").replace("\\", "") \
                .replace("WHERE AND", "WHERE")
            sql = "SELECT  /*+ parallel(4) */ count(*) as COUNT FROM (" + query_str + ")"
        else:
            if sqoop_prop['LOAD_TYPE'] == "FULL":
                sql = "SELECT /*+ parallel(4) */ count(*) as COUNT FROM " + properties['SCHEMA_NAME'] + "." \
                      + properties['TABLE_VIEW_NAME']
            else:
                sql = "SELECT /*+ parallel(4) */ count(*) as COUNT FROM " + properties['SCHEMA_NAME'] + "." \
                      + properties['TABLE_VIEW_NAME'] + " WHERE " + properties[
                          'COL_INCREMENTAL'] + ">= trunc(sysdate)-" + \
                      properties['DAYS_TO_EXTRACT']

        sqoop_prop['SQL'] = sql
        logging.info("get prop: %s", sqoop_prop)
        logging.info('SQL ' + sqoop_prop['SQL'])
        logging.info('SQOOP_CMD ' + sqoop_prop['SQOOP_CMD'])

        return sqoop_prop

    def get_inc_merge_properties(self, properties):
        """
        get the incremental merge properties from the properties file including the constructed
        sqoop command into the dictionary
        :param properties:
        :return: dictionary of incermental merge properties
        """

        global condition
        logging.info('getting incremental properties')
        logging.info("properties passed in file: %s", properties)

        inc_merge_prop = {}

        # JOB_NAME
        if properties.get('JOB_NAME') is None:
            raise self.exception("JOB_NAME is not passed in the Incremental merge properties file")
        else:
            inc_merge_prop['JOB_NAME'] = properties['JOB_NAME']
        # JOB_DEC
        inc_merge_prop['JOB_DESC'] = inc_merge_prop['JOB_NAME']

        # EMAIL_LIST
        if properties.get('EMAIL_LIST') is None:
            raise self.exception("EMAIL_LIST is not passed in the Incremental merge  properties file")
        else:
            inc_merge_prop['EMAIL_LIST_ID'] = properties['EMAIL_LIST']

        # EMAIL_ON_SUCCESS
        if properties.get('EMAIL_ON_SUCCESS') is None:
            inc_merge_prop['SUCCESS_EMAIL_FLAG'] = 'FALSE'
        else:
            inc_merge_prop['SUCCESS_EMAIL_FLAG'] = properties['EMAIL_ON_SUCCESS']
        logging.info("INC MERGE PROPERTIES2: %s", inc_merge_prop)
        # HADOOP_DATABASE_NAME
        if properties.get('HADOOP_DATABASE_NAME') is None:
            raise self.exception("HADOOP_DATABASE_NAME is not passed in the Incremental merge properties file")
        else:
            inc_merge_prop['SCHEMA_NAME'] = properties['HADOOP_DATABASE_NAME'].upper()

        # HADOOP_STG_DB_NAME
        if properties.get('HADOOP_STG_DB_NAME') is None:
            raise self.exception("HADOOP_STG_DB_NAME is not passed in the Incremental merge  properties file")
        else:
            inc_merge_prop['STG_SCHEMA_NAME'] = properties['HADOOP_STG_DB_NAME']

        # BASE_TABLE_NAME
        if properties.get('BASE_TABLE_NAME') is None:
            raise self.exception("HADOOP_STG_DB_NAME is not passed in the Incremental merge  properties file")
        else:
            inc_merge_prop['BASE_TABLE_NAME'] = properties['BASE_TABLE_NAME']

        # INC_TABLE_NAME
        inc_merge_prop['INC_TABLE_NAME'] = inc_merge_prop['BASE_TABLE_NAME'] + "_inc"

        # HDFS_BASE_TABLE_DIR
        if properties.get('HDFS_BASE_TABLE_DIR') is None:
            raise self.exception("HDFS_BASE_TABLE_DIR is not passed in the Incremental merge  properties file")
        else:
            inc_merge_prop['BASE_SOURCE_DIRECTORY'] = properties['HDFS_BASE_TABLE_DIR']

        # HDFS_BASE_TABLE_DIR
        inc_merge_prop['HDFS_BASE_TABLE_DIR'] = inc_merge_prop['BASE_SOURCE_DIRECTORY']

        # HDFS_INC_TABLE_DIR
        if properties.get('HDFS_INC_TABLE_DIR') is None:
            raise self.exception("HADOOP_STG_DB_NAME is not passed in the Incremental merge  properties file")
        else:
            inc_merge_prop['INC_SOURCE_DIRECTORY'] = properties['HDFS_INC_TABLE_DIR']
        # HDFS_INC_TABLE_DIR
        inc_merge_prop['HDFS_INC_TABLE_DIR'] = inc_merge_prop['INC_SOURCE_DIRECTORY']

        # S3_ARCH_TABLE_DIR
        if properties.get('S3_ARCH_TABLE_DIR') is None:
            raise self.exception("S3_ARCH_TABLE_DIR is not passed in the Incremental merge  properties file")
        else:
            inc_merge_prop['S3_ARCH_TABLE_DIR'] = properties['S3_ARCH_TABLE_DIR']

        # PARTITION_CODE
        if properties.get('PARTITION_CODE') is None:
            raise self.exception("PARTITION_CODE is not passed in the Incremental merge  properties file")
        else:
            inc_merge_prop['PARTITION_CODE'] = properties['PARTITION_CODE']

        # PARTITION_COL_NAME
        if properties.get('PARTITION_COL_NAME') is None:
            raise self.exception("PARTITION_COL_NAME is not passed in the Incremental merge  properties file")
        else:
            inc_merge_prop['PARTITION_COL_NAME'] = properties['PARTITION_COL_NAME']

        # REDUCER_COUNT
        if properties.get('REDUCER_COUNT') is not None:
            inc_merge_prop['SET_RED_CNT'] = "set mapred.reduce.tasks=" + properties['PARTITION_COL_NAME'] + ";"

        # PARTITION_COUNT_PER_NODE
        if properties.get('PARTITION_COUNT_PER_NODE') is not None:
            inc_merge_prop['SET_PART_CNT'] = "set hive.exec.max.dynamic.partitions.pernode=" + \
                                             properties['PARTITION_COUNT_PER_NODE'] + ";"

        # MAX_PARTITION_COUNT
        if properties.get('MAX_PARTITION_COUNT') is not None:
            inc_merge_prop['SET_MAX_PART_CNT'] = "set hive.exec.max.dynamic.partitions=" + \
                                                 properties['MAX_PARTITION_COUNT'] + ";"
        logging.info("INC MERGE PROPERTIES4: %s", inc_merge_prop)
        # MAX_CREATED_FILES
        if properties.get('MAX_CREATED_FILES') is not None:
            inc_merge_prop['SET_MAX_CRT_FILES'] = "set hive.exec.max.created.files=" + properties[
                'MAX_CREATED_FILES'] + ";"

        # MAPREDUCE_TIMEOUT
        if properties.get('MAPREDUCE_TIMEOUT') is not None:
            inc_merge_prop['SET_MAPREDUCE_TIMEOUT'] = "set mapreduce.task.timeout=" + properties[
                'MAPREDUCE_TIMEOUT'] + ";"

        # MAPREDUCE_MAP_MAXATTEMPTS
        if properties.get('MAPREDUCE_MAP_MAXATTEMPTS') is not None:
            inc_merge_prop['SET_MAPREDUCE_MAP_MAXATTEMPTS'] = "set mapreduce.map.maxattempts=" + \
                                                              properties['MAPREDUCE_MAP_MAXATTEMPTS'] + ";"

        # TYPE2_COLUMN_LIST
        if properties.get('TYPE2_COLUMN_LIST') is not None:
            inc_merge_prop['TYPE2_COLUMN_LIST'] = properties['TYPE2_COLUMN_LIST']

        # TYPE2_KEY_FIELD
        if properties.get('TYPE2_KEY_FIELD') is not None:
            inc_merge_prop['TYPE2_KEY_FIELD'] = properties['TYPE2_KEY_FIELD']

        # TYPE2_DATE_FIELD
        if properties.get('TYPE2_DATE_FIELD') is not None:
            inc_merge_prop['TYPE2_DATE_FIELD'] = properties['TYPE2_DATE_FIELD']

        # JOIN_COLUMN_LIST
        if properties.get('JOIN_COLUMN_LIST') is not None and len(properties['JOIN_COLUMN_LIST']) > 0:
            inc_merge_prop['JOIN_COLUMN_LIST'] = properties['JOIN_COLUMN_LIST']
        else:
            raise self.exception("JOIN_COLUMN_LIST is not passed in the Incremental merge  properties file")
        # CONDITION
        join_column_list = properties['JOIN_COLUMN_LIST']
        total_del_count = len(join_column_list.split(","))
        columns_list = join_column_list.split(",")
        print()
        "total number of comma delimiters are " + str(total_del_count)
        n = 0
        while n < total_del_count:
            print()
            var_columns_list = columns_list[n]
            if n == 0:
                condition = "t1." + var_columns_list + "=t2." + var_columns_list
            else:
                condition = condition + " and t1." + var_columns_list + "=t2." + var_columns_list
            n += 1
        inc_merge_prop['CONDITION'] = condition

        # JOIN_NVL_COLUMN_LIST
        if properties.get('JOIN_NVL_COLUMN_LIST') is not None and len(properties['JOIN_NVL_COLUMN_LIST']) > 0:
            join_nvl_column_list = properties['JOIN_NVL_COLUMN_LIST']
            total_del_count = len(join_nvl_column_list.split(","))
            nvl_columns_list = join_column_list.split(",")
            n = 0
            while n < total_del_count:
                condition = condition + " and nvl(t1." + nvl_columns_list[n] + ",'0')=nvl(t2." + nvl_columns_list[
                    n] + ",'0')"
                n += 1
        inc_merge_prop['CONDITION'] = condition

        # LOG_RETENTION_PERIOD
        if properties.get('LOG_RETENTION_PERIOD') is None:
            inc_merge_prop['LOG_RETENTION_PERIOD'] = 10
        else:
            inc_merge_prop['LOG_RETENTION_PERIOD'] = properties.get('LOG_RETENTION_PERIOD')

        # TARGET_IS_S3
        if properties.get('TARGET_IS_S3') is None:
            inc_merge_prop['TARGET_IS_S3'] = 'FALSE'
        else:
            inc_merge_prop['TARGET_IS_S3'] = properties.get('TARGET_IS_S3')
        return inc_merge_prop

    def get_full_load_properties(self, properties):
        """
        get the incremental merge properties from the properties file including the constructed
        sqoop command into the dictionary
        :param properties:
        :return: dictionary of incermental merge properties
        """

        logging.info('getting incremental properties')
        logging.info("properties passed in file: %s", properties)

        full_load_prop = {}

        # JOB_NAME
        if properties.get('JOB_NAME') is None:
            raise self.exception("JOB_NAME is not passed in the full load properties file")
        else:
            full_load_prop['JOB_NAME'] = properties['JOB_NAME']

        # JOB_DEC
        full_load_prop['JOB_DESC'] = full_load_prop['JOB_NAME']

        # EMAIL_LIST
        if properties.get('EMAIL_LIST') is None:
            raise self.exception("EMAIL_LIST is not passed in the full load  properties file")
        else:
            full_load_prop['EMAIL_LIST_ID'] = properties['EMAIL_LIST']

        # EMAIL_ON_SUCCESS
        if properties.get('EMAIL_ON_SUCCESS') is None:
            full_load_prop['SUCCESS_EMAIL_FLAG'] = 'FALSE'
        else:
            full_load_prop['SUCCESS_EMAIL_FLAG'] = properties['EMAIL_ON_SUCCESS']

        # HADOOP_BASE_DB_NAME
        if properties.get('HADOOP_BASE_DB_NAME') is None:
            raise self.exception("HADOOP_BASE_DB_NAME is not passed in the full load properties file")
        else:
            full_load_prop['HADOOP_BASE_DB_NAME'] = properties['HADOOP_BASE_DB_NAME'].upper()

        # HADOOP_STG_DB_NAME
        if properties.get('HADOOP_STG_DB_NAME') is None:
            raise self.exception("HADOOP_STG_DB_NAME is not passed in the full load  properties file")
        else:
            full_load_prop['HADOOP_STG_DB_NAME'] = properties['HADOOP_STG_DB_NAME']

        # BASE_TABLE_NAME
        if properties.get('BASE_TABLE_NAME') is None:
            raise self.exception("HADOOP_STG_DB_NAME is not passed in the full load  properties file")
        else:
            full_load_prop['BASE_TABLE_NAME'] = properties['BASE_TABLE_NAME']

        # STG_TABLE_NAME
        if properties.get('STG_TABLE_NAME') is None:
            raise self.exception("STG_TABLE_NAME is not passed in the full load  properties file")
        else:
            full_load_prop['STG_TABLE_NAME'] = properties['STG_TABLE_NAME']

        # BASE_DIRECTORY
        if properties.get('BASE_DIRECTORY') is None:
            raise self.exception("BASE_DIRECTORY is not passed in the full load  properties file")
        else:
            full_load_prop['BASE_DIRECTORY'] = properties['BASE_DIRECTORY']

        # STG_DIRECTORY
        if properties.get('STG_DIRECTORY') is None:
            raise self.exception("STG_DIRECTORY is not passed in the full load  properties file")
        else:
            full_load_prop['STG_DIRECTORY'] = properties['STG_DIRECTORY']

        # ARC_DIRECTORY
        if properties.get('ARC_DIRECTORY') is None:
            raise self.exception("ARC_DIRECTORY is not passed in the full load  properties file")
        else:
            full_load_prop['ARC_DIRECTORY'] = properties['ARC_DIRECTORY']

        # LOG_RETENTION_PERIOD
        if properties.get('LOG_RETENTION_PERIOD') is None:
            full_load_prop['LOG_RETENTION_PERIOD'] = 10
        else:
            full_load_prop['LOG_RETENTION_PERIOD'] = properties.get('LOG_RETENTION_PERIOD')

        # TARGET_IS_S3
        if properties.get('TARGET_IS_S3') is None:
            full_load_prop['TARGET_IS_S3'] = 'FALSE'
        else:
            full_load_prop['TARGET_IS_S3'] = properties.get('TARGET_IS_S3')

        # PARTITION_FLAG
        if properties.get('PARTITION_FLAG') is None:
            full_load_prop['PARTITION_FLAG'] = 'FALSE'
        else:
            full_load_prop['PARTITION_FLAG'] = properties.get('PARTITION_FLAG')

        if full_load_prop['PARTITION_FLAG'] == 'TRUE':
            if properties.get('PARTITION_CODE') is None:
                raise self.exception("PARTITION_CODE is not passed in the full load  properties file")
            else:
                full_load_prop['PARTITION_CODE'] = properties.get('PARTITION_CODE')
            if properties.get('PARTITION_COL_NAME') is None:
                raise self.exception("PARTITION_COL_NAME is not passed in the full load  properties file")
            else:
                full_load_prop['PARTITION_COL_NAME'] = properties.get('PARTITION_COL_NAME')

        PARTITION_CODE_LOGIC = ''
        PARTITION_COL_LOGIC = ''
        drop_partition = ''
        if full_load_prop.get('PARTITION_FLAG') == 'TRUE':
            PARTITION_CODE_LOGIC = "," + full_load_prop['PARTITION_CODE']
            drop_partition = "alter table " + full_load_prop['HADOOP_BASE_DB_NAME'] + "." + \
                             full_load_prop['BASE_TABLE_NAME'] + " drop partition (" + \
                             full_load_prop['PARTITION_COL_NAME'] + " > '0');"

        if full_load_prop.get('PARTITION_COL_NAME') is not None:
            PARTITION_COL_LOGIC = " partition(" + full_load_prop['PARTITION_COL_NAME'] + ")"

        if properties.get('DAILY_SNAPSHOT') == 'TRUE':
            currentTimeStamp = str(datetime.now().strftime('%Y-%m-%d'))
            PARTITION_CODE_LOGIC = ""
            PARTITION_COL_LOGIC = " partition(" + full_load_prop['PARTITION_COL_NAME'] + "='" + currentTimeStamp + "')"

        hive_prop = "-e  \"set hive.exec.dynamic.partition.mode=nonstrict;set avro.output.codec=snappy;" \
                    "set hive.exec.compress.intermediate=true;set hive.exec.compress.output=true;" \
                    "set hive.merge.mapredfiles=true;set hive.mapred.mode=nonstrict;"

        full_load = hive_prop + drop_partition + " insert overwrite table " + full_load_prop[
            'HADOOP_BASE_DB_NAME'] + "." + \
                    full_load_prop['BASE_TABLE_NAME'] + PARTITION_COL_LOGIC + " select a.* " + \
                    PARTITION_CODE_LOGIC + " from " + full_load_prop['HADOOP_STG_DB_NAME'] + "." + \
                    full_load_prop['STG_TABLE_NAME'] + " a\""
        full_load_prop['full_load'] = full_load
        return full_load_prop

    def build_full_load_properties(self, bucket_key, prop_file):
        """
         method return a dictionary object with different properties set up as the properties file
        :param bucket_key:
        :param prop_file: profile file name
        :return:
        """
        logging.info('build merge properties ' + bucket_key + '/' + prop_file)
        #  get the specified properties files from S3 into tmp path
        tmp_path = LOCAL_PATH + prop_file + '_' + self.batch
        key_path = self.env + '/' + bucket_key + '/' + prop_file
        batch_common.get_file_from_s3(key_path, tmp_path)

        # check if target directory is a s3 directory before translating the properties
        # print target_is_s3

        # Define runtime properties path = /tmp/runtime_<prop file>_batch
        runtime_path = LOCAL_PATH + 'runtime_' + prop_file + '_' + self.batch

        # Translate the properties file and put the file into runtime_path
        batch_common.translate_properties(key_path, runtime_path)

        subprocess.check_call("rm -f " + tmp_path, shell=True)

        # print 'Run time path ' + runtime_path
        # write properties into a dictionary
        logging.info("runtime_path", runtime_path)
        merge_properties = batch_common.get_dictionary(runtime_path)
        logging.info('properties: %s', merge_properties)
        # delete runtime properties file
        subprocess.check_call("rm -f " + runtime_path, shell=True)

        # get inc merge properties
        full_load_properties = self.get_full_load_properties(merge_properties)

        return full_load_properties

    def build_inc_merge_properties(self, bucket_key, prop_file):
        """
         method return a dictionary object with different properties set up as the properties file
        :param bucket_key:
        :param prop_file: profile file name
        :return:
        """
        logging.info('build merge properties ' + bucket_key + '/' + prop_file)
        #  get the specified properties files from S3 into tmp path
        tmp_path = LOCAL_PATH + prop_file + '_' + self.batch
        key_path = self.env + '/' + bucket_key + '/' + prop_file
        batch_common.get_file_from_s3(key_path, tmp_path)

        # check if target directory is a s3 directory before translating the properties
        # print target_is_s3

        # Define runtime properties path = /tmp/runtime_<prop file>_batch
        runtime_path = LOCAL_PATH + 'runtime_' + prop_file + '_' + self.batch

        # Translate the properties file and put the file into runtime_path
        batch_common.translate_properties(key_path, runtime_path)

        subprocess.check_call("rm -f " + tmp_path, shell=True)

        # print 'Run time path ' + runtime_path
        # write properties into a dictionary
        logging.info("runtime_path", runtime_path)
        merge_properties = batch_common.get_dictionary(runtime_path)
        # delete runtime properties file
        subprocess.check_call("rm -f " + runtime_path, shell=True)

        # get inc merge properties
        inc_merge_properties = self.get_inc_merge_properties(merge_properties)

        # Step1:Load data into base table based on data from incremental table
        # Step 1.1 - Check if all 3 Type 2 variables are set, if all are set use Type 2 merge
        # Step 1.1 - Check if all 3 Type 2 variables are not set, if all are set use Type 1 merge

        if inc_merge_properties.get('TYPE2_COLUMN_LIST') is not None and inc_merge_properties.get(
                'TYPE2_KEY_FIELD') is not None \
                and inc_merge_properties.get('TYPE2_DATE_FIELD') is not None:
            self.aws_type2_insert_overwrite(inc_merge_properties)
            print()
            "aws_type2_insert_overwrite"
        else:
            self.aws_insert_overwrite(inc_merge_properties)
            print()
            "aws_insert_overwrite"

        return inc_merge_properties

    def aws_insert_overwrite(self, inc_merge_properties):

        """
         method returns properties along with type1 hive query in dictionary
        :param inc_merge_properties: dictionary of incremental merge properties
        :return:

        """
        prop = ''
        if inc_merge_properties.get('SET_RED_CNT') is not None:
            prop = prop + inc_merge_properties.get('SET_RED_CNT')
        if inc_merge_properties.get('SET_MAX_PART_CNT') is not None:
            prop = prop + inc_merge_properties.get('SET_MAX_PART_CNT')
        if inc_merge_properties.get('SET_MAX_CRT_FILES') is not None:
            prop = prop + inc_merge_properties.get('SET_MAX_CRT_FILES')
        if inc_merge_properties.get('SET_MAPREDUCE_TIMEOUT') is not None:
            prop = prop + inc_merge_properties.get('SET_MAPREDUCE_TIMEOUT')
        if inc_merge_properties.get('SET_MAPREDUCE_MAP_MAXATTEMPTS') is not None:
            prop = prop + inc_merge_properties.get('SET_MAPREDUCE_MAP_MAXATTEMPTS')

        hive_set_properties = prop + " set hive.mapred.mode=nonstrict;set fs.s3n.multipart.uploads.enabled=true;" \
                                     "set hive.exec.dynamic.partition.mode=nonstrict;set avro.output.codec=snappy;" \
                                     "set hive.exec.compress.intermediate=true;set hive.exec.compress.output=true;" \
                                     "set hive.merge.smallfiles.avgsize=256000000;set hive.merge.mapredfiles=true;"

        hive_query = "-e \" " + hive_set_properties + \
                     " WITH BASE_QUERY AS (select t1.* from " + inc_merge_properties['SCHEMA_NAME'] + "." + \
                     inc_merge_properties['BASE_TABLE_NAME'] + " t1 where t1." + inc_merge_properties[
                         'PARTITION_COL_NAME'] + \
                     " IN (SELECT DISTINCT INCR_QUERY." + inc_merge_properties['PARTITION_COL_NAME'] + \
                     " FROM INCR_QUERY)), INCR_QUERY AS (SELECT a.*," + inc_merge_properties['PARTITION_CODE'] + \
                     " FROM " + inc_merge_properties['STG_SCHEMA_NAME'] + "." + \
                     inc_merge_properties['INC_TABLE_NAME'] + \
                     " a) insert overwrite table " + inc_merge_properties['SCHEMA_NAME'] + "." + \
                     inc_merge_properties['BASE_TABLE_NAME'] + " partition(" + inc_merge_properties[
                         'PARTITION_COL_NAME'] + \
                     ") select * from BASE_QUERY t1 WHERE NOT EXISTS (SELECT 1 FROM INCR_QUERY t2 WHERE " + \
                     inc_merge_properties['CONDITION'] + ") union all select * from INCR_QUERY; \""
        inc_merge_properties["QUERY"] = hive_query
        return inc_merge_properties

    def aws_type2_insert_overwrite(self, inc_merge_properties):
        """
         method returns properties along with type2 hive query in dictionary
        :param inc_merge_properties: dictionary of incremental merge properties
        :return:

        """
        prop = ''
        if inc_merge_properties.get('SET_RED_CNT') is not None:
            prop = prop + inc_merge_properties.get('SET_RED_CNT')
        if inc_merge_properties.get('SET_MAX_PART_CNT') is not None:
            prop = prop + inc_merge_properties.get('SET_MAX_PART_CNT')
        if inc_merge_properties.get('SET_MAX_CRT_FILES') is not None:
            prop = prop + inc_merge_properties.get('SET_MAX_CRT_FILES')
        if inc_merge_properties.get('SET_MAPREDUCE_TIMEOUT') is not None:
            prop = prop + inc_merge_properties.get('SET_MAPREDUCE_TIMEOUT')
        if inc_merge_properties.get('SET_MAPREDUCE_MAP_MAXATTEMPTS') is not None:
            prop = prop + inc_merge_properties.get('SET_MAPREDUCE_MAP_MAXATTEMPTS')

        hive_set_properties = prop + "set hive.mapred.mode=nonstrict;set fs.s3n.multipart.uploads.enabled=true;" \
                                     "set hive.exec.dynamic.partition.mode=nonstrict;set avro.output.codec=snappy;" \
                                     "set hive.exec.compress.intermediate=true;set hive.exec.compress.output=true;" \
                                     "set hive.merge.smallfiles.avgsize=256000000;set hive.merge.mapredfiles=true;"

        hive_query = "-e \" " + hive_set_properties + " insert overwrite table " + inc_merge_properties[
            'SCHEMA_NAME'] + "." + \
                     inc_merge_properties['BASE_TABLE_NAME'] + " SELECT " + inc_merge_properties['TYPE2_COLUMN_LIST'] + \
                     ", ROW_EFF_DATE, COALESCE(from_unixtime(unix_timestamp(ROW_EFF_DATE_NEXT) - 1 )," \
                     " '3000-01-01 00:00:00') AS ROW_END_DATE, " \
                     "CASE WHEN ROW_EFF_DATE_NEXT IS NULL THEN 'Y' ELSE 'N' END AS CURRENT_ROW_IND FROM " \
                     "(SELECT " + inc_merge_properties['TYPE2_COLUMN_LIST'] + "," + inc_merge_properties[
                         'TYPE2_DATE_FIELD'] + \
                     " as ROW_EFF_DATE, LEAD(" + inc_merge_properties['TYPE2_DATE_FIELD'] + " ,1) OVER " \
                                                                                            "(PARTITION BY " + \
                     inc_merge_properties['TYPE2_KEY_FIELD'] + " ORDER BY " + \
                     inc_merge_properties['TYPE2_DATE_FIELD'] + ") AS ROW_EFF_DATE_NEXT FROM " \
                                                                "(SELECT " + inc_merge_properties[
                         'TYPE2_COLUMN_LIST'] + " FROM " + inc_merge_properties['SCHEMA_NAME'] + \
                     "." + inc_merge_properties['BASE_TABLE_NAME'] + " BASE UNION ALL SELECT " + \
                     inc_merge_properties['TYPE2_COLUMN_LIST'] + " FROM " + inc_merge_properties[
                         'STG_SCHEMA_NAME'] + "." + \
                     inc_merge_properties['INC_TABLE_NAME'] + ") NEW_BASE_TABLE) AS FINAL_BASE_TABLE_INSERT " \
                                                              "WHERE ROW_EFF_DATE <> COALESCE(ROW_EFF_DATE_NEXT, " \
                                                              "'3000-01-01 00:00:00');\""

        inc_merge_properties["QUERY"] = hive_query
        return inc_merge_properties

    def get_oracle_table_count(self, server, user, password_file, query):
        logging.info("Oracle credentials: %s", query)
        # oracle_test_pass = password_file
        try:

            conn = boto.connect_s3()
            password_bucket = str(self.conf.get('oracle_conn', 'db_path').replace("s3a://", "").split('/')[0])
            password_file = str(self.conf.get('oracle_conn', 'db_path').replace("s3a://", "").split('/')[1])
            bucket = conn.get_bucket(password_bucket)
            hello_key = bucket.get_key(password_file + "/" + self.env + "/.password")
            password = hello_key.get_contents_as_string()
        except:
            logging.info("Exception in reading password file")
            sys.exit(2)

        connection_string = '{0}/{1}@{2}'.format(user.strip(), password.strip(), server.strip())
        with self.oracle.Connection(connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            count = cursor.fetchone()
            cursor.close()
        return count[0]

    def build_sqoop_properties(self, bucket_key, prop_file, load_type, ignore_count):
        """
         method return a dictionary object with different properties set up as the properties file
        :param ignore_count:
        :param load_type:
        :param bucket_key:
        :param prop_file: profile file name
        :return:
        """
        logging.info('build sqoop properties ' + bucket_key + '/' + prop_file + ' load type= ' + load_type)
        #  get the specified properties files from S3 into tmp path
        tmp_path = LOCAL_PATH + prop_file + '_' + self.batch
        key_path = self.env + '/' + bucket_key + '/' + prop_file
        batch_common.get_file_from_s3(key_path, tmp_path)

        # check if target directory is a s3 directory before translating the properties
        tmp_properties = batch_common.get_dictionary(tmp_path)
        target_is_s3 = 'S3_' in tmp_properties['TARGET_DIRECTORY']
        # print target_is_s3

        # Define runtime properties path = /tmp/runtime_<prop file>_batch
        runtime_path = LOCAL_PATH + 'runtime_' + prop_file + '_' + self.batch

        # Translate the properties file and put the file into runtime_path
        batch_common.translate_properties(key_path, runtime_path)

        subprocess.check_call("rm -f " + tmp_path, shell=True)

        # print 'Run time path ' + runtime_path
        # write properties into a dictionary
        properties = batch_common.get_dictionary(runtime_path)
        # print properties
        properties['TARGET_IS_S3'] = target_is_s3
        # delete runtime properties file
        subprocess.check_call("rm -f " + runtime_path, shell=True)

        # get sqoop properties
        sqoop_properties = self.get_sqoop_properties(properties, load_type.upper())
        logging.info("5: %s", sqoop_properties['SQL'])
        logging.info("properties oracle %s", sqoop_properties['DB_CONN'].replace('jdbc:oracle:thin:@', ''),
                     self.db_user, self.db_pwd_path, sqoop_properties['SQL'])

        if not ignore_count:
            count = self.get_oracle_table_count(sqoop_properties['DB_CONN'].replace('jdbc:oracle:thin:@', ''),
                                                self.db_user,
                                                self.db_pwd_path, sqoop_properties['SQL'])
            sqoop_properties['COUNT'] = count
            logging.info(" SOURCE COUNT: %s", count)

        return sqoop_properties

    def get_max_timestamp(self, sqoop_prop):
        get_max_query = 'select TRUNC(MAX(' + sqoop_prop['CHECK_COLUMN_VALUE'] + ')-1) FROM ' + sqoop_prop[
            'SCHEMA'] + '.' + sqoop_prop['TABLE_VIEW_NAME']

        server = sqoop_prop['DB_CONN'].replace('jdbc:oracle:thin:@', '')
        with open(self.db_pwd_path) as db_pwd_file:
            password = db_pwd_file.readline()
        connection_string = '{0}/{1}@{2}'.format(self.db_user.strip(), password.strip(), server.strip())
        with self.oracle.Connection(connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(get_max_query)
            count = cursor.fetchall()
            value = count[0]
            cursor.close()
        return value[0].strftime("%Y-%m-%d %H.%M.%S")

    def update_metastore(self, jobname, value):
        mysql_host = self.conf.get('sqoop_metastore', 'mysql_host')
        mysql_user = self.conf.get('sqoop_metastore', 'mysql_user')
        mysql_password = self.conf.get('sqoop_metastore', 'mysql_password')
        mysql_sqoop_metastore_db = self.conf.get('sqoop_metastore', 'mysql_sqoop_metastore_db')
        connection = self.mysql.connect(mysql_host, mysql_user, mysql_password, mysql_sqoop_metastore_db)
        cursor = connection.cursor()
        updateQuery = "update SQOOP_SESSIONS set propval = " + "\'" + value + "\'" + " where job_name = " + "\'" + \
                      jobname + "\'" + " and propname = 'incremental.last.value';"
        logging.info('updateQuery = ' + updateQuery)
        cursor.execute(updateQuery)
        connection.close()

    def check_job_exists(self, jobName):
        mysql_host = self.conf.get('sqoop_metastore', 'mysql_host')
        mysql_user = self.conf.get('sqoop_metastore', 'mysql_user')
        mysql_password = self.conf.get('sqoop_metastore', 'mysql_password')
        mysql_sqoop_metastore_db = self.conf.get('sqoop_metastore', 'mysql_sqoop_metastore_db')
        connection = self.mysql.connect(mysql_host, mysql_user, mysql_password,
                                        mysql_sqoop_metastore_db)
        cursor = connection.cursor()
        query = "select * from SQOOP_SESSIONS where job_name = " + "\'" + jobName + "\'" + \
                " and propname = 'incremental.last.value'"
        cursor.execute(query)
        rows = cursor.fetchall()

        if len(rows) is 0:
            return False
        else:
            return True

    def get_sqoop_v2_properties(self, sqoop_conf_parm, db_conn_opt, source_env):
        '''
        get the sqoop properties from the properties file including the constructed sqoop command into the dictionary
        :param sqoop_conf_parm
        :param db_conn_opt:
        :param source_env:
        :return: dictionary of sqoop properties
        '''
        global sqoop_cmd
        logging.info('Sqoop Input properties >>>')
        logging.info('\n' +
                     'cluster_name         = ' + str(sqoop_conf_parm['cluster_name']) + '\n' +
                     'env                  = ' + str(source_env) + '\n' +
                     'db_conn_opt          = ' + str(db_conn_opt) + '\n' +
                     'sqoop_type           = ' + str(sqoop_conf_parm['sqoop_type']) + '\n' +
                     'load_type            = ' + str(sqoop_conf_parm['load_type']) + '\n' +
                     'schemaName           = ' + str(sqoop_conf_parm['schemaName']) + '\n' +
                     'tableName            = ' + str(sqoop_conf_parm['tableName']) + '\n' +
                     'targetDir            = ' + str(sqoop_conf_parm['targetDir']) + '\n' +
                     'splitByColumn        = ' + str(sqoop_conf_parm['splitByColumn']) + '\n' +
                     'mapperCount          = ' + str(sqoop_conf_parm['mapperCount']) + '\n' +
                     'dataOutputFileFormat = ' + str(sqoop_conf_parm['dataOutputFileFormat']) + '\n' +
                     'mapColumns           = ' + str(sqoop_conf_parm['mapColumns']) + '\n' +
                     'sqoopJobName         = ' + str(sqoop_conf_parm['sqoopJobName']) + '\n' +
                     'incrementalMode      = ' + str(sqoop_conf_parm['incrementalMode']) + '\n' +
                     'checkColumnName      = ' + str(sqoop_conf_parm['checkColumnName']) + '\n' +
                     'columns              = ' + str(sqoop_conf_parm['columns']) + '\n' +
                     'checkColLastValue    = ' + str(sqoop_conf_parm['checkColLastValue']) + '\n' +
                     'compressCodec        = ' + str(sqoop_conf_parm['compressCodec']) + '\n' +
                     'fieldDelimiter       = ' + str(sqoop_conf_parm['fieldDelimiter']) + '\n' +
                     'isView               = ' + str(sqoop_conf_parm['isView']) + '\n' +
                     'isQuery              = ' + str(sqoop_conf_parm['isQuery']) + '\n' +
                     'queryString          = ' + str(sqoop_conf_parm['queryString']) + '\n' +
                     'streaming            = ' + str(sqoop_conf_parm['streaming']) + '\n' +
                     'streamTolerance      = ' + str(sqoop_conf_parm['streamTolerance']) + '\n'
                                                                                           'additionalArgs       = ' + str(
            sqoop_conf_parm['additionalArgs']))
        sqoop_prop = {}
        FULL_LOAD_TYPE = 'FULL'
        INC_LOAD_TYPE = 'INC'
        TEXT_FORAMT = 'TEXT'
        AVRO_FORMAT = 'AVRO'
        PARQUET_FORMAT = 'PARQUET'
        INC_MODE_APPEND = 'APPEND'
        INC_MODE_LAST_MODIFIED = 'LAST_MODIFIED'
        SQOOP_TYPE_IMPORT = 'import'
        SQOOP_TYPE_EXPORT = 'export'

        if sqoop_conf_parm['cluster_name'] is None:
            raise AirflowException("'cluster_name' is not passed.")

        # DATABASE_NAME for DATABASE CONNECTION
        if source_env is None:
            raise AirflowException("'env_type' is not passed.")
        else:
            sqoop_prop['DB_CONN'] = " --connect " + source_env + " "

        # SQOOP_JOB_TYPE
        if sqoop_conf_parm['sqoop_type'] is None:
            raise AirflowException("sqoop_type is not passed")
        else:
            if (sqoop_conf_parm['sqoop_type'] == SQOOP_TYPE_IMPORT or sqoop_conf_parm[
                'sqoop_type'] == SQOOP_TYPE_EXPORT):
                sqoop_prop['SQOOP_JOB_TYPE'] = sqoop_conf_parm['sqoop_type']
            else:
                raise AirflowException("invalid SQOOP_JOB_TYPE is passed in the Sqoop properties file")

        # LOAD_TYPE
        if sqoop_conf_parm['load_type'] is None:
            raise AirflowException("'load_type' is not passed.")
        else:
            if not (sqoop_conf_parm['load_type'] == FULL_LOAD_TYPE or sqoop_conf_parm['load_type'] == INC_LOAD_TYPE):
                raise AirflowException("Invalid load_type is passed.")
            else:
                sqoop_prop['LOAD_TYPE'] = sqoop_conf_parm['load_type']

        # SCHEMA
        if sqoop_conf_parm['schemaName'] is None:
            raise AirflowException("'schemaName' is not passed.")
        else:
            sqoop_prop['SCHEMA'] = sqoop_conf_parm['schemaName']

        # TABLE_VIEW_NAME
        if sqoop_conf_parm['tableName'] is None:
            raise AirflowException("'tableName' is not passed.")
        else:
            sqoop_prop['TABLE_VIEW_NAME'] = sqoop_conf_parm['tableName']

        # IS_VIEW
        if sqoop_conf_parm['isView'] is None:
            sqoop_prop['VIEW_FLAG'] = "FALSE"
        elif (sqoop_conf_parm['isView'] == 'TRUE' or sqoop_conf_parm['isView'] == 'FALSE'):
            sqoop_prop['VIEW_FLAG'] = sqoop_conf_parm['isView']
        else:
            raise AirflowException("Invalid 'isView' is passed, allows either TRUE or FALSE")

        # IS_QUERY
        if sqoop_conf_parm['isQuery'] is None:
            sqoop_prop['QUERY_FLAG'] = "FALSE"
        elif (sqoop_conf_parm['isQuery'] == 'TRUE' or sqoop_conf_parm['isView'] == 'isQuery'):
            sqoop_prop['QUERY_FLAG'] = sqoop_conf_parm['isQuery']
        else:
            raise AirflowException("Invalid 'isQuery' is passed, allows either TRUE or FALSE")

        if sqoop_conf_parm['load_type'] == FULL_LOAD_TYPE:
            if sqoop_prop['QUERY_FLAG'] == "TRUE":
                if sqoop_conf_parm['queryString'] is None:
                    raise AirflowException("'queryString' is not passed.")
                else:
                    # put $CONDITION back to the QUERY after where
                    sqoop_conf_parm['queryString'].replace(' WHERE ', ' where ')
                    sqoop_prop['SOURCE'] = ' --query "' + sqoop_conf_parm['queryString'] + ' AND $CONDITIONS ' + '"'
            else:
                sqoop_prop['SOURCE'] = " --table " + sqoop_prop['SCHEMA'] + '.' + sqoop_prop['TABLE_VIEW_NAME']
        else:
            sqoop_prop['SOURCE'] = " --table " + sqoop_prop['SCHEMA'] + '.' + sqoop_prop['TABLE_VIEW_NAME']

        # MAPPER_COUNT
        sqoop_prop['MAPPER_COUNT'] = " --num-mappers " + str(sqoop_conf_parm['mapperCount']) + " "

        # LOG_TRACING_LEVEL default to verbose
        sqoop_prop['LOG_TRACING_LEVEL'] = " --verbose "

        # COMPRESS
        sqoop_prop['COMPRESS'] = " --compression-codec " + sqoop_conf_parm['compressCodec'] + " "

        # SPLIT_BY_COLUMN
        if sqoop_conf_parm['splitByColumn'] is None:
            if sqoop_prop['QUERY_FLAG'] == "TRUE":
                raise AirflowException("isQuery is TRUE, but SPLIT_BY_COLUMN is not passed")
            else:
                if sqoop_prop['VIEW_FLAG'] == "TRUE":
                    raise AirflowException("isView is True, but SPLIT_BY_COLUMN is not passed.")
                else:
                    sqoop_prop['SPLIT_BY_COLUMN'] = ""
        else:
            sqoop_prop['SPLIT_BY_COLUMN'] = " --split-by " + sqoop_conf_parm['splitByColumn']

        # TARGET_DIRECTORY
        if sqoop_conf_parm['targetDir'] is None:
            raise AirflowException("'targetDir' is not passed.")
        else:
            if sqoop_conf_parm['targetDir'].startswith("s3://"):
                sqoop_prop['S3_TARGET_DIRECTORY'] = sqoop_conf_parm['targetDir']
                sqoop_prop['S3_TGT_FOLDER_FILE'] = sqoop_conf_parm['targetDir'][:-1] + '_$folder$'
                sqoop_prop['SQOOP_OP_DIR_FLAG'] = "S3"
            else:
                sqoop_prop['S3_TARGET_DIRECTORY'] = ""
                sqoop_prop['S3_TGT_FOLDER_FILE'] = ""
                sqoop_prop['SQOOP_OP_DIR_FLAG'] = "HDFS"
            sqoop_prop['SQP_TGT_DIRECTORY'] = " --target-dir " + sqoop_conf_parm['targetDir']

        # COLUMNS
        if sqoop_conf_parm['columns'] is None:
            sqoop_prop['COLUMNS'] = ""
        else:
            sqoop_prop['COLUMNS'] = " --columns " + sqoop_conf_parm['columns']

        # COL_CONVERSION
        if sqoop_conf_parm['mapColumns'] is None:
            sqoop_prop['COL_CONVERSION'] = ""
        else:
            sqoop_prop['COL_CONVERSION'] = " --map-column-java " + sqoop_conf_parm['mapColumns']

        ################ START: INCREMETNAL SQOOP #####################
        if sqoop_prop['LOAD_TYPE'] == INC_LOAD_TYPE:
            # INCREMENTAL_TYPE
            if (sqoop_conf_parm['incrementalMode'] == INC_MODE_APPEND):
                sqoop_prop['INCREMENTAL_MODE'] = " --incremental append "
            elif (sqoop_conf_parm['incrementalMode'] == INC_MODE_LAST_MODIFIED):
                sqoop_prop['INCREMENTAL_MODE'] = " --incremental lastmodified "
            elif (sqoop_conf_parm['dataOutputFileFormat'] == AVRO_FORMAT and sqoop_conf_parm[
                'incrementalMode'] is None):
                sqoop_prop['INCREMENTAL_MODE'] = " --incremental append "
            else:
                raise AirflowException("incrementalMode should be passed either LAST_MODIFIED or APPEND ")

            # CHECK COLUMNS
            if sqoop_conf_parm['checkColumnName'] is None:
                raise AirflowException("checkColumnName is not passed.")
            else:
                sqoop_prop['CHECK_COLUMN'] = " --check-column " + sqoop_conf_parm['checkColumnName']
                sqoop_prop['CHECK_COLUMN_VALUE'] = sqoop_conf_parm['checkColumnName']

            # LAST_VALUE
            if sqoop_conf_parm['checkColLastValue'] is None:
                raise AirflowException("checkColLastValue is not passed.")
            else:
                sqoop_prop['LAST_VALUE'] = ' --last-value ' + '"' + sqoop_conf_parm['checkColLastValue'] + '"'

            # SQOOP_JOB_NAME
            if sqoop_conf_parm['sqoopJobName'] is None:
                raise AirflowException("sqoopJobName is not passed")
            else:
                sqoop_prop['SQOOP_JOB_NAME'] = sqoop_conf_parm['sqoopJobName']
                sqoop_prop['CREATE_JOB'] = " job --create " + sqoop_conf_parm['sqoopJobName']

            # STREAMING
            if sqoop_conf_parm['streaming'] is None:
                sqoop_prop['STREAMING'] = "FALSE"
            elif (sqoop_conf_parm['streaming'] == 'TRUE' or sqoop_conf_parm['streaming'] == 'FALSE'):
                sqoop_prop['STREAMING'] = sqoop_conf_parm['streaming']
            else:
                raise AirflowException("Invalid 'streaming' is passed.")

            sqoop_prop['SQOOP_JOB_TYPE'] = ' -- ' + sqoop_prop['SQOOP_JOB_TYPE']
        ################ END : INCREMETNAL SQOOP #####################

        if sqoop_conf_parm['dataOutputFileFormat'] is None:
            raise AirflowException("'dataOutputFileFormat' is not passed.")

        if (sqoop_conf_parm['dataOutputFileFormat'] != TEXT_FORAMT and sqoop_conf_parm['fieldDelimiter'] is not None):
            raise AirflowException("fieldDelimiter should be None for ", sqoop_conf_parm['dataOutputFileFormat'])

        if sqoop_conf_parm['dataOutputFileFormat'] == TEXT_FORAMT:
            if sqoop_conf_parm['fieldDelimiter'] is None:
                raise AirflowException("dataOutputFileFormat is TEXT, but fieldDelimiter is not passed.")
            else:
                sqoop_prop['DATA_OUTPUT_FILE_FORMAT'] = ' --as-textfile ' + " --fields-terminated-by " + \
                                                        sqoop_conf_parm['fieldDelimiter'] + " "

        if sqoop_conf_parm['dataOutputFileFormat'] == AVRO_FORMAT:
            if (sqoop_prop['LOAD_TYPE'] == INC_LOAD_TYPE and sqoop_prop['SQOOP_OP_DIR_FLAG'] == "S3"):
                raise AirflowException("can't be import to S3 in AVRO format")
            elif (sqoop_prop['LOAD_TYPE'] == INC_LOAD_TYPE and sqoop_conf_parm[
                'incrementalMode'] == INC_MODE_LAST_MODIFIED):
                raise AirflowException("--incremental lastmodified  can't be used in conjunction with AVRO format.")
            else:
                sqoop_prop['DATA_OUTPUT_FILE_FORMAT'] = ' --as-avrodatafile '

        if sqoop_conf_parm['dataOutputFileFormat'] == PARQUET_FORMAT:
            if (sqoop_prop['LOAD_TYPE'] == INC_LOAD_TYPE):
                raise AirflowException("Sqoop can't support incremental load to PARQUET format")
            else:
                sqoop_prop['DATA_OUTPUT_FILE_FORMAT'] = ' --as-parquetfile '

        sqoop_prop['STREAM_TOLERANCE'] = sqoop_conf_parm['streamTolerance']

        if sqoop_conf_parm['additionalArgs'] is None:
            sqoop_prop['ADDITIONAL_ARGS'] = ""
        else:
            sqoop_prop['ADDITIONAL_ARGS'] = " " + sqoop_conf_parm['additionalArgs']

        ################ START : SQOOP CMD CREATION #####################
        if (sqoop_prop['LOAD_TYPE'] == FULL_LOAD_TYPE):
            logging.info('SQOOP Full Load Command :')
            sqoop_cmd = sqoop_prop['SQOOP_JOB_TYPE'] + \
                        sqoop_prop['DB_CONN'] + \
                        ' ' + db_conn_opt + \
                        sqoop_prop['SOURCE'] + \
                        sqoop_prop['SQP_TGT_DIRECTORY'] + \
                        sqoop_prop['MAPPER_COUNT'] + \
                        sqoop_prop['DATA_OUTPUT_FILE_FORMAT'] + \
                        sqoop_prop['LOG_TRACING_LEVEL'] + \
                        sqoop_prop['COMPRESS'] + \
                        sqoop_prop['SPLIT_BY_COLUMN'] + \
                        sqoop_prop['COLUMNS'] + \
                        sqoop_prop['COL_CONVERSION'] + \
                        sqoop_prop['ADDITIONAL_ARGS']

        if (sqoop_prop['LOAD_TYPE'] == INC_LOAD_TYPE):
            logging.info('SQOOP Incremental Load Command :')
            sqoop_cmd = sqoop_prop['CREATE_JOB'] + \
                        sqoop_prop['SQOOP_JOB_TYPE'] + \
                        sqoop_prop['DB_CONN'] + \
                        ' ' + db_conn_opt + \
                        sqoop_prop['SOURCE'] + \
                        sqoop_prop['SQP_TGT_DIRECTORY'] + \
                        sqoop_prop['MAPPER_COUNT'] + \
                        sqoop_prop['DATA_OUTPUT_FILE_FORMAT'] + \
                        sqoop_prop['LOG_TRACING_LEVEL'] + \
                        sqoop_prop['COMPRESS'] + \
                        sqoop_prop['SPLIT_BY_COLUMN'] + \
                        sqoop_prop['COLUMNS'] + \
                        sqoop_prop['COL_CONVERSION'] + \
                        sqoop_prop['INCREMENTAL_MODE'] + \
                        sqoop_prop['CHECK_COLUMN'] + \
                        sqoop_prop['LAST_VALUE'] + \
                        sqoop_prop['ADDITIONAL_ARGS']

        sqoop_prop['SQOOP_CMD'] = sqoop_cmd
        ################ END : SQOOP CMD CREATION #####################

        # Table Count
        if sqoop_prop['QUERY_FLAG'] == "TRUE":
            query_str = (sqoop_conf_parm['queryString']).strip().upper().replace("\"", "").replace("\\", "") \
                .replace("WHERE AND", "WHERE")
            sql = "SELECT  /*+ parallel(4) */ count(*) as COUNT FROM (" + query_str + ")"
        else:
            if (sqoop_prop['LOAD_TYPE'] == FULL_LOAD_TYPE):
                sql = "SELECT /*+ parallel(4) */ count(*) as COUNT FROM " + sqoop_prop['SCHEMA'] + "." \
                      + sqoop_prop['TABLE_VIEW_NAME']
            else:
                sql = "SELECT /*+ parallel(4) */ count(*) as COUNT FROM " + sqoop_prop['SCHEMA'] + "." \
                      + sqoop_prop['TABLE_VIEW_NAME'] + " WHERE " + sqoop_conf_parm[
                          'checkColumnName'] + " > trunc(TO_DATE(" + "'" + sqoop_conf_parm['checkColLastValue'][
                                                                           :-2] + "'" + ",'YYYY-MM-DD HH24.MI.SS'" + "))"

        sqoop_prop['SQL'] = sql

        return sqoop_prop

