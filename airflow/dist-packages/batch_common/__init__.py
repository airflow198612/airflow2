from datetime import datetime, timedelta
import copy
from random import randint

DAILY = timedelta(days=1)

DEFAULT_ARGS = {
    'owner': 'etldev',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'start_date': datetime(2015, 11, 18),
    'email': ['catherine.wong@nike.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 10,
    'provide_context': True,
    'retry_delay': timedelta(minutes=2)
}

DAILY_RUN = {
    'owner': 'etldev',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'start_date': datetime(2015, 11, 18),
    'email': ['catherine.wong@nike.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 10,
    'provide_context': True,
    'retry_delay': timedelta(minutes=2),
}


def get_default_arg():
    return copy.copy(DEFAULT_ARGS)


def get_daily_run():
    return copy.copy(DAILY_RUN)


from .dependency_manager import DMError as DMErrorWrapped
from .bi_common import BICommon as BICommonSingleton
from .sqoop_tools import SqoopTools as SqoopToolsSingleton


class DMError(object):
    def __new__(cls, function, jobId, currentInterval, *args, **kwargs):
        from airflow.exceptions import AirflowException
        return DMErrorWrapped(function, jobId, currentInterval, AirflowException, *args, **kwargs)

    @classmethod
    def on_failure(cls, context):
        from airflow.exceptions import AirflowException
        return DMErrorWrapped.on_failure(context, AirflowException)

# Model the previous GLOBAL state as a run-time-loaded Singleton
class BICommon(object):
    instance = None

    def __new__(cls):
        if not BICommon.instance:
            from airflow.configuration import conf
            from airflow.exceptions import AirflowException
            BICommon.instance = BICommonSingleton(conf, AirflowException)
        return BICommon.instance


class SqoopTools(object):
    instance = None

    def __new__(cls, env):
        if not SqoopTools.instance:
            from airflow.configuration import conf
            from airflow.exceptions import AirflowException
            SqoopTools.instance = SqoopToolsSingleton(env, conf, AirflowException)
        return SqoopTools.instance


def get_dictionary(filename):
    return BICommon().get_dictionary(filename)


def get_oracle_table_count(server, user, password_file, query):
    return BICommon().get_oracle_table_count(server, user, password_file, query)


def get_file_from_s3(key_path, dest_path):
    """
    :param key_path: path of the file e.g dev/common/prop/pyprofile
    :param dest_path: path of the destination
    :return:
    """
    return BICommon().get_file_from_s3(key_path, dest_path)


def delete_s3_path(s3_path):
    return BICommon().delete_s3_path(s3_path)

def translate_properties(key_path,runtime_path):
    return BICommon().translate_properties(key_path,runtime_path)

def archive_s3_path(s3_archive_path,s3_source_path):
    return BICommon().archive_s3_path(s3_archive_path,s3_source_path)

def get_wait_time(counter):
    wait_time = (2 ** counter) + randint(0, 9) + counter
    return wait_time
