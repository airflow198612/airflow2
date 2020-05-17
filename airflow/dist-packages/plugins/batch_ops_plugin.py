#!/usr/bin/python2.7

#######################################################################################################################
# batch_ops_plugin.py
# Description: custom operators for Nike BI batch processing
# Go to the bottom of the code to see what are the operators created in this code
# the code
#######################################################################################################################
import logging
import time
from airflow.models import BaseOperator, XCom
from airflow import settings
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
from batch_common import BICommon
from airflow.exceptions import AirflowException
from sqlalchemy import and_
from datetime import datetime, timedelta
import random 


def pull_profile_from_xcom(context):
    profile = context['ti'].xcom_pull(key='profile', task_ids='start_batch')
    if profile is None:
        raise AirflowException("Cannot find profile, need to use BatchStartOperator to persist profile")
    return profile


def pull_env_from_xcom(context):
    env = context['ti'].xcom_pull(key='env', task_ids='start_batch')
    if env is None:
        raise AirflowException("Cannot find env, need to use BatchStartOperator to persist env")
    return env


class BatchOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 *args, **kwargs):
        super(BatchOperator, self).__init__(*args, **kwargs)
        self.profile = None

    @staticmethod
    def get_prev_run(context):
        logging.info('getting previous run dates for dag')
        execution_date = context['execution_date']
        cls = XCom
        filters = [cls.key == 'exec_dates', cls.task_id == 'success', cls.dag_id == context['dag'].dag_id,
                   cls.execution_date < execution_date]

        session = settings.Session()
        query = (
            session.query(cls.value)
            .filter(and_(*filters))
            .order_by(cls.execution_date.desc(), cls.timestamp.desc())
            .limit(1))
        result = query.first()
        session.close()

        if result:
            return result.value
        else:
            return None


class BatchStartOperator(BatchOperator):
    ui_color = '#D8BFD8'

    def check_if_catch_up(self, context):
        try:
            time_diff = datetime.now() - context['execution_date']
            if time_diff > \
                    (context['dag'].schedule_interval + context['dag'].schedule_interval):
                logging.info(
                    "This is a catch up run, this whole dag will be marked success, only the most recent "
                    "schedule will be executed.")
                self.is_catchup = True
        except Exception:
            self.is_catchup = False

    @apply_defaults
    def __init__(self,
                 task_id='start_batch',
                 catch_up=False,
                 *args, **kwargs):
        super(BatchStartOperator, self).__init__(task_id=task_id, *args, **kwargs)
        self.env_type = None
        self.profile = None
        self.catch_up = catch_up
        self.is_catchup = False

    def execute(self, context):
        logging.info("storing batch information ")
        # check if the profile is already persisted in xcom, this only occurs if the DMStartOperator has run before this
        self.check_if_catch_up(context)
        if self.is_catchup and self.catch_up:
            # don't run the current run - force complete this run, and let the real run to run
            for t in context['dag'].tasks:
                if t != context['task']:
                    t.run(
                        start_date=context['execution_date'],
                        end_date=context['execution_date'],
                        mark_success=True,
                        ignore_dependencies=True)
        else:

            bicommon = BICommon()
            self.profile = bicommon.get_profile()

            epoch = int(time.time())

            # persist env in xcom
            self.env_type = bicommon.env

            execution_date = context['execution_date'].strftime('%Y-%m-%dT%H:%M:%S')
            curr_rundate = context['ds_nodash']

            prev_run = self.get_prev_run(context)

            if prev_run is not None:
                prev_exec_date = prev_run['execution_date']
                prev_rundate = prev_run['curr_rundate']
                prev_epoch = prev_run['curr_epoch']
            else:
                prev_exec_date = None
                prev_rundate = None
                prev_epoch = None

            xcom_keys = ['env','execution_date','curr_rundate','curr_epoch','prev_rundate','prev_epoch','prev_exec_date']

            for key in xcom_keys:

                # Waiting for a random duration
                wait_time = random.randrange(0, 30)
                time.sleep(wait_time)

                # Set Key Values
                if key == 'env':
                    value = bicommon.env
                elif key == 'curr_epoch':
                    value = epoch
                else:
                    value=eval(key)

                logging.info("Storing %s in xcom table: %s" %(key,value))

                # Start the XCOM Push
                retry_counter = 0 
                while True:
                    try:
                        self.xcom_push(context=context, key=key, value=value)
                        break
                    except Exception as e:
                        retry_counter += 1
                        wait_time = retry_counter*60
                        # Random wait in between re-attempts
                        time.sleep(wait_time)
                        if retry_counter > 3:
                            break
                        else:
                            pass
                # If already tried 3 times, raise an error
                if retry_counter == 4:
                    logging.info("Encountered exception while inserting data into xcom table")
                    raise ValueError(e)

class BatchEndOperator(BatchOperator):
    ui_color = '#D8BFD8'

    @apply_defaults
    def __init__(self,
                 task_id='success',
                 *args, **kwargs):
        super(BatchEndOperator, self).__init__(task_id=task_id, *args, **kwargs)
        self.profile = None

    def execute(self, context):
        logging.info("storing end batch information ")

        execution_date = context['execution_date'].strftime('%Y-%m-%dT%H:%M:%S')

        # try getting the xcom from start_batch, if can't get it, get variables at this step instead of
        # throwing exception
        try:
            curr_rundate = context['ti'].xcom_pull(key='curr_rundate', task_ids='start_batch')
            epoch = context['ti'].xcom_pull(key='curr_epoch', task_ids='start_batch')
            prev_rundate = context['ti'].xcom_pull(key='prev_rundate', task_ids='start_batch')
            prev_epoch = context['ti'].xcom_pull(key='prev_epoch', task_ids='start_batch')
            prev_exec_date = context['ti'].xcom_pull(key='prev_exec_date', task_ids='start_batch')
        except Exception:
            logging.info("xcom not found in start_batch")
            epoch = int(time.time())
            curr_rundate = context['ds_nodash']
            item = {
                'execution_date': execution_date,
                'curr_rundate': curr_rundate,
                'curr_epoch': epoch
            }
            self.xcom_push(context=context, key='exec_dates', value=item)

        else:
            item = {
                'execution_date': execution_date,
                'curr_rundate': curr_rundate,
                'curr_epoch': epoch,
                'prev_exec_date': prev_exec_date,
                'prev_rundate': prev_rundate,
                'prev_epoch': prev_epoch,
            }
            self.xcom_push(context=context, key='exec_dates', value=item)


# Defining the plugin class
class AirflowBatchOpPlugin(AirflowPlugin):
    name = "batch_ops_plugin"
    operators = [
        BatchStartOperator,
        BatchEndOperator
    ]
