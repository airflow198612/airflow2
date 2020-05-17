#!/usr/bin/python2.7

#######################################################################################################################
# tasdependencysensor_plugin.py
# Description: task dependency sensor - enhanced external task sensor
# Go to the bottom of the code to see what are the operators created in this code
# the code
#######################################################################################################################
import logging
from airflow.models import TaskInstance
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

from datetime import timedelta
import json
from airflow.utils.state import State
from airflow import settings

from airflow.operators import BaseSensorOperator
from airflow.hooks.base_hook import BaseHook

class TaskDependencySensor(BaseSensorOperator):
    """
    Waits for a task to complete in a different DAG in with more options
    :param external_dag_id: The dag_id that contains the task you want to
        wait for
    :type external_dag_id: string
    :param external_task_id: The task_id that contains the task you want to
        wait for
    :type external_task_id: string
    :param allowed_states: list of allowed states, default is ``['success']``
    :type allowed_states: list
    :param execution_delta: time difference with the previous execution to
        look at, the default is the same execution_date as the current task.
        For yesterday, use [positive!] datetime.timedelta(days=1). Either
        execution_delta or execution_date_fn can be passed to
        ExternalTaskSensor, but not both.
    :type execution_delta: datetime.timedelta
    :param execution_delta_json: json object contains the mapping of key value pair
        of the execution hour and the timedelta in hours. Either execution_delta
        or execution_delta_json or execution_data_variable can be passed, but not more than one.
    :type execution_delta_json: json
    :param execution_delta_variable: key of the variable that is set up in admin - variable
        where the value contains the json mapping in the same format of execution_delta_json
    :type execution_date_variable: string
    :param cluster_id: cluster_id of the inter cluster dependency, connection setup has to be set up in admin -
        connections screen
    :type cluster_id: string
    """
    ui_color = '#96b0d3'

    @apply_defaults
    def __init__(
            self,
            external_dag_id,
            external_task_id,
            allowed_states=None,
            execution_delta=None,
            execution_delta_json=None,
            cluster_id=None,
            *args, **kwargs):
        super(TaskDependencySensor, self).__init__(*args, **kwargs)
        self.allowed_states = allowed_states or [State.SUCCESS]
        if execution_delta_json and execution_delta:
            raise ValueError(
                'Only one of `execution_date` or `execution_delta_json` maybe provided to TaskDependencySensor; not more than one.')

        self.execution_delta = execution_delta
        self.execution_delta_json = execution_delta_json
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id
        self.cluster_id = cluster_id

    def poke(self, context):
        # Setting default args for ngap2 airflow db
        db_type = 'mysql'
        internal_db = True
        
        # Reading connection to check db type
        if self.cluster_id is not None:
            conn = BaseHook.get_connection(self.cluster_id)
            logging.info("checking for Extra field in connections to determine db type mysql or postgres or lambda")
            # check for db type from extra args
            for arg_name, arg_val in conn.extra_dejson.items():
                if arg_name in ['db_type']:
                    db_type = arg_val
                if arg_name in ['internal_db']:
                    internal_db = False

        if self.execution_delta:
            dttm = context['execution_date'] - self.execution_delta
        elif self.execution_delta_json:
            hour = context['execution_date'].strftime('%H')
            delta = self.execution_delta_json[hour]
            hour_d = int(delta.split(':')[0])
            minute_d = int(delta.split(':')[1] if len(delta.split(':')) > 1 else '00')

            final_minutes = 0
            if hour_d < 0:
                final_minutes = (hour_d *60) - minute_d
            else:
                final_minutes = (hour_d*60) + minute_d

            dttm = context['execution_date'] - timedelta(minutes=final_minutes)
        else:
            dttm = context['execution_date']

        allowed_states = tuple(self.allowed_states)
        if len(allowed_states) == 1:
            sql = " SELECT ti.task_id FROM task_instance ti WHERE ti.dag_id = '{self.external_dag_id}' " \
                  "AND ti.task_id = '{self.external_task_id}' AND ti.state = ('{allowed_states[0]}') " \
                  "AND ti.execution_date = '{dttm}';".format(**locals())
        else:
            sql = "SELECT  ti.task_id FROM task_instance ti WHERE ti.dag_id = '{self.external_dag_id}' " \
                  "AND ti.task_id = '{self.external_task_id}' AND ti.state in {allowed_states} " \
                  "AND ti.execution_date = '{dttm}';".format(**locals())

        if self.cluster_id is None:
            logging.info(
                'Poking for '
                '{self.external_dag_id}.'
                '{self.external_task_id} on '
                '{dttm} ... '.format(**locals()))

            TI = TaskInstance

            session = settings.Session()
            count = session.query(TI).filter(
                TI.dag_id == self.external_dag_id,
                TI.task_id == self.external_task_id,
                TI.state.in_(self.allowed_states),
                TI.execution_date == dttm,
            ).count()

            session.commit()
            session.close()

        elif internal_db == True:
            logging.info(
                'Poking for '
                '{self.external_dag_id}.'
                '{self.external_task_id} on '
                '{dttm} on {self.cluster_id} ... '.format(**locals()))
            hook = None

            if db_type == 'mysql':
                from airflow.hooks.mysql_hook import MySqlHook
                hook = MySqlHook(mysql_conn_id=self.cluster_id)

            elif db_type == 'postgres' :
                from airflow.hooks.postgres_hook import PostgresHook
                hook = PostgresHook(postgres_conn_id=self.cluster_id)

            else:
                raise Exception("Please specify correct db type")

            records = hook.get_records(sql)

            if not records:
                count = 0
            else:
                if str(records[0][0]) in ('0', '',):
                    count = 0
                else:
                    count = len(records)
                logging.info('task record found')
        else:
            host = conn.host
            user = conn.login
            password = conn.password
            dbname = conn.schema
            port = conn.port
            url = None
            for arg_name, arg_val in conn.extra_dejson.items():
                if arg_name in ['endpoint']:
                    url = arg_val
            if not url:
                raise KeyError('Lambda endpoint is not specified in Extra args')
            
            logging.info(
                'Poking for '
                '{self.external_dag_id}.'
                '{self.external_task_id} on '
                '{dttm} on {self.cluster_id} ... '.format(**locals()))
            
            import requests
            from requests.adapters import HTTPAdapter
            from requests.packages.urllib3.util.retry import Retry
            payload = "{\"ENDPOINT\":\""+host+"\",\"PORT\":\""+str(port)+"\",\"DBUSER\":\""+user+"\",\"DBPASSWORD\":\""+password+"\",\"DATABASE\":\""+dbname+"\", \"DB_TYPE\":\""+db_type+"\",\"QUERY\":\""+sql+"\"}"
            headers = {
                'Content-Type': "application/json",
                'cache-control': "no-cache",
            }
            session= requests.Session()
            retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504, 500])
            session.mount('https://', HTTPAdapter(max_retries=retries))
            response = session.post(url.replace(' ', ''), headers=headers, data=payload)
            if response.status_code == 200:
                count = int(response.text)
            else:
                raise Exception(response.content)
        logging.info(count)
        return count


# Defining the plugin class
class TaskDependencySensorPlugin(AirflowPlugin):
    name = "taskdependencysensor_plugin"
    operators = [
        TaskDependencySensor
    ]

