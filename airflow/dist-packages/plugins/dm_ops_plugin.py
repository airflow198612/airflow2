#!/usr/bin/python2.7

#######################################################################################################################
# dm_ops_plugin.py
# Description: Dependency Manager start and done operator
# Go to the bottom of the code to see what are the operators created in this code
# the code
#######################################################################################################################

import logging
import time
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
import batch_common
from batch_common import BICommon
import boto3
from datetime import datetime, timedelta
from time import sleep
import json
from airflow.exceptions import AirflowException, AirflowSensorTimeout


#######################################################################################################################
# dm_ops_plugin.py
# Description: custom operators for Nike BI batch processing
# Go to the bottom of the code to see what are the operators created in this code
# the code
#######################################################################################################################


class DMException(Exception):
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        super(DMException, self).__init__(message)


class DMOperator(BaseOperator):
    """
     invoke AWS Lambda
    :param function: The function name to invoke.
    :type function: string
    :param payload: payload that pass to the function
    :type payload: string
    """

    template_fields = ('function', 'jobId')

    ui_color = '#B6E7ED'

    @apply_defaults
    def __init__(self,
                 function=None,
                 job_id=None,
                 *args, **kwargs):
        super(DMOperator, self).__init__(*args, **kwargs)
        self.function = function
        self.payload = None
        self.job_id = job_id
        self.function = function
        self.current_interval = None
        self.client = None
        self.profile = None

    def execute(self, context):
        pass

    def create_payload(self, operation, status):
        logging.info("job id = " + self.job_id)
        logging.info("in create payload operation= " + operation)
        # payload="{'operation': 'getJobs', 'job_id': {0} }".format( self.job_id)
        if status is not None:
            payload = '{"operation": "%s", "job_id": "%s", "interval_id": "%s" ,"run_status": "%s"}' % (
                operation, self.job_id, self.current_interval, status)
        else:
            payload = '{"operation": "%s", "job_id": "%s", "interval_id": "%s"}' % (
                operation, self.job_id, self.current_interval)
        print payload
        self.payload = payload

    def call_lambda(self, operation, status):
        # invoke lambda function

        self.client = boto3.client('lambda', region_name='us-east-1')
        self.create_payload(operation, status)
        success = False
        counter = 0
        while not success and counter < 5:
            try:
                response = self.client.invoke(
                    FunctionName=self.function,
                    Payload=self.payload)
                success = True
                logging.info(response)
                break
            except Exception as excp:
                logging.info("exception when invoking lambda: " + str(excp) + " trying again...")
            counter += 1
            wait_time = batch_common.get_wait_time(counter)
            time.sleep(wait_time)

        if not success:
            logging.info("Tried 5 times already to call lambda")
            raise AirflowException("Tried 5 times already to call lambda")

        # check HTTP status code returned from Lambda call, 200 is good
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logging.info("Lambda return 200 - successful")
        else:
            logging.info("Lambda call returns bad result {response}".format(response=response))
            raise DMException("Error calling Lambda function")
        return response

    def call_DM(self, operation, status=None):

        logging.info("operation = " + operation)

        response = self.call_lambda(operation, status)
        logging.info(response['ResponseMetadata']['HTTPStatusCode'] )
        # check HTTP status code returned from Lambda call, 200 is good
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logging.info("Lambda return 200 - successful")
        else:
            logging.info("Lambda call returns bad result: {response}".format(response=response))
            raise DMException("Error calling Lambda function")

        # getting the payload from response to get the
        stream = response['Payload']
        r = stream.read()
        j = json.loads(r)
        if isinstance(j, list):
            logging.info("Response return %s", j[0])
            return j[0]
        else:
            logging.info("Response return %s", j)
            return j

    def get_function(self):
        if self.function is None:
            bicommon = BICommon()
            self.profile = bicommon.get_profile()
            if self.profile is not None:
                logging.info('getting DM function from profile')
                self.function = self.profile['DM_ENDPOINT']

    def poke(self, context):
        pass


class DMStartOperator(DMOperator):
    """
     invoke Dependency Manager, will set the job to running
    :param function: The function name to invoke.
    :type function: string
    :param operation: The operation to use in the payload
    :type operation: string
    :param job_id: The Dependency Manager job id
    :type job_id: string
    :param selfCatchUp: indicate whether the job is self catch up, if not all schedules will run
    :type selfCatchUp: boolean
    """
    template_fields = ('function', 'job_id')

    ui_color = '#B6E7ED'

    @apply_defaults
    def __init__(self,
                 jobId=None,
                 job_id=None,
                 task_id='start_batch',
                 function=None,
                 self_catch_up=True,
                 poke_interval=60,
                 timeout=60 * 60 * 24 * 1,
                 *args, **kwargs):
        if jobId is not None:
            job_id = jobId
        super(DMStartOperator, self).__init__(job_id=job_id,task_id=task_id, function=function, *args, **kwargs)
        self.job_id = job_id
        self.self_catch_up = self_catch_up
        self.current_interval = None
        self.had_dependencies = False
        self.is_catchup = False
        self.run_frequency = None
        self.last_run = None
        self.next_run = None
        self.last_run_status = None
        self.poke_interval = poke_interval
        self.timeout = timeout
        self.profile = None

    def get_jobs(self):

        # invoke DM function to get latest job instance
        response = self.call_DM("getJobs")

        try:
            self.run_frequency = response['run_frequency']
        except KeyError:
            self.run_frequency = None

        try:
            self.had_dependencies = response['state']['has_dependencies']
        except KeyError:
            self.had_dependencies = False

        try:
            dependencies = response['state']['depends_on']
        except KeyError:
            dependencies = None

        try:
            self.last_run_status = response['state']['last_run_status']
        except KeyError:
            self.last_run_status = None

        try:
            last_run = response['state']['last_run']
        except KeyError:
            last_run = None

        try:
            next_run = response['state']['next_run']
        except KeyError:
            next_run = None

        logging.info('Run Frequency: %s', self.run_frequency)
        logging.info('Has Dependencies: %s', self.had_dependencies)
        logging.info('Dependencies: %s', dependencies)
        logging.info('Last Run Status: %s', self.last_run_status)

        logging.info('Last Run: %s', last_run)
        logging.info('Next Run: %s', next_run)

        # substring the date from YYYY-MM-DDT00:00:)) to YYYY-MM-DD
        if last_run is not None:
            self.last_run = last_run[:10]

        if next_run is not None:
            self.next_run = next_run[:10]

    def poke(self, context):
        # check Dependency Manager to see if the job can start
        # DM will return 200, success and is_ready when the predeccessor job has
        # completed
        try:
            response = self.call_DM("startJobInstance")
        except DMException:
            return False
        try:
            is_ready = response['is_ready']
        except KeyError:
            is_ready = False

        try:
            success = response['success']
        except KeyError:
            success = False

        if is_ready and success:
            return True
        else:
            return False

    def execute(self, context):

        # setting batch_info in table and persist profile and epoch in xcom
        super(DMStartOperator, self).execute(context)
        # default to the profile DM endpoint if function is not passed

        self.get_function()

        logging.info("DM function: %s", self.function)

        # invoke DM function to get latest job instance
        self.get_jobs()
        logging.info('Context on Airflow is %s', context['ds'])
        logging.info("run Frequency %s", self.run_frequency)
        logging.info("schedule interval on Airflow is %s", context['dag'].schedule_interval)

        # check if this is a possible catchup (delay run) situation, the design is to force this instance to complete
        # let the most recent instance to run
        # if the current time and the execution time is more than 2 intervals behind, that is a catch up run

        try:
            if self.get_time_diff(context) > \
                    (context['dag'].schedule_interval + context['dag'].schedule_interval):
                logging.info(
                    "This is a catch up run, this whole dag will be marked success, only the most recent "
                    "schedule will be executed.")
                self.is_catchup = True
            else:
                logging.info("This is a regular run")
                self.is_catchup = False
        except Exception:
            self.is_catchup = False


        if self.is_catchup and self.self_catch_up:
            # don't run the current run - force complete this run, and let the real run to run
            for t in context['dag'].tasks:
                if t != context['task']:
                    t.run(
                        start_date=context['execution_date'],
                        end_date=context['execution_date'],
                        mark_success=True,
                        ignore_dependencies=True)
        else:

            # get current interval id to be pass to lambda DM call of startJobInstances
            # since airflow schedule kicks off at last minute of the schedule day
            # add one day to the airflow execution date

            run_date = context['execution_date'] + timedelta(days=1)
            self.current_interval = run_date.strftime('%Y-%m-%dT%H:%M')
            logging.info('Current Interval - real process date is %s', self.current_interval)

            if self.last_run_status == 'NONE':
                logging.info("This is the Initial run of job")

            started_at = datetime.now()

            # check if job is ready to run by calling the poke method
            while not self.poke(context):
                sleep(self.poke_interval)
                if (datetime.now() - started_at).seconds > self.timeout:
                    raise AirflowSensorTimeout('Snap. Time is OUT.')

            logging.info("Success criteria met. set job to RUNNING status in DM")
            try:
                response = self.call_DM("updateJobInstance", "RUNNING")
            except DMException:
                raise AirflowException("error update the job to running in DM, callDM exception")

            # check if the success keyword is in the response
            try:
                success = response['success']
            except KeyError:
                logging.info("updating the job to running DM not successful")
                raise AirflowException("update the job to running in DM not successful")

    def get_time_diff(self, context):
        return datetime.now() - context['execution_date']


class DMDoneOperator(DMOperator):
    """
     invoke Dependency Manager, will set the job to DONE
    :param function: The function name to invoke.
    :type function: string
    :param job_id: The Dependency Manager job id
    :type job_id: string
    """
    template_fields = ('function', 'job_id')

    ui_color = '#B6E7ED'

    @apply_defaults
    def __init__(self,
                 jobId=None,
                 job_id=None,
                 task_id='success',
                 function=None,
                 *args, **kwargs):
        if jobId is not None:
            job_id = jobId
        super(DMDoneOperator, self).__init__(job_id=job_id, task_id=task_id, function=function, *args, **kwargs)
        self.operation = 'updateJobInstance'
        self.status = None
        self.job_id = job_id
        self.current_interval = None
        self.status = "DONE"

    def execute(self, context):

        # get profile
        logging.info("Dependency Manager ending operator")

        self.get_function()

        # get current interval id to be pass to lambda DM call of updateJobInstance to DONE
        run_date = context['execution_date'] + timedelta(days=1)
        self.current_interval = run_date.strftime('%Y-%m-%dT%H:%M')
        logging.info('Current Interval is %s', self.current_interval)

        logging.info('calling DM to updateJobInstance %s to %s', (self.job_id, self.status))
        try:
            response = self.call_DM(self.operation, self.status)
        except DMException:
            raise AirflowException("error update the job to %s in DM", self.operation)

        try:
            success = response['success']
        except KeyError:
            success = False

        if not success:
            raise AirflowException("Update the job to %s in DM not successful", self.status)

            # calling the super class - BatchEndOperator execute method
            # super(DMDoneOperator, self).execute(context)


class DMErrorOperator(DMOperator):
    """
     invoke Dependency Manager, will set the job to ERROR
    :param function: The function name to invoke.
    :type function: string
    :param job_id: The Dependency Manager job id
    :type job_id: string
    """
    template_fields = ('function', 'job_id')

    ui_color = '#B6E7ED'

    @apply_defaults
    def __init__(self,
                 jobId=None,
                 job_id=None,
                 task_id='dmerror',
                 function=None,
                 *args, **kwargs):
        if jobId is not None:
            job_id = jobId
        super(DMErrorOperator, self).__init__(job_id=job_id, task_id=task_id, function=function, *args, **kwargs)

        self.status = "ERROR"
        self.job_id = job_id
        self.operation = 'updateJobInstance'
        self.current_interval = None
        self.profile = None

    def execute(self, context):

        logging.info("Dependency Manager Error operator")
        self.get_function()

        # get current interval id to be pass to lambda DM call of updateJobInstance to DONE
        run_date = context['execution_date'] + timedelta(days=1)
        self.current_interval = run_date.strftime('%Y-%m-%dT%H:%M')
        logging.info('Current Interval is %s', self.current_interval)

        logging.info('calling DM to updateJobInstance %s to %s', (self.job_id, self.status))
        try:
            response = self.call_DM(self.operation, self.status)
        except DMException:
            raise AirflowException("error update the job to %s in DM", self.operation)

        try:
            success = response['success']
        except KeyError:
            success = False

        if not success:
            raise AirflowException("Update the job to %s in DM not successful", self.status)


# Defining the plugin class
class AirflowDMOpPlugin(AirflowPlugin):
    name = "dm_ops_plugin"
    operators = [
        DMStartOperator,
        DMDoneOperator,
        DMErrorOperator
    ]
