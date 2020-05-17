import logging
import boto3
import botocore
import json
import subprocess
import time
import sys
from datetime import datetime
from random import randint
import collections
# airflow
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils import apply_defaults
from airflow.utils.decorators import apply_defaults

class SqsOperator(BaseOperator):

    template_fields = tuple()
    ui_color = '#7A35E8'

    @apply_defaults
    def __init__(self,
                 queue_name,
                 aws_region,
                 task_id,
                 *args, **kwargs):
        super(SqsOperator, self).__init__(task_id=task_id,*args, **kwargs)
        self.queue_name = queue_name
        self.aws_region = aws_region


    def execute(self,context):
        pass



    def getSqsEndpoint(self):
        sqsEp = boto3.resource('sqs', region_name=self.aws_region)
        if sqsEp is None:
            raise botocore.exceptions.ClientError.response['Error']["Failed to get SQS Endpoint"]
        return sqsEp

    def getsqsqueue(self):
        sqs = self.getSqsEndpoint()
        if sqs is not None:
            try:
                self.queue = sqs.get_queue_by_name(QueueName=self.queue_name)
            except botocore.exceptions.ClientError as exp:
                if exp.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
                    logging.info("SQS queue {0} does not exist in region: {1}".format(self.queue_name, self.aws_region))
                raise exp
        return self.queue



    def getmessages(self, sqs_wait_time_seconds=5, sqs_visibility_timeout=30):
        ''' Returns a list of all available SQS.Message objects, uses long polling'''
        messages = []
        while True:
            response = self.queue.receive_messages(MaxNumberOfMessages=1,
                                                   VisibilityTimeout=sqs_visibility_timeout,
                                                   WaitTimeSeconds=sqs_wait_time_seconds)
            if not response:
                break
            messages += response
        return messages


    def getmessage(self, sqs_wait_time_seconds=5, sqs_visibility_timeout=30):
        messages = self.queue.receive_messages(MaxNumberOfMessages=1,
                                               VisibilityTimeout=sqs_visibility_timeout,
                                               WaitTimeSeconds=sqs_wait_time_seconds)
        # if len(messages) == 0:
        #    return None
        return messages




class SqsS3EventSensor(SqsOperator):
    """
    Connect to AWS SQS webservice, poke for message availability. If messages are available, then process them otherwise
    keep poking for new messages.
    """
    template_fields = tuple()
    ui_color = '#7A35E8'


    @apply_defaults
    def __init__(self,
                 queue_name,
                 action,
                 aws_region,
                 task_id,
                 *args, **kwargs):
        super(SqsS3EventSensor, self).__init__(queue_name=queue_name,aws_region=aws_region,task_id=task_id,*args, **kwargs)
        self.queue_name = queue_name
        self.action = action.lower()
        self.aws_region = aws_region

    def execute(self,context):
        if self.action == 'poke':
            self.poke_msg()
        else:
            logging.info('Invalid action filed')


    def poke_msg(self):

        self.getsqsqueue()

        # poke for new message
        logging.info('Poking for message in: {0}'.format(self.queue_name))

        messages = self.getmessages()

        if len(messages) == 0:
            logging.info('Continue to poke...')

            ispoke = True
            while ispoke:
                mz = self.getmessages()

                if len(mz) == 0:
                    ispoke = True
                    logging.info('Poking for message in: {0}'.format(self.queue_name))
                else:
                    ispoke = False

        else:
            logging.info('Message available')
            # iterate over each message



class SqsS3EventOperator(SqsOperator):
    """
    Connect to AWS SQS webservice, poke for message availability. If messages are available, then process them otherwise
    keep poking for new messages.
    """
    template_fields = tuple()
    ui_color = '#7A35E8'

    @apply_defaults
    def __init__(self,
                 queue_name,
                 action,
                 destination,
                 is_encryption_enabled,
                 aws_region,
                 task_id='test',
                 *args, **kwargs):
        super(SqsS3EventOperator, self).__init__(queue_name=queue_name,aws_region=aws_region,task_id=task_id,*args, **kwargs)
        self.queue_name = queue_name
        self.action = action.lower()
        self.destination = destination
        self.is_encryption_enabled = is_encryption_enabled
        self.aws_region = aws_region


    def execute(self,context):
        if self.action == 'getmessage':
            self.copy_files()
        else:
            logging.info('Invalid action filed')




    def copy_files(self):
        self.getsqsqueue()
        messages = self.getmessages()

        for message in messages:
            print message
            message_body_json = json.loads(message.body)

            bucket = message_body_json['Records'][0]['s3']['bucket']['name']
            file_path = message_body_json['Records'][0]['s3']['object']['key']
            src_absolute_path = 's3://'+ bucket + '/' + file_path
            file_name = file_path.split('/')[-1]
            check_sum = message_body_json['Records'][0]['s3']['object']['size']


            logging.info('Processing file : {0}'.format(file_name))
            if self.is_encryption_enabled == 'True':
                sse=" --sse"
            else:
                sse=""

            if str(check_sum).strip(' \t\n\r') != '0':

                source_to_tmp = "aws s3 cp {0} /tmp/ {1}".format(src_absolute_path,sse)
                tmp_success = subprocess.call(source_to_tmp, shell=True)
                tmp_to_dest = "aws s3 mv /tmp/{0} {1}/ {2} ".format(file_name,self.destination,sse)
                dest_success = subprocess.call(tmp_to_dest, shell=True)
                file_count = "aws s3 ls {0}/{1} |wc -l ".format(self.destination,file_name)
                find_file = subprocess.check_output(file_count, shell=True)
                file_size = subprocess.check_output("aws s3 ls {0}/{1} |awk '{2}' ".format(self.destination, file_name,"{print $3}"), shell=True)

                logging.info(
                    " tmp_success :{0} , dest_success : {1} , find_file: {2} ,check_sum:{3} ,file_size :{4}".format(
                        tmp_success, dest_success, find_file, check_sum, file_size.strip(' \t\n\r')))
                if tmp_success == 0 and dest_success == 0 and find_file.rstrip('\n') == '1' and str(check_sum).strip(' \t\n\r') == file_size.strip(' \t\n\r'):
                    logging.info("Deleting file : {0}".format(file_name))
                    message.delete()
                    logging.info("Deleted {0} from queue".format(file_name))

                else:
                    logging.info("Copy failed for message : {0}".format(message_body_json))
            else:
                logging.info("Input file {0} is empty".format(file_name))




    def getSqsEndpoint(self):
        sqsEp = boto3.resource('sqs', region_name=self.aws_region)
        if sqsEp is None:
            raise botocore.exceptions.ClientError.response['Error']["Failed to get SQS Endpoint"]
        return sqsEp



class SqsSnsEventOperator(SqsOperator):
    """
    Connect to AWS SQS webservice, poke for message availability. If messages are available, then process them .
    """
    template_fields = tuple()
    ui_color = '#7A35E8'

    @apply_defaults
    def __init__(self,
                 queue_name,
                 action,
                 destination,
                 is_encryption_enabled,
                 aws_region,
                 job_name,
                 wait_time,
                 task_id='test',
                 *args, **kwargs):
        super(SqsSnsEventOperator, self).__init__(queue_name=queue_name,aws_region=aws_region,task_id=task_id,*args, **kwargs)
        self.queue_name = queue_name
        self.job_name = job_name
        self.wait_time = wait_time
        self.action = action.lower()
        self.destination = destination
        self.is_encryption_enabled = is_encryption_enabled
        self.aws_region = aws_region


    def execute(self,context):
        """
        This method is the entry point for execution
        :param context:
        :return:
        """
        if self.action == 'sns':
            self.poke_messages()
            logging.info('Success Messages for ' + self.job_name + ' available , Poking completed')
            time.sleep(60)
            self.process_sns()
        else:
            logging.info('Invalid action filed')

    def poke_messages(self):
        """
        This method is to poke for message availability for the specified job_name
        :return:
        """
        success_flag = False
        total_wait_time = 0
        while not success_flag and total_wait_time < self.wait_time:
            self.getsqsqueue()
            messages = self.getmessages()
            job_messages = []
            for message in messages:
                print message
                message_body_json = json.loads(message.body)
                job_name = message_body_json.get('job_desc')
                job_status = message_body_json.get('status_text')

                if self.job_name == job_name and job_status == 'SUCCESS':
                    job_messages.append(message)
            if len(job_messages) > 0:
                success_flag = True
            else:
                wait_time = 60
                total_wait_time += wait_time
                time.sleep(wait_time)
        if total_wait_time > self.wait_time:
            logging.info('The process has exceeded the poking time')
            # raise Exception
            sys.exit(1)


    def process_sns(self):
        """
        This method is to read the queue and then process the messages for the job success status.This method runs till
        the wait time is reached.
        :return:
        """
        success_flag = False
        self.getsqsqueue()
        messages = self.getmessages()
        job_messages_dict = {}
        del_success_messages = []
        del_other_status_messages = []
        job_end_dates = set()
        for message in messages:
            print message
            message_body_json = json.loads(message.body)
            job_name = message_body_json.get('job_desc')
            job_status = message_body_json.get('status_text')
            job_end_dt = message_body_json.get('job_endtm')[:10]


            if self.job_name == job_name and job_status == 'SUCCESS':
                #job_messages.append(message)
                job_messages_dict[message] = job_end_dt
                job_end_dates.add(job_end_dt)
                del_success_messages.append(message)

            elif self.job_name == job_name and job_status != 'SUCCESS':
                del_other_status_messages.append(message)

        ordered_job_messages_dict = collections.OrderedDict(sorted(job_messages_dict.items(),reverse=True))
        job_messages = list(ordered_job_messages_dict.keys())

        for msg in job_messages:
            msg_json= json.loads(msg.body)
            job_name = msg_json.get('job_desc')
            job_status = msg_json.get('status_text')
            job_end_dates_sorted = list(sorted(job_end_dates,reverse=True))
            cur_dt = time.strftime("%m/%d/%Y")
            if job_end_dates_sorted[0] == cur_dt:
                logging.info('Status for '+job_name + ' is ' + job_status + ' for ' + job_end_dates_sorted[0])
                success_flag = True
                break
            elif abs((datetime.strptime(job_end_dates_sorted[0], "%m/%d/%Y") - datetime.strptime(cur_dt, "%m/%d/%Y")).days) == 1:
                logging.info('Status for ' + job_name + ' is ' + job_status + ' for ' + job_end_dates_sorted[0])
                success_flag = True
                break
            elif abs((datetime.strptime(job_end_dates_sorted[0], "%m/%d/%Y") - datetime.strptime(cur_dt, "%m/%d/%Y")).days) == 2:
                logging.info('Status for ' + job_name + ' is ' + job_status + ' for ' + job_end_dates_sorted[0])
                success_flag = True
                break
            else:
                continue

        self.delete_messages(del_success_messages)
        self.delete_messages(del_other_status_messages)
        if success_flag is True:
            logging.info('Job Succeeded')

        if success_flag == False:
            logging.info('Status for ' + self.job_name + ' is not available since the past two days, please check with upstream')
            #raise Exception
            sys.exit(1)

    def delete_messages(self,messages):
        """
        This method is to delete messages in the queue
        :param messages: list having all the message objects
        :return:
        """
        for msg in messages:
            msg.delete()


class SqsPlugin(AirflowPlugin):
    name = "sqs_plugin"
    operators = [SqsS3EventSensor,SqsS3EventOperator,SqsSnsEventOperator]
