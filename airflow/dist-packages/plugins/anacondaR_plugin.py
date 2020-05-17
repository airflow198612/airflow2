#!/usr/bin/python2.7

#######################################################################################################################
# anacondaR.py
# Description: AnacondaR spin up and terminate operators
# Go to the bottom of the code to see what are the operators created in this code
# the code
#######################################################################################################################
import logging

from airflow import AirflowException
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
from airflow.configuration import conf
import boto3
import time
from random import randint

from airflow.operators import BaseOperator


class AnacondaROperator(BaseOperator):
    """
    Base AnacondR operator
    """

    ui_color = '#D4EBFF'

    @apply_defaults
    def __init__(
            self,
            instance_type,
            *args, **kwargs):

        super(AnacondaROperator, self).__init__(*args, **kwargs)
        self.instance_type = instance_type.lower()
        self.asg_name = None
        self.max_size = None
        self.min_size = None
        self.desired_capacity = None
        self.xcom_instance_list = []
        self.active_instances = []
        self.dag_instance_list = []
        self.client = None
        self.ec2_client = None
        self.instance_id = None
        self.instance_count = 0
        self.asg_list = []

    def execute(self, context):
        pass

    def get_auto_scaling_group(self):
        cluster = conf.get('core', 'cluster')
        asg_list = []
        # getting all auto scaling groups for this cluster
        if self.asg_name is None:
            asg_list = self.get_auto_scaling_group_list(cluster)
        else:
            asg_list.append(self.asg_name)


        asg_found = False
        instances = None

        # getting the auto scaling group for this instance type
        for a in asg_list:
            response = self.client.describe_auto_scaling_groups(AutoScalingGroupNames=[a])
            for asg in response['AutoScalingGroups']:
                instance_type_found = False
                for tag in asg['Tags']:
                    if tag['Key'] == 'instancetype' and tag['Value'] == self.instance_type.lower():
                        instance_type_found = True
                if instance_type_found:
                    self.asg_name = asg['AutoScalingGroupName']
                    self.min_size = asg['MinSize']
                    instances = asg['Instances']
                    self.max_size = asg['MaxSize']
                    self.desired_capacity = asg['DesiredCapacity']
                    logging.info("found ASG = {asg}".format(asg=self.asg_name))
                    asg_found = True
                    break
        if not asg_found:
            raise AirflowException(
                "Cannot found AnacondaR instance with the type {instance_type}".format(
                    instance_type=self.instance_type))
        else:
            # populate the instance id into the self.active_intances
            for i in instances:
                if i['LifecycleState'] in ['InService','Pending'] and i['ProtectedFromScaleIn'] == True:
                    if i['InstanceId'] not in self.active_instances:
                        self.active_instances.append(i['InstanceId'])

            self.instance_count = len(self.active_instances)

    def get_auto_scaling_group_list(self,cluster):
        asg_list = []
        key_name = "anacondar-{cluster}".format(cluster=cluster)
        if self.asg_name is None:
            logging.info("calling get auto scaling group for {instance_type}".format(instance_type=self.instance_type))

            # getting all AnacondaR auto scaling groups

            response = self.client.get_paginator('describe_tags').paginate(
                Filters=[
                    {'Name': 'key',
                     'Values': [key_name]
                     },
                ]
            )

            for page in response:
                for tags in page[u'Tags']:
                    asg_list.append(tags[u'ResourceId'])

        else:
            asg_list.append(self.asg_name)

        return asg_list

    def check_autoscaling_activity(self,action):
        counter = 0
        success = False
        while not success and counter < 10:
            try:
                response = self.client.describe_scaling_activities(AutoScalingGroupName=self.asg_name)

                if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                    counter += 1
                    time.sleep(self.get_wait_time(counter))
                else:
                    success = True
            except Exception:
                counter += 1
                time.sleep(self.get_wait_time(counter))

        time.sleep(20)
        if not success:
            raise AirflowException("Having issues with getting autoscaling activity")

        if action == 'launch' and 'Launching a new EC2 instance' in response['Activities'][0]['Description']:

            if response['Activities'][0]['StatusCode'] == 'Successful':
                return True
            else:
                if response['Activities'][0]['StatusCode'] == 'Failed':
                    response1 = self.client.update_auto_scaling_group(
                        AutoScalingGroupName=self.asg_name,
                        MinSize=0,
                        MaxSize=0,
                        DesiredCapacity=0)
                    raise AirflowException("Failure in autoscaling activity - Response - {response}".format(response=response['Activities'][0]))
        else:
            return True

    def get_dag_instance_list(self):
        logging.info("checking the active instances running from this dag")
        logging.info("Active instance running in this ASG: {instances}".format(instances=self.active_instances))
        logging.info("Instances that are spun up for this dag: {instances}".format(instances=self.xcom_instance_list))
        if len(self.xcom_instance_list) > 0:
            self.dag_instance_list = []
            for i in self.xcom_instance_list:
                if i in self.active_instances:
                    self.dag_instance_list.append(i)


    @staticmethod
    def get_wait_time(counter):
        wait_time = (2 ** counter) + randint(0, 9) + counter
        return wait_time


class AnacondaRSpinUpOperator(AnacondaROperator):
    """
    Spins up AnacondaR node
    """
    ui_color = '#D4EBFF'

    @apply_defaults
    def __init__(
            self,
            instance_type,
            *args, **kwargs):

        super(AnacondaRSpinUpOperator, self).__init__(task_id='anacondaR_spinup', instance_type=instance_type, *args,
                                                       **kwargs)
        self.instance_type = instance_type.lower()

    def execute(self, context):

        self.xcom_instance_list = context['ti'].xcom_pull(key='instance_list', task_ids='anacondaR_spinup')
        if self.xcom_instance_list is None:
            self.xcom_instance_list = []
        self.client = boto3.client('autoscaling', region_name='us-east-1')
        self.ec2_client = boto3.client('ec2', region_name='us-east-1')
        self.get_auto_scaling_group()
        cluster =  conf.get('core', 'cluster')


        # only scale up when there is no active instance already running for this dag
        if self.instance_count == 0:
            self.scale_up_asg()
            logging.info('tty endpoint for anaconda worker is')
            endpoint = 'tty-tf-' + cluster + '-' + self.instance_type + '.ngap2.nike.com'
            logging.info(endpoint.strip())
            if self.instance_id is not None:
                self.xcom_instance_list.append(self.instance_id)
                self.xcom_push(context=context, key='instance_id', value=self.instance_id)
                self.xcom_push(context=context, key='asg_name', value=self.asg_name)
                self.xcom_push(context=context, key='instance_list', value=self.xcom_instance_list)

            # sleeping 60 seconds to make sure dags have a chance to be synced down to the worker node
            time.sleep(60)
            counter = 0
            while self.instance_count == 0 and counter < 5:
                counter += counter
                self.get_auto_scaling_group()
                time.sleep(self.get_wait_time(counter))

            if self.instance_count == 0:
                self.check_autoscaling_activity('launch')
                raise AirflowException('Unable to spin up instance')
            else:
                logging.info('Waiting for the instance to be up. Approx. 20 min.')
                time.sleep(1200)
                logging.info('Waiting for the instance to be up.')

        else:
            logging.info("An instance has been running already for this autoscaling group/queue")

    def scale_up_asg(self):
        self.min_size = 1
        self.max_size = 1
        self.desired_capacity = 1

        success = False
        counter = 0
        while not success and counter < 5:
            try:
                response = self.client.update_auto_scaling_group(
                    AutoScalingGroupName=self.asg_name,
                    MinSize=self.min_size,
                    MaxSize=self.max_size,
                    DesiredCapacity=self.desired_capacity,
                    NewInstancesProtectedFromScaleIn=True)

                if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                    logging.info(response)
                    counter += 1
                    time.sleep(self.get_wait_time(counter))
                else:
                    success = True
            except Exception:
                counter += 1
                time.sleep(self.get_wait_time(counter))
                logging.error(Exception)

        if not success:
            raise AirflowException("cannot spin up AnacondaR instance")

        # wait enough for the instance to be in service
        time.sleep(120)

        if self.check_autoscaling_activity('launch'):

            counter = 0
            while self.instance_id is None and counter < 15:
                try:
                    logging.info("checking if instance is being spun up(will try to check 15 times) counter {counter}".format(counter=str(counter)))
                    response = self.client.describe_auto_scaling_groups(AutoScalingGroupNames=[self.asg_name])
                    if len(response['AutoScalingGroups'][0]['Instances']) == self.desired_capacity:
                        for i in response['AutoScalingGroups'][0]['Instances']:
                            if i not in self.active_instances and i['LifecycleState'] in ['InService']:
                                self.instance_id = i['InstanceId']
                    counter += 1
                    if counter < 15:
                        time.sleep(30)
                except Exception:
                    counter += 1
                    if counter < 15:
                        time.sleep(30)

    def check_ec2_instance_status(self):
        instance_up = False
        counter = 1
        logging.info("Waiting for the EC2 instance {instance} to be ready".format(instance=self.instance_id))
        # waiting for 2 minutes before polling the status
        time.sleep(120)
        while not instance_up and counter < 15:
            try:
                response = self.ec2_client.describe_instance_status(InstanceIds=[self.instance_id])
                if response['InstanceStatuses'][0]['InstanceState']['Name'] == 'running' and \
                                response['InstanceStatuses'][0]['InstanceStatus']['Details'][0]['Status'] == 'passed':
                    instance_up = True
                else:
                    time.sleep(self.get_wait_time(counter))
                    counter += 1
            except Exception:
                time.sleep(self.get_wait_time(counter))
                counter += 1
        return instance_up


class AnacondaRTerminateOperator(AnacondaROperator):
    """
    Terminate AnacondaR node
    """
    ui_color = '#D4EBFF'

    @apply_defaults
    def __init__(
            self,
            instance_type,
            *args, **kwargs):
        super(AnacondaRTerminateOperator, self).__init__(task_id='anacondaR_terminate', instance_type=instance_type,
                                                          *args, **kwargs)
        self.instance_type = instance_type.lower()

    def execute(self, context):
        self.s3_client = boto3.client('s3')
        self.client = boto3.client('autoscaling', region_name='us-east-1')
        self.ec2_client = boto3.client('ec2', region_name='us-east-1')
        self.instance_id = context['ti'].xcom_pull(key='instance_id', task_ids='anacondaR_spinup')
        self.xcom_instance_list = context['ti'].xcom_pull(key='instance_list', task_ids='anacondaR_spinup')
        if self.xcom_instance_list is None:
            self.xcom_instance_list = []
        self.get_auto_scaling_group()
        self.get_dag_instance_list()

        # only scale down if there are instances from this dags that are active
        if len(self.active_instances) > 0:
            self.scale_down_asg()
            #self.del_route53()
            logging.info("Instance(s) terminated")
        else:
            logging.info("No active instance from this dag. Exiting...")

    def del_route53(self):
        cluster = conf.get('core', 'cluster')
        tty_bucket = conf.get('core', 'tty_bucket')
        tty_prefix = conf.get('core', 'tty_prefix')
        logging.info("deleting the route53 entries. passing values to s3 bucket to trigger lambda to delete the records")
        logging.info(tty_bucket.strip())
        logging.info(tty_prefix)
        logging.info(cluster)
        logging.info(self.instance_type)
        prefix = tty_prefix + '/' + cluster + '/' + self.instance_type
        endpoint = 'tty-tf-' + cluster + '-' + self.instance_type
        logging.info(prefix.strip())
        logging.info(endpoint.strip())
        response = self.s3_client.list_objects(Bucket=tty_bucket.strip(),Prefix=prefix.strip())
        logging.info(response)
        keys = []
        if 'Contents' in response:
            keys = [{'Key': x['Key']} for x in response['Contents']]
            logging.info(keys)
            logging.info('Deleting the Keys from the S3 bucket')
            self.s3_client.delete_objects(Bucket=tty_bucket.strip(), Delete={"Objects": keys,'Quiet': True})
            logging.info('Deleted Keys Successfully')


    def scale_down_asg(self):
        logging.info(
            "setting instance protection to False for instance {instance}".format(instance=self.active_instances))
        # wait 60 seconds to make sure the log is going to Kibana
        logging.info(
            "Waiting for 60 seconds to make sure log is going to Kibana before terminating instance")
        time.sleep(60)
        response = self.client.set_instance_protection(
            InstanceIds=self.active_instances,
            AutoScalingGroupName=self.asg_name,
            ProtectedFromScaleIn=False
        )
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            logging.info(response)
            raise AirflowException("cannot set instance protection to False")


        logging.info("scaling down the auto scaling group to 0")
        min_size = 0
        max_size = 0
        desired_capacity = 0
        response = self.client.update_auto_scaling_group(
            AutoScalingGroupName=self.asg_name,
            MinSize=min_size,
            MaxSize=max_size,
            DesiredCapacity=desired_capacity)
        logging.info(response)
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            logging.info(response)
            raise AirflowException("cannot scale down AnacondaR instance")

        self.terminate_instances()
        # wait enough for the instance to terminate
        time.sleep(60)

    def terminate_instances(self):
        instance_down = False
        counter = 1
        logging.info("Waiting for the EC2 instance {instance} to be terminated".format(instance=self.active_instances))
        # waiting for 2 minutes before polling the status
        while not instance_down and counter < 1:
            try:
                response = self.ec2_client.terminate_instances(InstanceIds=self.active_instances)
                if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    instance_down = True
                else:
                    time.sleep(self.get_wait_time(counter))
                    logging.info(response)
                    counter += 1
            except Exception:
                time.sleep(self.get_wait_time(counter))
                counter += 1


class AnacondaRPlugin(AirflowPlugin):
    name = "AnacondaRPlugin"
    operators = [
        AnacondaRSpinUpOperator,
        AnacondaRTerminateOperator
    ]
