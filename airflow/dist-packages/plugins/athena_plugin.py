#!/usr/bin/python2.7

#######################################################################################################################
# athena_plugin.py
# Description: Athena operator would only support msck repair table <table_name>, alter table <table_name> add partition(part_name=<name>, alter table <table_name> set location "s3_loc")
# Go to the bottom of the code to see what are the operators created in this code
# the code

#######################################################################################################################

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

import sys
import logging
import botocore
import boto3
import time
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
from random import randint


class AthenaOperator(BaseOperator):

    template_fields = ['command']
    ui_color = '#ededef'

    @apply_defaults
    def __init__(self,
                 command,
                 database_name = 'blank',
                 table_name = 'blank',
                 aws_region = 'us-east-1',
                 s3_location = 's3://nike-emr-working/airflow-athena-jdbc/',
                 glue_check_time = 15,
                 fail_flag = False,
                *args, **kwargs):
        super(AthenaOperator, self).__init__(*args, **kwargs)
        self.command = command
        self.aws_region = aws_region
        self.s3_location = s3_location
        self.glue_check_time = glue_check_time
        self.fail_flag = fail_flag
        self.database_name = database_name
        self.table_name = table_name
        logging.info("Query: " + str(self.command))

    def execte_query(self):
        from botocore.exceptions import ClientError
        #connect to athena using boto3,default aws_region is 'us-east-1'
        athena_client = boto3.client('athena',self.aws_region)
        glue_client = boto3.client('glue', self.aws_region)
        s3_staging_dir = self.s3_location

        if (self.database_name == 'blank' or self.table_name == 'blank'):
            for i in self.command.split(' '):
                if '.' in i:
                    target = i
                    logging.info("schema.tablename is:" + target)
                    self.database_name = target.split('.')[0]
                    self.table_name = target.split('.')[1]

        try:
            logging.info("Running query: {command}".format(command=self.command))

            if (('set location' in self.command) or ('SET LOCATION' in self.command) or ('SET location' in self.command) or ('set LOCATION' in self.command)):
                    glue_response_before = glue_client.get_table(DatabaseName= self.database_name,Name= self.table_name)
                    StartTime = glue_response_before["Table"]["UpdateTime"]

            athena_response = athena_client.start_query_execution(QueryString = self.command,ResultConfiguration = {'OutputLocation': s3_staging_dir})
            # Get query ID
            query_id = str(athena_response['QueryExecutionId'])
            query_state = ['QUEUED']


            # Get the Query state and wait till query execution
            while (query_state not in ['SUCCEEDED', 'FAILED', 'CANCELLED']):
                time.sleep(0.15)
                query_state = athena_client.batch_get_query_execution(QueryExecutionIds=[query_id])['QueryExecutions'][0]['Status']['State']
                if query_state in ['RUNNING', 'QUEUED']:
                    wait_time = randint(60, 70)
                    time.sleep(wait_time)


            logging.info("query state is :" + query_state)
            if query_state in ['FAILED', 'CANCELLED']:
                logging.info("Query not successful please validate ")
                sys.exit()
            elif query_state in ['SUCCEEDED']:
                logging.info("Command Executed Successfully")

            #check if the Athena query results are reflecting in glue metastore
            #This is done only for "alter table ....set location .." query as UpdateTime parameter is updated only for this query.
            #For all the other Athena Queries UpdateTime remains unchanged in Glue(as confirmed by aws)



            if query_state == 'SUCCEEDED':

                if (('set location' in self.command) or ('SET LOCATION' in self.command) or ('SET location' in self.command) or ('set LOCATION' in self.command)):
                        counter = 0
                        max_limit = (self.glue_check_time * 60)
                        #logging.info("glue check loop limit time is:" + str(max_limit))
                        glue_response_after = glue_client.get_table(DatabaseName=self.database_name,Name=self.table_name)
                        UpdateTime = glue_response_after["Table"]["UpdateTime"]
                        while((StartTime == UpdateTime) and (counter <= max_limit)):
                            logging.info("Inside While Loop")
                            counter += 100
                            wait_time = 100
                            glue_client = boto3.client('glue', self.aws_region)
                            glue_response_updated = glue_client.get_table(DatabaseName=self.database_name,Name=self.table_name)
                            UpdateTime = glue_response_updated["Table"]["UpdateTime"]
                            time.sleep(wait_time)

                        #logging.info("Start time is :" + str(StartTime))
                        #logging.info("Update time is:" + str(UpdateTime))

                        if (StartTime > UpdateTime):
                                raise AirflowException("Please contact aws support as updated time of" + self.Table_name + "has been decreased than start time")

                        if (UpdateTime > StartTime):
                                logging.info("Glue metastore updated successfully")
                        elif (self.fail_flag is False):
                                logging.info("Glue metastore not updated, marking success as fail_flag is set to False")
                        else:
                                raise AirflowException("Athena Task failed since glue metastore is not updated and fail_flag is set to True")


        except ClientError as e:
            raise AirflowException('There is some issue in executing the command {0}!!!'.format(e.message))


    def execute(self,context):
        self.execte_query()

class AthenaPlugin(AirflowPlugin):
    name = "athena_plugin"
    operators = [AthenaOperator]



