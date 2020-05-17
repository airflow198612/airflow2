from __future__ import print_function, unicode_literals
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from airflow.utils.decorators import apply_defaults
import logging
from airflow.hooks.dbapi_hook import DbApiHook
from airflow.models import Connection
from airflow.utils.db import provide_session
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.dbapi_hook import DbApiHook
from cerberus.client import CerberusClient
from boxsdk import *
from boxsdk import JWTAuth
import logging
import boto3,botocore
import os , shutil
from urlparse import urlparse


# Will show up under airflow.operators.PluginOperator
class BoxFileTransferOperator(BaseOperator):
    """
    CREATE BY : shabarish vemula
    Connects to the URL for an event trigger.
    *******TBD********:param event_name: reference to a specific Metric Insights Event
    """
    ui_color = '#ededef'

    @apply_defaults
    def __init__(self, conn_id='box', box_dir=None, box_file_name=None,box_folder_id=None, s3path=None, s3_Region=None, file_type=None, *args, **kwargs):
        super(BoxFileTransferOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        if not self.conn_id:
            raise ValueError("Must provide reference to Box connection in airflow!")
        self.box_dir = box_dir
        self.box_file_name = box_file_name
        self.box_folder_id=box_folder_id
        self.box_url = None
        self.box_username = None
        self.box_password = None
        self.s3path = s3path
        self.file_type = file_type
        self.s3_Region = s3_Region

    def execute(self, context):
        # Getting URL for the conn_id
        client = self.get_conn()
        logging.info('Box dir is:' + str(self.box_dir))
        logging.info('Box filename:' + str(self.box_file_name))
        logging.info('Box folder_id:' + str(self.box_folder_id))
        self.upload_file()
  

    def get_conn(self):
        """
        Returns an authentication with app in box.
        """

        try:
	    client = CerberusClient('https://prod.cerberus.nikecloud.com')
            sdk = JWTAuth(
                client_id=client.get_secrets_data("shared/ngap-box/client_id")["client_id"],
                client_secret=client.get_secrets_data("shared/ngap-box/client_secret")["client_secret"],
                enterprise_id=client.get_secrets_data("shared/ngap-box/enterprise_id")["enterprise_id"],
                jwt_key_id=client.get_secrets_data("shared/ngap-box/jwt_key_id")["jwt_key_id"],
                rsa_private_key_data=((client.get_secrets_data("shared/ngap-box/boxpemkey")["pemkey"]).replace('\\n', '\n')),
                rsa_private_key_passphrase=bytes((client.get_secrets_data("shared/ngap-box/rsa_private_key_passphrase")["rsa_private_key_passphrase"]).encode('utf-8'))
            )
            client = Client(sdk)
            return client
        except TypeError as e:
            print(e)
    def s3_copy_local(self,region,script_path):
        logging.info("Will try to get contents of " + script_path)
        directory = '/tmp/box_scripts/'
        s3 = boto3.resource('s3', region_name=region)
        o = urlparse(script_path)
        bucket_name = o.netloc
        s3_object_key = o.path.strip('/')
        bucket = s3.Bucket(bucket_name)
        response_iterator = bucket.objects.filter(Prefix=s3_object_key)
        for response in response_iterator:
            logging.info(response)
            path, filename = os.path.split(response.key)
            logging.info("path is " + path)
            logging.info("filename is " + filename)
            logging.info("response is " + response.key)
            if filename.endswith(self.file_type):
                logging.info(filename)
                if not os.path.exists(directory):
                    os.makedirs('/tmp/box_scripts/')
                local_file_path = '/tmp/box_scripts/' + response.key.split('/')[-1]
                bucket.download_file(response.key, local_file_path)
                logging.info(local_file_path)
                client = self.get_conn()
                client.folder(self.box_folder_id).upload(local_file_path, self.box_file_name)
                os.remove(local_file_path)

            else:
                logging.info("No %s files in %s" %(self.file_type,s3_object_key))
        # shutil.rmtree(directory)

    def upload_file(self):
        """
        Upload a file to the specific location
        """
        client = self.get_conn()
        file = str(self.s3path).strip()
        file_content = self.s3_copy_local(self.s3_Region, file)


class S3BoxFileTransferOperator(BaseOperator):
    """
    CREATE BY : shabarish vemula
    Connects to the URL for an event trigger.
    *******TBD********:param event_name: reference to a specific Metric Insights Event
    """
    ui_color = '#ededef'

    @apply_defaults
    def __init__(self, conn_id='box', box_dir=None, box_file_name=None,box_folder_id=None, s3path=None, s3_Region=None, file_type=None, *args, **kwargs):
        super(S3BoxFileTransferOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        if not self.conn_id:
            raise ValueError("Must provide reference to Box connection in airflow!")
        self.box_dir = box_dir
        self.box_file_name = box_file_name
        self.box_folder_id=box_folder_id
        self.box_url = None
        self.box_username = None
        self.box_password = None
        self.s3path = s3path
        self.file_type = file_type
        self.s3_Region = s3_Region

    def execute(self, context):
        # Getting URL for the conn_id
        client = self.get_conn()
        logging.info('Box dir is:' + str(self.box_dir))
        logging.info('Box filename:' + str(self.box_file_name))
        logging.info('Box folder_id:' + str(self.box_folder_id))
        self.upload_file()

    def get_conn(self):
        """
        Returns an authentication with app in box.
        """

        try:
            client = CerberusClient('https://prod.cerberus.nikecloud.com')
            sdk = JWTAuth(
                client_id=client.get_secrets_data("shared/ngap-box/client_id")["client_id"],
                client_secret=client.get_secrets_data("shared/ngap-box/client_secret")["client_secret"],
                enterprise_id=client.get_secrets_data("shared/ngap-box/enterprise_id")["enterprise_id"],
                jwt_key_id=client.get_secrets_data("shared/ngap-box/jwt_key_id")["jwt_key_id"],
                rsa_private_key_data=((client.get_secrets_data("shared/ngap-box/boxpemkey")["pemkey"]).replace('\\n', '\n')),
                rsa_private_key_passphrase=bytes((client.get_secrets_data("shared/ngap-box/rsa_private_key_passphrase")["rsa_private_key_passphrase"]).encode('utf-8'))
            )
            client = Client(sdk)
            return client
        except TypeError as e:
            print(e)
    def download_box_local(self,file_id,filename):
        client = self.get_conn()
        with open("/tmp/" + filename, 'wb') as file_loc:
            client.file(file_id).download_to(file_loc)
            file_loc.close()
    def box_to_s3(self,region,script_path,filename):
        s3 = boto3.resource('s3', region_name=region)
        o = urlparse(script_path)
        bucket_name = o.netloc
        s3_object_key = o.path.strip('/')
        try:
            s3.meta.client.upload_file("/tmp/" + filename, bucket_name, s3_object_key + '/' + filename)
            logging.info("Files are uploaded to %s" % script_path)
            os.remove("/tmp/" + filename)
        except botocore.exceptions.ClientError as e:
            logging.info("File was not uploaded successfully", e)

    def upload_file(self):
        """
        Upload a file to the specific location
        """
        client = self.get_conn()
        items = client.folder(folder_id=self.box_folder_id).get_items(offset=0)
        for file in items:
            file_id = file.get()['id']
            file_name = file.get()['name']
            file_type = file.get()['type']
            if file_type == 'file':
                self.download_box_local(file_id,file_name)
                self.box_to_s3(self.s3_Region,self.s3path,file_name)

# Defining the Plugin class
class BoxPlugin(AirflowPlugin):
    name = "box_plugin"
    operators = [BoxFileTransferOperator,S3BoxFileTransferOperator]

