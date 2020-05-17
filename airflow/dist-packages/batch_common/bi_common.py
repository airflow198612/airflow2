import json
import time
import socket
import subprocess
from boto import ec2, boto
from boto.s3.key import Key
import re
import logging

import boto3
from configparser import ConfigParser

# XXX Pull this from the configuration file, instead
LOCAL_PATH = '/tmp/'


class BICommon(object):
    def get_env(self):
        """
        get environment from the profile
        """

        # get profile from profile.cfg
        profile = self.get_profile()
        env = profile['ENV'].lower()
        return env

    def __init__(self, conf, exception=None, mysql=None):
        self.pyprofile_path = conf.get('code_repo', 'profile')
        self.profile_config_path = conf.get('code_repo', 'profile_config')
        self.env = self.get_env()
        self.batch = str(int(time.time()))
        self.db_user = str(conf.get('oracle_conn', 'db_user'))
        self.db_pwd_path = str(conf.get('oracle_conn', 'db_path')) + self.env + '/.password'
        self.db_conn_opt = " --username " + self.db_user + " --password-file " + self.db_pwd_path
        self.conf = conf
        self.exception = exception
        self.mysql = mysql


    def get_s3_path(self, bucket_name, bucket_key, infile):
        logging.info('construct s3 path from ' + bucket_key + ' ' + infile)
        s3_path = 's3://' + bucket_name + '/' + self.env + '/' + bucket_key + '/' + infile
        logging.info(s3_path)
        return s3_path

    def get_s3_runtime_path(self, bucket_key, prop_file, curr_epoch):
        logging.info('construct s3 runtime path from ' + bucket_key + ' ' + prop_file + ' ' + curr_epoch)
        s3_working = self.conf.get('code_repo', 'working_bucket')
        return self.get_s3_path(s3_working, bucket_key, 'runtime_' + prop_file + '_' + curr_epoch)

    def get_s3_repo_path(self, bucket_key, repo_file):
        logging.info('construct s3 repo path ' + ' ' + bucket_key + ' ' + repo_file)
        return self.get_s3_path(self.conf.get('code_repo', 'repo_bucket'), bucket_key, repo_file)

    def get_file_from_s3(self, key_path, dest_path):
        logging.info('getting file from s3: ' + key_path + ' into ' + dest_path)
        conn = boto.connect_s3()
        bucket = conn.get_bucket(self.conf.get('code_repo', 'repo_bucket'))
        k = Key(bucket)
        k.key = key_path
        k.get_contents_to_filename(dest_path)

    def delete_s3_path(self, s3_path):
        # XXX This will need to be protected as a privileged command
        logging.info("deleting file " + s3_path)
        subprocess.check_call("aws s3 rm " + s3_path + " --recursive", shell=True)

    def archive_s3_path(self, s3_archive_path,s3_source_path):
        logging.info("archiving s3 archive " + s3_archive_path+" to base location "+s3_source_path)
        subprocess.check_call("aws s3 sync s3://" + s3_archive_path + " s3://" + s3_source_path + " --delete", shell=True)

    def get_records_count_from_file(self, cluster_name, date, schema_name, table_name):
        global cluster_id, last_line, log_file
        aws_region = self.conf.get('emr', 'aws_region')
        aws_profile = self.conf.get('emr', 'aws_profile')
        emr_session = boto3.session.Session(region_name=aws_region)
        emr_client = emr_session.client(aws_profile)
        response = emr_client.list_clusters(ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'])
        for cluster in response['Clusters']:
            if cluster['Name'] == cluster_name:
                cluster_id = cluster['Id']
                logging.info("cluster_id is: %s", cluster_id)
        logging.info(
            "Job will wait for 12 mins to fetch log file from s3. log file is updated to s3 for every 10 mins ")
        time.sleep(600)
        conn = boto.connect_s3()
        working_bucket = self.conf.get('code_repo', 'working_bucket')
        bucket = conn.get_bucket(working_bucket)
        path = 'airflow-genie/' + cluster_name + "/" + date + "/logs/" + cluster_id + "/hadoop-mapreduce/history/" + \
               date[:4] + "/" + date[4:6] + "/" + date[6:8] + "/000000/"
        logging.info("log file location in s3 is: %s", path)
        search_string = ".*" + schema_name.upper() + "." + table_name.upper() + "*"
        search_string_query = ".*QueryResult*"
        for key in bucket.list(prefix=path, delimiter='/'):
            ifile = key.name
            if re.search(search_string, ifile) is not None:
                log_file = ifile
            elif re.search(search_string_query, ifile) is not None:
                log_file = ifile

        logging.info("log file is to read: %s", log_file)
        copy_tmp = "aws s3 cp s3://" + working_bucket + "/" + log_file + " /tmp/"
        subprocess.check_call(copy_tmp, shell=True)
        tmp_file = log_file.rsplit("/", 1)[1]
        unzip_file_bash = "gunzip /tmp/" + tmp_file
        subprocess.check_call(unzip_file_bash, shell=True)
        unzip_file = tmp_file.replace(".gz", "")
        with open("/tmp/" + unzip_file, 'r') as txt:
            for line in txt.readlines():
                last_line = line

        logging.info(last_line)
        parsed_json = json.loads(last_line)
        log_count = \
        parsed_json['event']['org.apache.hadoop.mapreduce.jobhistory.JobFinished']['mapCounters']['groups'][1][
                'counts'][1]['value']
        subprocess.check_call("rm -f /tmp/" + unzip_file, shell=True)
        return log_count

    def get_profile(self):
        """
        get the profile
        :return: return the profile in a dictionary
        """
        logging.info('getting pyprofile into dictionary')
        config = ConfigParser()
        config.optionxform = str

        try:
            config.readfp(open(self.profile_config_path))
            profile = dict(config.items('ngap_profile'))
        except Exception:
            profile = {'ENV': ''}

        return profile


    def get_dictionary(self, filename):
        """
        get properties files into a dictory
        :param filename:
        :return: dictiornary from the properties
        """
        logging.info('creating dictionary from ' + filename)
        properties = {}
        infile = open(filename)
        if not infile:
            logging.info('File is empty')
        else:
            logging.info('File is not empty')

        for line in infile:
            pair = line.replace("\n", "").replace('"', '').replace("declare -x ", "")
            (key, _, value) = pair.partition("=")
            properties[key] = value
        logging.info('properties are:', properties)
        return properties

    def translate_properties(self,
                             key_path,
                             runtime_path):
        """ call the translate_prop_file script to translate properties file with common profile
        """
        # First, get profile from s3
        logging.info('translate properties file from ' + key_path + ' into ' + runtime_path)
        # self.get_pyprofile_from_s3()

        # then get the specified properties files from S3 into tmp path
        tmp_path = LOCAL_PATH + 'tran_prop' + '_' + self.batch
        self.get_file_from_s3(key_path, tmp_path)

        # using bash command to translate the properties of the specific properties file from Profile

        bashCommand = "sh translate_prop.sh " + self.pyprofile_path + " " + tmp_path + " " + \
                      runtime_path
        # print bashCommand
        subprocess.check_call(bashCommand, shell=True)

        # remove temp file
        subprocess.check_call("rm -f " + tmp_path, shell=True)

    def get_properties(self, bucket_key, prop_file):
        #  get the specified properties files from S3 into tmp path
        tmp_path = LOCAL_PATH + prop_file + '_' + self.batch
        key_path = self.env + '/' + bucket_key + '/' + prop_file
        self.get_file_from_s3(key_path, tmp_path)

        # Define runtime properties path = /tmp/runtime_<prop file>_batch
        runtime_path = LOCAL_PATH + 'runtime_' + prop_file + '_' + self.batch

        # Translate the properties file and put the file into runtime_path
        self.translate_properties(key_path, runtime_path)
        properties = self.get_dictionary(runtime_path)
        return properties

    def translate_pig_param(self,
                            tmp_path,
                            runtime_path,
                            currdate,
                            prevdate,
                            curr_epoch,
                            prev_epoch):
        """ call the translate_prop_file script to translate properties file with common profile
        """
        # First, get profile from s3
        logging.info(
            'translate pig param to ' + runtime_path + ' curr date ' + currdate + ' prev date ' + prevdate +
            ' curr epoch ' + curr_epoch + 'prev epoch ' + prev_epoch)
        # self.get_pyprofile_from_s3()

        # using bash command to translate the properties of the specific properties file from Profile
        logging.info(
            "sh /var/airflow/plugins/translate_pig_param.sh " + self.pyprofile_path + " " + tmp_path + " " +
            runtime_path + " " + currdate + " " + prevdate + " " + curr_epoch + " " + prev_epoch)
        bashCommand = "sh /var/airflow/plugins/translate_pig_param.sh " + self.pyprofile_path + " " + tmp_path + \
                      " " + runtime_path + " " + currdate + " " + prevdate + " " + curr_epoch + " " + prev_epoch
        subprocess.check_call(bashCommand, shell=True)

        # remove temp file
        subprocess.check_call("rm -f " + tmp_path, shell=True)

    def build_pig_param(self, bucket_key, prop_file, currdate, prevdate, curr_epoch, prev_epoch, curr_ts, prev_ts):
        """
         method return a dictionary object with different properties set up as the properties file
        :param prev_ts:
        :param curr_ts:
        :param bucket_key:
        :param prop_file:
        :param currdate:
        :param prevdate:
        :param curr_epoch:
        :param prev_epoch:
        :return:
        """
        logging.info(
            'building pig param ' + bucket_key + ' ' + prop_file + ' ' + currdate + ' ' + prevdate + ' ' + curr_epoch \
            + ' ' + prev_epoch)
        #  get the specified properties files from S3 into tmp path
        key_path = self.env + '/' + bucket_key + '/' + prop_file
        tmp_path = LOCAL_PATH + prop_file + '_' + self.batch
        self.get_file_from_s3(key_path, tmp_path)
        # Define runtime properties path = /tmp/runtime_<prop file>_batch
        runtime_path = LOCAL_PATH + 'runtime_' + prop_file + '_' + self.batch
        logging.info("runtime_path " + runtime_path)

        # merging profile, currdate, prevate, curr_epoch, prev_epoch to the runtime properties file
        with open(self.pyprofile_path, 'r') as infile:
            lines = infile.readlines()

        with open(runtime_path, 'w') as outfile:
            for line in lines:
                if "--username" not in line:
                    b = line[0:line.index('=') + 1] + '"' + line[line.index('=') + 1:line.index('\n')] + '"'
                    outfile.write(b + '\n')
            outfile.write('CURRDATE = "' + currdate + '"\n')
            outfile.write('CURR_EPOCH = "' + curr_epoch + '"\n')
            outfile.write('CURR_TS = "' + curr_ts + '"\n')
            outfile.write('PREVDATE = "' + prevdate + '"\n')
            outfile.write('PREV_EPOCH = "' + prev_epoch + '"\n')
            outfile.write('PREV_TS = "' + prev_ts + '"\n')
            with open(tmp_path, 'r') as infile:
                outfile.write(infile.read())

        # copy runtime file from temp directory to s3
        working_path = self.get_s3_runtime_path(bucket_key, prop_file, curr_epoch)
        bashCommand = "aws s3 cp " + runtime_path + " " + working_path
        logging.info(bashCommand)
        subprocess.check_call(bashCommand, shell=True)

        bashCommand = "rm -f " + runtime_path
        subprocess.check_call(bashCommand, shell=True)
        return working_path
