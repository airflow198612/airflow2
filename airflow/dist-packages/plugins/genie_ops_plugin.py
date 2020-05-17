#!/usr/bin/python2.7

#######################################################################################################################
# cust_ops_plugin.py
# Description: custom operators for Nike BI batch processing
# Go to the bottom of the code to see what are the operators created in this code
# the code
#######################################################################################################################
import logging
import time

from airflow import settings
from airflow.models import BaseOperator, XCom
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
import batch_common
from batch_common import BICommon, SqoopTools

import boto3
import urllib
from airflow.exceptions import AirflowException
import genie2.client.wrapper
import genie2.model.ClusterCriteria
import genie2.model.Job
import genie2.model.FileAttachment
from airflow.configuration import conf
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import and_
# from plugins import make_emr_client # only if code is in init

# paas SDK
from emr_client.rest import ApiException
# hooks
from hooks.paas_hooks import EmrApiHook
from snowflake_plugin import SnowFlakeOperator, SnowFlakeHook


class GenieHook(BaseHook):
    """
    Interact with Genie
    """

    def __init__(self, config=conf):
        self.genie_endpoint = config.get('genie', 'GENIE_ENDPOINT')
        self.retries_times = int(config.get('genie', 'RETRIES_TIMES'))
        self.none_on_404 = bool(config.get('genie', 'NONE_ON_404'))
        self.no_retry_http_low = int(config.get('genie', 'NO_RETRY_HTTP_LOW'))
        self.no_retry_http_high = int(config.get('genie', 'NO_RETRY_HTTP_HIGH'))

    def get_genie(self):
        """
        Returns Genie object
        """
        genie = genie2.client.wrapper.Genie2(self.genie_endpoint,
                                             genie2.client.wrapper.RetryPolicy(tries=self.retries_times,
                                                                               none_on_404=self.none_on_404,
                                                                               no_retry_http_codes=range(
                                                                                   self.no_retry_http_low,
                                                                                   self.no_retry_http_high)))

        return genie

    # implemented for contract as abstract methods
    def get_pandas_df(self, sql):  # pragma: no cover
        pass

    def run(self, sql):  # pragma: no cover
        pass

    def get_records(self, sql):  # pragma: no cover
        pass

    def get_conn(self):  # pragma: no cover
        pass


class GenieOperator(BaseOperator):
    """
    Submit a job to Genie
    :param command: The command, set of commands to submit.
    :type command: string
    :param job_name: Name of job
    :type job_name: string
    :param job_type: Type of job
    :type job_type: string
    :param sched_type: schedule type - GenieAdhoc, etl
    :type sched_type: string
    :param version:  version of the command to use
    :type version: string
    :param file_dependencies: file to be uploaded to Genie
    :type file_dependencies: string
    :param env: environment variables
    :type env: dict
    :
    """
    template_fields = ('command', 'job_name', 'job_type', 'sched_type', 'version', 'file_dependencies', 'env')

    ui_color = '#D8BFD8'

    @apply_defaults
    def __init__(self,
                 command,
                 job_type,
                 job_name=None,
                 sched_type='GenieAdhoc',
                 version='default',
                 file_dependencies=None,
                 env=None,
                 conn_id="emr_api_default",
                 *args, **kwargs):
        super(GenieOperator, self).__init__(*args, **kwargs)
        self.command = command
        self.job_type = job_type
        self.job_name = job_name
        self.sched_type = sched_type
        self.version = version
        self.file_dependencies = file_dependencies
        self.env = env
        if self.job_name is None:
            self.job_name = kwargs['task_id']
        self.conn_id = conn_id
        self.emr_client = None

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

        return result.value if result else None

    def make_emr_client(self):
        connection = BaseHook.get_connection(self.conn_id)
        print connection.host
        return EmrApiHook(connection)

    def execute(self, context):
        # Create a Genie client which proxies API calls through wrapper which retries failures based on various
        #  return codes

        genie = GenieHook().get_genie()

        # Get emr client to describe cluster
        self.emr_client = self.make_emr_client()

        try:
            response = self.emr_client.describe_cluster(self.sched_type)
            logging.info("EMR cluster name: %s", self.sched_type)
            logging.info("EMR cluster id: %s", response.cluster_id)
            logging.info("EMR version: %s", response.version)
            logging.info("EMR describe cluster: %s", response)

        except ApiException as e:
            logging.error("Exception when calling describe cluster: %s\n" % e)

        # Configure spark command
        if self.job_type == 'spark':
            spark_hive_site = response.applications.spark_hive_site
            if response.version not in ['emr-5.0.0', 'emr-5.1.0', 'emr-5.2.0', 'emr-5.3.0', 'emr-5.3.1']:
                s3_path = spark_hive_site.replace("spark-hive-site.xml", "")
                files = spark_hive_site + "," + s3_path + "truststore.jks," + s3_path + "keystore.jks"
            else:
                files = spark_hive_site
            if '--files ' in self.command:
                commands = self.command.split("--files ")
                spark_command = commands[0] + " --files " + files + ',' + commands[1]
            else:
                spark_command = "--files " + files + " " + self.command
            self.command = spark_command

        # Update the Hive and Spark command string based on Okera
        if response.cerebro == 'true':
            if self.job_type == 'spark':
                spark_cmd = " --jars hdfs:///user/hadoop/lib/okera-hive-metastore.jar,hdfs:///user/hadoop/lib/recordservice-spark-2.0.jar " + self.command
                self.command = spark_cmd
            elif self.job_type == 'hive':
                if '-e ' in self.command:
                    hive_cmd = self.command.split("\"", 1)[1]
                    hive_cmd = '-e \"'+'add jar hdfs:///user/hadoop/lib/okera-hive-metastore.jar; add jar hdfs:///user/hadoop/lib/recordservice-hive.jar; '+hive_cmd
                    self.command = hive_cmd

        # Create a job instance and fill in the required parameters
        job = genie2.model.Job.Job()

        # Get execution date from context to append to job name
        job.name = self.job_name + '_' + str(context['execution_date'])

        # Set job type
        job.user = "mapred"
        if self.job_type in ('pig', 'hive', 'spark'):
            job.user = "hadoop"

        # Set job version
        if self.version != 'default':
            job.version = self.version
        else:
            job.version = "2.6.0"

        # Create a list of cluster criterias which determine the cluster to run the job on
        job.clusterCriterias = list()
        cluster_criteria = genie2.model.ClusterCriteria.ClusterCriteria()
        criteria = set()
        criteria.add("sched:" + self.sched_type)
        criteria.add("type:yarn")
        cluster_criteria.tags = criteria
        job.clusterCriterias.append(cluster_criteria)

        # Create the set of command criteria which will determine what command Genie executes for the job
        logging.info('job type: ' + self.job_type)
        logging.info('command: ' + self.command)
        logging.info(str(criteria))
        command_criteria = set()
        command_criteria.add("type:" + self.job_type)
        job.commandCriteria = command_criteria

        # Add any dependencies for this job. Could use attachments but since these are already available on the system
        # will instead use the file dependencies field.
        job.fileDependencies = self.file_dependencies

        # Any command line arguments to run along with the command. In this case it holds the actual query but this
        # could also be done via an attachment or file dependency.
        job.commandArgs = str(self.command)
        logging.info("submitting job")
        # Submit the job to Genie
        job = genie.submitJob(job)

        # Get the kibana url
        kibana_url = conf.get('kibana', 'kibana_endpoint')
        # Pin the emr cluster name in kibana
        kibana_pinned_filter = "?#/dashboard/EMR-Cluster-Metrics-and-Applications-Dashboard?_g=(filters:!((meta:(" \
                               "disabled:!f,index:'logstash-genie-logs*',key:emr_cluster_name,negate:!f," \
                               "value:" + self.sched_type + "),query:(match:(emr_cluster_name:(query:" + self.sched_type + "," \
                               "type:phrase))))),refreshInterval:(display:Off,pause:!f,section:0,value:0)," \
                               "time:(from:now-30m,mode:quick,to:now))&_a=(filters:!(),panels:!((col:1," \
                               "id:Cluster-Metrics-M-Application-Status,row:6,size_x:6,size_y:2,type:visualization)," \
                               "(col:1,id:Cluster-Metrics-M-Cluster-Status,row:8,size_x:6,size_y:2," \
                               "type:visualization),(col:7,id:Cluster-Stats-PC-Application-Total-Elapsed-Time,row:6," \
                               "size_x:6,size_y:4,type:visualization),(col:1," \
                               "id:Cluster-Stats-AC-Available-vs.-Allocated-MB,row:10,size_x:6,size_y:4," \
                               "type:visualization),(col:7,id:Cluster-Stats-AC-Available-vs.-Allocated-VCores,row:10," \
                               "size_x:6,size_y:4,type:visualization),(col:1," \
                               "id:Cluster-Stats-LC-Application-Allocated-MB,row:14,size_x:6,size_y:4," \
                               "type:visualization),(col:7,id:Cluster-Stats-LC-Application-Allocated-VCores,row:14," \
                               "size_x:6,size_y:4,type:visualization),(col:7," \
                               "id:Genie-Logs-DT-Log-Levels-by-EMR-Cluster,row:18,size_x:6,size_y:4," \
                               "type:visualization),(col:1,columns:!(emr_cluster_name,genie_job_id,genie_job_type," \
                               "log_message),id:Genie-Job-Commands,row:18,size_x:6,size_y:4,sort:!('@timestamp',asc)," \
                               "type:search),(col:2,id:EMR-Cluster-Metrics-and-Applications-Dashboard-Note,row:1," \
                               "size_x:10,size_y:5,type:visualization)),query:(query_string:(analyze_wildcard:!t," \
                               "query:'*')),title:'EMR%20Cluster%20Metrics%20and%20Applications%20Dashboard') "
        kibana_pinned_url = kibana_url + kibana_pinned_filter

        logging.info(
            "<a href=" + kibana_pinned_url + "style='color: rgb(0,255,0)'><font color='00994C'>click me for detailed logs in kibana</font></a>")

        while job.status != "SUCCEEDED" and job.status != "KILLED" and job.status != "FAILED":
            logging.info("Job " + job.id + " is " + job.status)
            time.sleep(10)
            job = genie.getJob(job.id)

        if job.status == "SUCCEEDED":
            logging.info("Job succeeded...")
        else:
            raise ValueError('Exception submitting job')

        return job.status


class GenieHiveOperator(GenieOperator):
    @apply_defaults
    def __init__(self,
                 command,
                 job_name=None,
                 job_type='hive',
                 sched_type='GenieAdhoc',
                 version='default',
                 file_dependencies=None,
                 env=None,
                 *args, **kwargs):
        super(GenieHiveOperator, self).__init__(command=command,
                                                job_name=job_name,
                                                job_type=job_type,
                                                sched_type=sched_type,
                                                version=version,
                                                file_dependencies=file_dependencies,
                                                env=env,
                                                *args, **kwargs)


class GeniePigOperator(GenieOperator):
    @apply_defaults
    def __init__(self,
                 command,
                 job_name=None,
                 job_type='pig',
                 sched_type='GenieAdhoc',
                 version='default',
                 file_dependencies=None,
                 env=None,
                 *args, **kwargs):
        super(GeniePigOperator, self).__init__(command=command,
                                               job_name=job_name,
                                               job_type=job_type,
                                               sched_type=sched_type,
                                               version=version,
                                               file_dependencies=file_dependencies,
                                               env=env,
                                               *args, **kwargs)


class GeniePigBatchOperator(GeniePigOperator):
    @apply_defaults
    def __init__(self,
                 prop_key_path,
                 prop_file,
                 script_key_path,
                 script_file,
                 job_name=None,
                 use_hcatalog=False,
                 job_type='pig',
                 sched_type='GenieAdhoc',
                 version='default',
                 options=None,
                 file_dependencies=None,
                 env=None,
                 param='',
                 *args, **kwargs):

        super(GeniePigBatchOperator, self).__init__(command=None,
                                                    job_name=job_name,
                                                    job_type=job_type,
                                                    sched_type=sched_type,
                                                    version=version,
                                                    file_dependencies=file_dependencies,
                                                    env=env,
                                                    param='',
                                                    *args, **kwargs)

        # process the properties file provided by the batch job
        logging.info(kwargs)
        self.prop_key_path = prop_key_path
        self.prop_file = prop_file
        self.script_key_path = script_key_path
        self.script_file = script_file
        self.env_type = None
        self.use_hcatalog = use_hcatalog
        self.options = options
        self.param = param


    def execute(self, context):

        logging.info(context['ti'])
        ti = context['ti']
        curr_epoch = str(ti.xcom_pull(key='curr_epoch', task_ids='start_batch'))
        currdate = context['ds_nodash']
        execution_date = context['execution_date'].strftime('%Y-%m-%dT%H:%M:%S')
        curr_ts = execution_date.replace(':', '').replace('-', '')[8:]
        logging.info('current epoch = %s', curr_epoch)

        bicommon = BICommon()
        self.profile = bicommon.get_profile()
        self.env_type = bicommon.env

        prev_run = self.get_prev_run(context)
        if prev_run is not None:
            prev_epoch = str(prev_run['curr_epoch'])
            prevdate = prev_run['curr_rundate']
            if prev_run['execution_date'] is not None:
                prev_ts = prev_run['execution_date'][11:].replace(':', '')
            else:
                prev_ts = ''
        else:
            prev_epoch = ''
            prevdate = ''
            prev_ts = ''

        logging.info(prev_epoch)

        s3_script_path = bicommon.get_s3_repo_path(self.script_key_path, self.script_file)

        s3_runtime_path = bicommon.build_pig_param(self.prop_key_path, self.prop_file, currdate, prevdate, curr_epoch,
                                                   prev_epoch, curr_ts, prev_ts)

        if self.options is None:
            self.options = ""

        if self.use_hcatalog:
            self.command = self.options + ' -useHCatalog --param_file ' + s3_runtime_path + ' ' + self.param + ' ' + s3_script_path
        else:
            self.command = self.options + ' --param_file ' + s3_runtime_path + ' ' + self.param + ' ' + s3_script_path
        super(GeniePigBatchOperator, self).execute(context)


class GenieHadoopOperator(GenieOperator):
    @apply_defaults
    def __init__(self,
                 command,
                 job_name=None,
                 job_type='hadoop',
                 sched_type='GenieAdhoc',
                 version='default',
                 file_dependencies=None,
                 env=None,
                 *args, **kwargs):
        super(GenieHadoopOperator, self).__init__(command=command,
                                                  job_name=job_name,
                                                  job_type=job_type,
                                                  sched_type=sched_type,
                                                  version=version,
                                                  file_dependencies=file_dependencies,
                                                  env=env,
                                                  *args, **kwargs)


class GenieS3DistCpOperator(GenieOperator):
    @apply_defaults
    def __init__(self,
                 command,
                 job_name=None,
                 job_type='hadoop',
                 sched_type='GenieAdhoc',
                 version='default',
                 jar="/apps/hadoop/current/share/hadoop/common/lib/s3-dist-cp.jar",
                 file_dependencies=None,
                 env=None,
                 *args, **kwargs):
        super(GenieS3DistCpOperator, self).__init__(command="jar " + jar + " " + command,
                                                    job_name=job_name,
                                                    job_type=job_type,
                                                    sched_type=sched_type,
                                                    version=version,
                                                    file_dependencies=file_dependencies,
                                                    env=env,
                                                    *args, **kwargs)


class GenieSparkOperator(GenieOperator):
    @apply_defaults
    def __init__(self,
                 command,
                 job_name=None,
                 job_type='spark',
                 sched_type='GenieAdhoc',
                 version='default',
                 file_dependencies=None,
                 env=None,
                 *args, **kwargs):
        super(GenieSparkOperator, self).__init__(command=command,
                                                 job_name=job_name,
                                                 job_type=job_type,
                                                 sched_type=sched_type,
                                                 version=version,
                                                 file_dependencies=file_dependencies,
                                                 env=env,
                                                 *args, **kwargs)
        self.command = ' --master yarn --deploy-mode cluster ' + self.command


class CanaryOperator(GenieOperator):
    def __init__(self,
                 command,
                 job_name=None,
                 job_type='spark',
                 sched_type='GenieAdhoc',
                 version='default',
                 file_dependencies=None,
                 env=None,
                 *args, **kwargs):
        super(CanaryOperator, self).__init__(command=command,
                                             job_name=job_name,
                                             job_type=job_type,
                                             sched_type=sched_type,
                                             version=version,
                                             file_dependencies=file_dependencies,
                                             env=env,
                                             *args, **kwargs)

        if isinstance(self.command, dict):
            # Setup the mandatory parameters for canary validation
            mandatory_keys = ('dag_name', 'dag_exec_dt', 'validation_type', 's3_canary_file')
            self.canary_conn_id = 'canary'

            # Check all the mandatory parameters exists in input dictionary
            if all(key in self.command for key in mandatory_keys):
                default_spark_configs = ' --conf spark.yarn.maxAppAttempts=1 '
                spark_configs = self.command.get('spark_configs', default_spark_configs)

                # Deriving airflow cluster name and environment
                cluster = conf.get('core', 'cluster')
                if 'dev' in cluster:
                    airflow_env = 'dev'
                    airflow_cluster_name = cluster.replace(airflow_env, '')
                elif 'qa' in cluster:
                    airflow_env = 'qa'
                    airflow_cluster_name = cluster.replace(airflow_env, '')
                else:
                    airflow_env = 'prod'
                    airflow_cluster_name = cluster

                # Updating with airflow details
                self.command.update({'airflow_cluster_name': airflow_cluster_name, 'airflow_env': airflow_env})

                self.canary_db_conn = self.get_canary_mysql_conn()
                self.command.update(self.canary_db_conn)

                canary_path = ' --jars s3://nike-emr-working/' + airflow_env + '/common/mysql-connector-java-5.1.26.jar ' \
                                                                               '--files s3://nike-emr-bin/' + airflow_env + '/common/scripts/canary_defaults.conf ' \
                                                                                                                            's3://nike-emr-bin/' + airflow_env + '/common/scripts/canary_validation.py '

                import json
                command_json = json.dumps(self.command)
                self.command = ' --master yarn --deploy-mode cluster ' + spark_configs + canary_path + command_json

                # logging.info(self.command)
            else:
                logging.error("Mandatory keys for canary execution: %s\n" % format(mandatory_keys))
                logging.error("Mandatory key(s) NOT exists in input: %s\n" % format(self.command))
                raise Exception

        else:
            logging.error("Input is NOT a dictionary: %s\n" % format(self.command))
            raise Exception

    def get_canary_mysql_conn(self):
        """
        Returns host, db, login details as configured in connection.
        """
        from airflow.hooks.dbapi_hook import DbApiHook
        conn = DbApiHook.get_connection(self.canary_conn_id)
        canary_username = conn.login
        canary_password = conn.password
        canary_database = conn.schema
        canary_hostname = conn.host

        # logging.info("canary_username={0}, canary_database={1}, canary_hostname={2}"
        #             .format(canary_username, canary_database, canary_hostname))
        return {'canary_hostname': canary_hostname,
                'canary_database': canary_database,
                'canary_jdbc_conn_props':
                    {'user': canary_username,
                     'password': canary_password
                     }
                }

class GenieSqoopOperator(GenieOperator):
    @apply_defaults
    def __init__(self,
                 command,
                 job_name=None,
                 job_type='sqoop',
                 sched_type='GenieAdhoc',
                 version='default',
                 file_dependencies=None,
                 env=None,
                 *args, **kwargs):
        super(GenieSqoopOperator, self).__init__(command=command,
                                                 job_name=job_name,
                                                 job_type=job_type,
                                                 sched_type=sched_type,
                                                 version=version,
                                                 file_dependencies=file_dependencies,
                                                 env=env,
                                                 *args, **kwargs)


class GenieSqoopBatchOperator(GenieSqoopOperator):
    @apply_defaults
    def __init__(self,
                 prop_key_path,
                 prop_file,
                 cluster_name,
                 load_type="FULL",
                 ignore_count=True,
                 job_name=None,
                 job_type='sqoop',
                 sched_type='GenieAdhoc',
                 version='default',
                 file_dependencies=None,
                 env=None,
                 *args, **kwargs):
        super(GenieSqoopBatchOperator, self).__init__(
            command=None,
            job_name=job_name,
            job_type=job_type,
            sched_type=sched_type,
            version=version,
            file_dependencies=file_dependencies,
            env=env,
            *args, **kwargs)
        # process the properties file provided by the batch job
        self.prop_key_path = prop_key_path
        self.prop_file = prop_file
        self.env_type = None
        self.load_type = load_type.upper()
        self.cluster_name = cluster_name
        self.operator = 'GenieSqoopBatchOperator'
        self.ignore_count = ignore_count

    def execute(self, context):
        bicommon = BICommon()
        self.profile = bicommon.get_profile()
        self.env_type = bicommon.env
        sqoop_tools = SqoopTools(self.env_type)
        sqoop_prop = sqoop_tools.build_sqoop_properties(self.prop_key_path, self.prop_file, self.load_type,
                                                     self.ignore_count)
        self.command = sqoop_prop['SQOOP_CMD']
        if sqoop_prop['TARGET_IS_S3']:
            logging.info(
                'delete s3 directory' + sqoop_prop['S3_TARGET_DIRECTORY'] + ' ' + sqoop_prop['S3_TGT_FOLDER_FILE'])
            bicommon.delete_s3_path(sqoop_prop['S3_TARGET_DIRECTORY'])
            bicommon.delete_s3_path(sqoop_prop['S3_TGT_FOLDER_FILE'])
        status = super(GenieSqoopBatchOperator, self).execute(context)

        if status != "SUCCEEDED":
            logging.info("Job failed")
            raise Exception("Job failed")
        else:
            logging.info("Sqoop Job Succeeded")
            if not self.ignore_count:
                logging.info('Oracle Count: %s', sqoop_prop['COUNT'])
                count = sqoop_prop['COUNT']
                logging.info("Validating counts from source and sqoop log")
                logging.info("Count from the source: %s", count)
                logging.info("cluster_name: %s", self.cluster_name)
                logging.info("schema_name: %s", sqoop_prop['SCHEMA'])
                logging.info("table_name: %s", sqoop_prop['TABLE_VIEW_NAME'])
                date = time.strftime("%Y%m%d")
                logging.info("date: %s", date)
                log_records_count = bicommon.get_records_count_from_file(self.cluster_name, date, sqoop_prop['SCHEMA'],
                                                                         sqoop_prop['TABLE_VIEW_NAME'])
                logging.info("Count from the log file: %s", log_records_count)
                tolerance = int(log_records_count) * float(sqoop_prop['STREAM_TOLERANCE'])
                count_with_tolerance = log_records_count + int(tolerance)
                logging.info("Count from the target file with tolerance: %s", count_with_tolerance)
                if count <= count_with_tolerance:
                    logging.info("Counts matched Job is Success ")
                else:
                    logging.info("Counts are not matched ,Job Failed")
                    raise Exception("Counts are not matched in source and log file ,Job Failed")

class GenieFullLoadOperator(GenieHiveOperator):
    @apply_defaults
    def __init__(self,
                 prop_key_path,
                 prop_file,
                 job_name=None,
                 job_type='hive',
                 sched_type='GenieAdhoc',
                 version='default',
                 file_dependencies=None,
                 env=None,
                 *args, **kwargs):
        super(GenieFullLoadOperator, self).__init__(
            command=None,
            job_name=job_name,
            job_type=job_type,
            sched_type=sched_type,
            version=version,
            file_dependencies=file_dependencies,
            env=env,
            *args, **kwargs)

        self.prop_key_path = prop_key_path
        self.prop_file = prop_file
        self.env_type = None

    def execute(self, context):
        """

        :param context:
        """
        bicommon = BICommon()
        self.profile = bicommon.get_profile()
        self.env_type = bicommon.env
        sqoop_tools = SqoopTools(self.env_type)
        logging.info(self.prop_file, self.prop_key_path)
        full_load_prop = sqoop_tools.build_full_load_properties(self.prop_key_path, self.prop_file)
        self.command = full_load_prop['full_load']
        status = super(GenieFullLoadOperator, self).execute(context)
        if status != "SUCCEEDED":
            logging.info("Job failed")
            logging.info("Executing AWS S3 SYNC from archive")
            bicommon.archive_s3_path(full_load_prop['ARC_DIRECTORY'],full_load_prop['BASE_DIRECTORY'])
            logging.info("Executing AWS S3 SYNC from archive is done")
            raise Exception("Job failed")
        else:
            logging.info("Job Succeeded")

class GenieHiveIncMergeOperator(GenieHiveOperator):
    @apply_defaults
    def __init__(self,
                 prop_key_path,
                 prop_file,
                 job_name=None,
                 job_type='hive',
                 sched_type='GenieAdhoc',
                 version='default',
                 file_dependencies=None,
                 env=None,
                 *args, **kwargs):
        super(GenieHiveIncMergeOperator, self).__init__(
            command=None,
            job_name=job_name,
            job_type=job_type,
            sched_type=sched_type,
            version=version,
            file_dependencies=file_dependencies,
            env=env,
            *args, **kwargs)

        self.prop_key_path = prop_key_path
        self.prop_file = prop_file
        self.env_type = None

    def execute(self, context):
        """

        :param context:
        """
        bicommon = BICommon()
        self.env_type = bicommon.env
        sqoop_tools = SqoopTools(self.env_type)
        logging.info(self.prop_file, self.prop_key_path)
        inc_merge_prop = sqoop_tools.build_inc_merge_properties(self.prop_key_path, self.prop_file)
        self.command = inc_merge_prop['QUERY']
        status = super(GenieHiveIncMergeOperator, self).execute(context)

        if status != "SUCCEEDED":
            logging.info("Executing AWS S3 SYNC from archive")
            bicommon.archive_s3_path(inc_merge_prop['S3_ARCH_TABLE_DIR'],inc_merge_prop['HDFS_BASE_TABLE_DIR'])
            logging.info("Executing AWS S3 SYNC from archive is done")
            raise Exception
        else:
            logging.info("Job Succeeded")

class GenieSqoop_V2_operator(GenieSqoopOperator):
    '''
        Create and Execute sqoop command from input params and also create and execute Sqoop Job (if job exists otherwise creates a new job and execute).
    '''
    FULL_LOAD_TYPE = 'FULL'
    INC_LOAD_TYPE = 'INC'
    DEFAULT_MAPPER_COUNT = 4
    TEXT_FORAMT = 'TEXT'
    AVRO_FORMAT = 'AVRO'
    PARQUET_FORMAT = 'PARQUET'
    COMPRESSION_CODEC = 'org.apache.hadoop.io.compress.SnappyCodec'
    DEFAULT_TEXT_FIELD_DELIMITER = '\\t'
    INC_MODE_APPEND = 'APPEND'
    INC_MODE_LAST_MODIFIED = 'LAST_MODIFIED'
    SQOOP_TYPE_IMPORT = 'import'
    SQOOP_TYPE_EXPORT = 'export'
    default_streamTolerance = 0.005

    @apply_defaults
    def __init__(self,
                 cluster_name,
                 command=None,
                 job_name=None,
                 job_type='sqoop',
                 sched_type='GenieAdhoc',
                 version='default',
                 file_dependencies=None,
                 env=None,
                 sqoop_type=SQOOP_TYPE_IMPORT,
                 load_type=None,
                 schemaName=None,
                 tableName=None,
                 targetDir=None,
                 splitByColumn=None,
                 mapperCount=DEFAULT_MAPPER_COUNT,
                 dataOutputFileFormat=TEXT_FORAMT,
                 columns=None,
                 mapColumns=None,
                 sqoopJobName=None,
                 incrementalMode=None,
                 checkColumnName=None,
                 checkColLastValue=None,
                 compressCodec=COMPRESSION_CODEC,
                 fieldDelimiter=DEFAULT_TEXT_FIELD_DELIMITER,
                 isView=None,
                 isQuery=None,
                 queryString=None,
                 streaming=None,
                 sourceEnv=None,
                 streamTolerance=default_streamTolerance,
                 additionalArgs=None,
                 *args, **kwargs):
        super(GenieSqoop_V2_operator, self).__init__(command=command,
                                                     job_name=job_name,
                                                     job_type=job_type,
                                                     sched_type=sched_type,
                                                     version=version,
                                                     file_dependencies=file_dependencies,
                                                     env=env,
                                                     *args, **kwargs)
        self.env_type = None
        self.sqoop_conf_parm = {}
        self.cluster_name = self.sqoop_conf_parm['cluster_name'] = cluster_name
        self.env = env
        self.sourceEnv = sourceEnv
        self.sqoop_conf_parm['sqoop_type'] = sqoop_type
        self.sqoop_conf_parm['load_type'] = load_type
        self.sqoop_conf_parm['schemaName'] = schemaName
        self.sqoop_conf_parm['tableName'] = tableName
        self.sqoop_conf_parm['targetDir'] = targetDir
        self.sqoop_conf_parm['splitByColumn'] = splitByColumn
        self.sqoop_conf_parm['mapperCount'] = mapperCount
        self.sqoop_conf_parm['dataOutputFileFormat'] = dataOutputFileFormat
        self.sqoop_conf_parm['columns'] = columns
        self.sqoop_conf_parm['mapColumns'] = mapColumns
        self.sqoop_conf_parm['sqoopJobName'] = sqoopJobName
        self.sqoop_conf_parm['incrementalMode'] = incrementalMode
        self.sqoop_conf_parm['checkColumnName'] = checkColumnName
        self.sqoop_conf_parm['checkColLastValue'] = checkColLastValue
        self.sqoop_conf_parm['compressCodec'] = compressCodec
        self.sqoop_conf_parm['fieldDelimiter'] = fieldDelimiter
        self.sqoop_conf_parm['isView'] = isView
        self.sqoop_conf_parm['isQuery'] = isQuery
        self.sqoop_conf_parm['queryString'] = queryString
        self.sqoop_conf_parm['streaming'] = streaming
        self.sqoop_conf_parm['streamTolerance'] = streamTolerance
        self.sqoop_conf_parm['additionalArgs'] = additionalArgs

    def execute(self, context):
        bicommon = BICommon()
        self.profile = bicommon.get_profile()
        self.env_type = bicommon.env
        source_env = self.profile['DCBI1']
        if self.sourceEnv != None:
            source_env = self.sourceEnv
        sqoop_tools = SqoopTools(self.env_type)
        sqoop_prop = sqoop_tools.get_sqoop_v2_properties(self.sqoop_conf_parm, bicommon.db_conn_opt, source_env)
        logging.info('sqoop_prop >>>')
        logging.info(
            "=========================================================================================")
        for key in sqoop_prop:
            separator = " " * (25 - len(str(key))) + ": "
            logging.info(key + separator + str(sqoop_prop[key]))
        logging.info(
            "=========================================================================================")

        if (sqoop_prop['SQOOP_OP_DIR_FLAG'] == "S3"):
            batch_common.delete_s3_path(sqoop_prop['S3_TARGET_DIRECTORY'])
            batch_common.delete_s3_path(sqoop_prop['S3_TGT_FOLDER_FILE'])

        if (sqoop_prop['LOAD_TYPE'] == self.FULL_LOAD_TYPE):
            self.command = sqoop_prop['SQOOP_CMD']
            status = super(GenieSqoop_V2_operator, self).execute(context)
            if status != "SUCCEEDED":
                logging.info("Sqoop Job Failed")
                raise Exception("Job failed")
            else:
                logging.info("Sqoop Job Succeeded")

        if (sqoop_prop['LOAD_TYPE'] == self.INC_LOAD_TYPE):
            if sqoop_prop['STREAMING'] == 'YES':
                max_timestamp = sqoop_tools.get_max_timestamp(sqoop_prop)
                logging.info('get max_timestamp : ', max_timestamp)

            # Create Job if not exists.
            sqoop_job_exits = sqoop_tools.check_job_exists(sqoop_prop['SQOOP_JOB_NAME'])
            if sqoop_job_exits is False:
                self.command = sqoop_prop['SQOOP_CMD']
                status = super(GenieSqoop_V2_operator, self).execute(context)
                if status != "SUCCEEDED":
                    logging.info("Sqoop Job Creation failed")
                    raise AirflowException("Sqoop Job Creation failed")
                else:
                    logging.info("Sqoop Job Creation Succeeded")

                    # Execute Job
            sqoop_exec_cmd = 'job --exec ' + sqoop_prop['SQOOP_JOB_NAME']
            self.command = sqoop_exec_cmd
            status = super(GenieSqoop_V2_operator, self).execute(context)

            if status != "SUCCEEDED":
                logging.info("Sqoop Job Execution failed")
                raise Exception("Job failed")
            else:
                logging.info("Sqoop Job Succeeded")

            if sqoop_prop['STREAMING'] == 'YES' and sqoop_prop['LOAD_TYPE'] == "INC":
                sqoop_tools.update_metastore(sqoop_prop['SQOOP_JOB_NAME'], max_timestamp)


class GenieSnowflakeOperator(GenieOperator, SnowFlakeOperator):
    """
    Created By: Senthil Jeyakumar, Bharani Manavalan
    Description: Operator to Load snowflake table using Apache spark    
    """
    template_fields = ('job_name', 'job_type', 'sched_type', 'conn_id', 'parameters')
    ui_color = '#D8BFD8'

    @apply_defaults
    def __init__(self,
                 parameters,
                 job_name=None,
                 job_type='spark',
                 sched_type='GenieAdhoc',
                 version='default',
                 file_dependencies=None,
                 env=None,
                 conn_id='snowflake',
                 packages=None,
                 *args, **kwargs):
        super(GenieSnowflakeOperator, self).__init__(command='dummy_command',
                                                 job_name=job_name,
                                                 job_type=job_type,
                                                 sched_type=sched_type,
                                                 version=version,
                                                 file_dependencies=file_dependencies,
                                                 env=env,
                                                 sql_file='dummy.sql',
                                                 parameters=parameters,
                                                 packages=None,
                                                 *args, **kwargs)
        self.conn_id = conn_id
        self.snow_conn_id = conn_id
        self.emr_conn_id = 'emr_api_default'
        self.parameters = parameters
        self.hook = None
        self.s3_hook = None
        self.cur = None
        self.conn = None
        self.command = None
        self.command_json = None
        self.airflow_cluster_name = None
        self.env_type = None
        self.sfPresteps_sql='/app/bin/common/airflow_deploy/snowflakesql/geniesnowflakeoperator_presteps.sql'
        self.sfPoststeps_sql='/app/bin/common/airflow_deploy/snowflakesql/geniesnowflakeoperator_poststeps.sql'
        self.sfPostgrants_sql='/app/bin/common/airflow_deploy/snowflakesql/geniesnowflakeoperator_postgrants.sql'
        self.packages=packages

    def execute(self, context):
        """
        Main method for execution
        """
        # Validate the input parameters
        self.validate_input_params()

        # Identify the execution environment
        self.get_env()

        # Perform action based on the load type
        if self.parameters.get('load_type').lower() == 'incremental':
            # Snowflake Presteps execution
            self.submit_job_snowflake(self.sfPresteps_sql)
            logging.info("Snowflake pre-steps execution succeeded")

            # Calling the spark job to load into snowflake table
            self.submit_job_emr(context)

            # Snowflake Poststeps execution
            self.submit_job_snowflake(self.sfPoststeps_sql)
            self.apply_grants()
            logging.info("Snowflake post-steps execution succeeded")

        elif self.parameters.get('load_type').lower() == 'full':
            self.submit_job_emr(context)
            self.apply_grants()

        else:
            raise Exception("NOT a supported value for load_type: %s\n" % format(self.parameters.get('load_type')))

    def submit_job_snowflake(self, sql_file_path):
        """
        Function to establish snowflake connection
        and submit commands for execution
        """
        try:
            self.get_cursor()
            sql_file_path = str(sql_file_path).strip()
            self.snowflake_query_exec(self.cur, self.conn.schema, sql_file_path)
        except:
            self.cur.close()
            raise Exception("Snowflake step Failed, Job failed")
        finally:
            self.cur.close()

    def submit_job_emr(self, context):
        """
        Function to submit the spark job to EMR which loads the
        snowflake table
        """
        # Get snowflake connection details based on conn_id
        self.hook = SnowFlakeHook(conn_id=self.conn_id)
        self.conn = self.hook.get_conn()

        # Update the parameters for the spark job
        # to use the snowflake conn details
        import base64
        self.parameters.update({'account_name': self.conn.host,
                                'database': self.conn.schema,
                                'username': self.conn.login,
                                'password': base64.b64encode(self.conn.password),
                                'warehouse': self.conn.extra_dejson.get('warehouse', ''),
                                'role': self.conn.extra_dejson.get('role', '')})

        # Set spark job related configs if provided
        spark_configs = self.parameters.get('spark_configs', ' ')
        if self.packages:
            spark_packages=self.packages
        else:
            spark_packages = ' --packages net.snowflake:snowflake-jdbc:3.4.2,net.snowflake:spark-snowflake_2.11:2.2.8 '
        geniesnowflake_sparkjob = 's3://nike-emr-bin/' + self.env_type + '/common/scripts/geniesnowflake_sparkload.py '

        import json
        self.command_json = json.dumps(self.parameters)
        self.conn_id = self.emr_conn_id
        self.command = ' --master yarn --deploy-mode cluster ' + \
                        spark_configs + \
                        spark_packages + \
                        geniesnowflake_sparkjob + \
                        self.command_json
        super(GenieSnowflakeOperator, self).execute(context)
        self.conn_id = self.snow_conn_id

    def get_env(self):
        """
        Function to identify the execution environment details
        based on airflow configuration
        """
        self.airflow_cluster_name = conf.get('core', 'cluster')
        bicommon = BICommon()
        self.env_type = bicommon.env

        self.parameters.update({'airflow_cluster_name': self.airflow_cluster_name, 'env': self.env_type})

    def validate_input_params(self):
        """
        Function to validate the input parameters
        """
        if isinstance(self.parameters, dict):
            # Setup the mandatory params for snowflake load
            mandatory_keys = ('load_type', 'hive_database', 'hive_table', 'sfSchema', 'sfTable', 'sfGrantee_roles')
            if not all(key in self.parameters for key in mandatory_keys):
                logging.info("Mandatory keys for GenieSnowflakeOperator(parameters): %s\n" % format(mandatory_keys))
                logging.error("Mandatory key(s) NOT exists in GenieSnowflakeOperator(parameters): %s\n" % format(self.parameters))
                raise Exception("Job failed")

            # Setting up pre,post and grants scripts for snowflake
            self.sfPresteps_sql = self.parameters.get('sfPresteps_sql', self.sfPresteps_sql)
            self.sfPoststeps_sql = self.parameters.get('sfPoststeps_sql', self.sfPoststeps_sql)
            self.sfPostgrants_sql = self.parameters.get('sfPostgrants_sql', self.sfPostgrants_sql)
        else:
            logging.error("Input is NOT a dictionary: %s\n" % format(self.parameters))
            raise Exception("Job failed")

    def apply_grants(self):
        """
        Function to apply grants after successful execution
        """
        grantee_roles = self.parameters.get('sfGrantee_roles')
        for grantee_role in grantee_roles.split(','):
            self.parameters.update({'sfGrantee_roles': grantee_role})
            self.submit_job_snowflake(self.sfPostgrants_sql)


# Defining the plugin class
class AirflowGenieOpPlugin(AirflowPlugin):
    name = "genie_ops_plugin"
    operators = [GenieHiveOperator,
                 GeniePigOperator,
                 GenieHadoopOperator,
                 GenieS3DistCpOperator,
                 GenieSparkOperator,
                 CanaryOperator,
                 GenieSqoopOperator,
                 GenieSqoopBatchOperator,
                 GenieFullLoadOperator,
                 GenieHiveIncMergeOperator,
                 GenieSqoop_V2_operator,
                 GeniePigBatchOperator,
                 GenieSnowflakeOperator]

