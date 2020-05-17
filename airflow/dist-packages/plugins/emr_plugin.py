# airflow
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from airflow.hooks.base_hook import BaseHook
from airflow.configuration import conf
from airflow.models import Variable
from batch_common import DMError, BICommon
# paas SDK
from emr_client.models.payload import Payload
from emr_client.rest import ApiException
# hooks
from hooks.paas_hooks import EmrApiHook

import logging
import time
from random import randint
import subprocess
#profile = BICommon().get_profile()
#ENV_TAG = profile['ENV']

class EmrOperator(BaseOperator):
    """
    Spins up or terminates an AWS EMR cluster, including Genie registration.
    """
    template_fields = tuple()
    ui_color = '#7A35E8'
    @apply_defaults
    def __init__(
            self,
            cluster_action,
            cluster_name,
            custom_dns=False,
            num_core_nodes=1,
            num_task_nodes=0,
            core_bid_type='ON_DEMAND',
            task_bid_type='SPOT',
            conn_id="emr_api_default",
            master_inst_type=None,
            core_inst_type=None,
            task_inst_type=None,
            emr_version=None,
            applications=None,
            classification='bronze',
            project_id=None,
            properties=[],
            tags=[],
            bootstrap_actions=[],
            cost_center=None,
            long_running_cluster=False,
            auto_scaling=False,
            core_max=None,
            core_min=None,
            task_max=None,
            task_min=None,
            core_scale_up=None,
            core_scale_down=None,
            task_scale_up=None,
            task_scale_down=None,
            is_instance_fleet=False,
            instance_fleets=None,
            cerebro_cdas=False,
            cerebro_hms=False,
            group=None,
            *args, **kwargs):

        super(EmrOperator, self).__init__(*args, **kwargs)


        self.conn_id = conn_id
        self.cluster_action = cluster_action.lower()
        self.custom_dns = custom_dns
        self.cluster_name = cluster_name
        if self.cluster_action != 'spinup' and self.cluster_action != 'terminate':
            raise ValueError(
                "Invalid cluster_action '{0}', must be either 'spinup' or 'terminate'".format(cluster_action))
        self.num_core_nodes = num_core_nodes
        self.num_task_nodes = num_task_nodes
        self.core_bid_type = core_bid_type.upper()
        self.task_bid_type = task_bid_type.upper()
        self.master_inst_type = master_inst_type
        self.core_inst_type = core_inst_type
        if self.master_inst_type is None:
            self.master_inst_type = 'default'
        if self.core_inst_type is None:
            self.core_inst_type = 'default'
        self.task_inst_type = task_inst_type
        if self.task_inst_type is None:
            self.task_inst_type = 'default'
        if emr_version is None:
            self.emr_version = conf.get('emr', 'default_emr_version')
        else:
            self.emr_version = 'emr-' + emr_version
        self.applications = applications
        self.classification = classification
        self.project_id = project_id
        self.properties = properties
        self.long_running_cluster = long_running_cluster
        self.bootstrap_actions = bootstrap_actions
        self.cost_center = cost_center
        if self.cost_center is None:
            self.cost_center = conf.get('emr', 'tag_costcenter')
        self.auto_scaling = auto_scaling
        self.core_max = core_max
        self.core_min = core_min
        self.task_max = task_max
        self.task_min = task_min
        self.tags = tags
        self.group = group
        self.core_scale_up = core_scale_up
        self.core_scale_down = core_scale_down
        self.task_scale_up = task_scale_up
        self.task_scale_down = task_scale_down
        self.emr_client = None
        self.is_instance_fleet = is_instance_fleet
        self.instance_fleets = instance_fleets
        self.cerebro_cdas = cerebro_cdas
        self.cerebro_hms = cerebro_hms

    def execute(self, context):
        self.emr_client = self.make_emr_client()

        bicommon = BICommon()
        profile = bicommon.get_profile()
        ENV_TAG = profile['ENV']

        # Set Environment Specific Tag
        self.tags.append({"Key": "environment", "Value": ENV_TAG.lower()})

        if self.cerebro_cdas == True:
            cerebro_tag = False
            if len(self.tags) != 0:
                for a in self.tags:
                # Retain tag passed in the EMROperator
                    if a["Key"] == "cerebro_cluster":
                        cerebro_env = a["Value"]
                        logging.info("Cerebro Environment Provided:  ({0})".format(cerebro_env))
                        cerebro_tag = True
                        break
                    else:
                        logging.info("EMR Tags don't include Cerebro Environment")
            if cerebro_tag == False:
                        try:
                            # Set Airflow Cluster Specific Cerebro Cluster
                            cerebro_env = Variable.get("cerebro_cluster")
                            logging.info("Cerebro Environment Provided:  ({0})".format(cerebro_env))
                            self.tags.append({"Key": "cerebro_cluster", "Value": cerebro_env})
                        except:
                            # Set Default Environment Cerebro Cluster
                            cerebro_env = ENV_TAG
                            logging.info("Cerebro Environment Provided:  ({0})".format(cerebro_env))
                            self.tags.append({"Key": "cerebro_cluster", "Value": cerebro_env})
                # Remove dups in the dictionary
        self.tags = [dict(x) for x in set(tuple(item.items()) for item in self.tags)]
        logging.info("Environment Tags Provided :  ({0})".format(self.tags))


        if self.applications is not None and 'sqoop' in self.applications:
            process = subprocess.Popen(['airflow', 'list_tasks', context['dag'].dag_id, '--tree'],
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            output = {}
            if process.stdout is not None:
                output["stdout"] = ""
                for line in process.stdout.readlines():
                    if 'Task(GenieSqoop_V2_operator)' in str(line) or 'Task(GenieSqoopBatchOperator)' in str(
                            line) or 'Task(GenieSqoopOperator)' in str(line):
                        output["stdout"] += str(line)
                if len(output["stdout"]) == 0:
                    raise ValueError(
                        "Sqoop operators are not used in DAG but sqoop is specified in EMR applications,Please remove sqoop from applications")
        if self.cluster_action == 'spinup':
            self.__spinup(context)
        else:
            self.__terminate(context)

    def make_emr_client(self):
        connection = BaseHook.get_connection(self.conn_id)
        print connection.host
        return EmrApiHook(connection)

    def __spinup(self, context):
        """
        Requests Paas  to spinup the EMR cluster, then waits for spinup completion.
        """
        logging.info("EMR SPINUP (cluster_name {0})".format(self.cluster_name))
        api_call_success = 0
        try:

            payload = Payload(cluster_name=self.cluster_name, custom_dns_status=self.custom_dns, num_task_nodes=self.num_task_nodes,
                              num_core_nodes=self.num_core_nodes, classification=self.classification,
                              core_inst_type=self.core_inst_type, task_inst_type=self.task_inst_type,
                              master_inst_type=self.master_inst_type,
                              task_bid_type=self.task_bid_type, project_id=self.project_id,
                              cost_center=self.cost_center, emr_version=self.emr_version,
                              bootstrapactions=self.bootstrap_actions, long_running_cluster=self.long_running_cluster,
                              emr_configurations=self.properties, applications=self.applications,
                              auto_scaling=self.auto_scaling, core_max=self.core_max, core_min=self.core_min,
                              task_max=self.task_max, task_min=self.task_min, core_scale_up=self.core_scale_up,
                              core_scale_down=self.core_scale_down, task_scale_up=self.task_scale_up,
                              task_scale_down=self.task_scale_down, tags=self.tags,
                              is_instance_fleet=self.is_instance_fleet, instance_fleets=self.instance_fleets,
                              cerebro_cdas=self.cerebro_cdas, cerebro_hms=self.cerebro_hms, group=self.group)

            self.emr_client.create_cluster(payload)

        except ApiException as exp:
            print exp
            if exp.reason == 'Gateway Timeout':
                time.sleep(30)
                try:
                    response = self.emr_client.describe_cluster(self.cluster_name)
                    if response._cluster_status == 'STARTING':
                        logging.info("Spinup Initiated for EMR cluster:" + self.cluster_name)
                    else:
                        logging.info(
                            "encounter exception when submit AWS request to create cluster, Please check logs....")
                        raise ValueError("encounter exception when submit AWS request to create cluster.")
                except:
                    logging.info("encounter exception when submit AWS request to create cluster, Please check logs....")
                    raise ValueError("encounter exception when submit AWS request to create cluster.")
            elif exp.reason == 'Bad Request' and exp.body is not None and 'already exists in active state' in exp.body:
                logging.info("EMR with given name already exists,checking for existing emr status...")
                pass

            else:
                logging.info("encounter exception when submit AWS request to create cluster, Please check logs....")
                raise ValueError("encounter exception when submit AWS request to create cluster." + str(exp))

        #Below code block determines the required limit i.e wait time for emr spinup completion and would be passed as parameter to
        # __wait_for_spinup(self, cluster_name,limit) and __wait_for_spinup_instancefleet(self, cluster_name,limit) methods.

        datascience_cluster_match = 'dstools'

        #For datascience cluster, value from airflow variable named "datascientists" i.e "True" is assigned to datascience_flag

        try :
            datascience_flag = Variable.get("datascientists")
        except ValueError :
            datascience_flag = 'False'


        #This condition determines if the cluster is a persistent datascience cluster
        if datascience_cluster_match in self.cluster_name and (datascience_flag == 'True'):
            is_datascience_flag = True
        else:
            is_datascience_flag = False

        #limit i.e wait time is set in the below code block based on the above flags
        if is_datascience_flag and self.is_instance_fleet:
                        limit = ((self.spot_timeout_duration_minutes() * 60) + (75 * 60) + (15 * 60))
                        self.__wait_for_spinup_instancefleet(self.cluster_name, limit)
        elif is_datascience_flag:
                        limit = (75 * 60)
                        self.__wait_for_spinup(self.cluster_name, limit)
        elif self.is_instance_fleet:
                        limit = ((self.spot_timeout_duration_minutes() * 60) + (30 * 60))
                        self.__wait_for_spinup_instancefleet(self.cluster_name, limit)
        else:
                        limit = 20
                        self.__wait_for_spinup(self.cluster_name, limit)

    def spot_timeout_duration_minutes(self):
        """
        The method is used the calculate the maximum of timeout_duration_minutes for spot instancefleet
        :return: max(timeout_duration_minutes)
        """
        timeout_duration_minutes_list = []
        for instance_fleet in self.instance_fleets:
            if instance_fleet.launch_specifications is not None:
                time = instance_fleet.launch_specifications.spot_specification.timeout_duration_minutes
                timeout_duration_minutes_list.append(time)
            else:
                time = 0
                timeout_duration_minutes_list.append(time)
        return max(timeout_duration_minutes_list)


    def __wait_for_spinup(self, cluster_name,limit):
        success = False
        error = False
        counter = 0
            # try to poll for the status of the cluster for 10 times by default and till limit for datascience cluster
            # this will cause the wait time to be cumulative of 2 to power of 1 to 10, close to 34 minutes for default scenario
        while not success and counter < limit:
            try:
                response = self.emr_client.describe_cluster(cluster_name)
                if response._cluster_status == 'READY':
                        success = True
                        break
                logging.info(
                    "Spinup Initiated for EMR cluster: " + cluster_name + " and its ClusterId is:" + response._cluster_id)
                if counter == 0:
                    # wait for 7 minutes before polling for the cluster status. This is for on-demand
                    time.sleep(420)

                if response._cluster_status in ['TERMINATING', 'TERMINATED', 'FAILED']:
                    success = True
                    error = True
                    break
                logging.info("Status of EMR: " + response._cluster_status)

            except Exception as excp:
                logging.info("Getting exception while getting status of EMR: " + cluster_name + " " + str(excp))
                logging.info("Retrying to get the status ")

            if counter > 19:
                # Use this counter for additional wait time kicks in for datascience clusters
                wait_time = randint(130, 140)
                time.sleep(wait_time)
            else:
                # wait_time will be 2 mins after the initial check of 420s for on-demand clusters
                counter += 1
                wait_time = randint(120, 130)
                time.sleep(wait_time)

        if not success:
            logging.info("Tried 10 times already to check if the cluster is up and it's not")
            # terminate the cluster
            self.emr_client.terminate_cluster(cluster_name)
            raise ValueError("Error while getting status of EMR '{0}'".format(self.cluster_name))

        if error:
            logging.info("Cluster cannot be spinned up")
            raise ValueError("Problem spinning up cluster '{0}'".format(self.cluster_name))
        logging.info(
            "Spinup Completed for EMR cluster: " + cluster_name + " and its ClusterId is:" + response._cluster_id)



    def __wait_for_spinup_instancefleet(self, cluster_name,limit):
        """
        This method has logic for extra wait time for spinning up spot instance fleet for datascience and other clusters
        :param cluster_name:
        :return:
        """
        success = False
        error = False
        counter = 0
        total_wait_time = 0

        while not success and total_wait_time < limit:
            try:
                response = self.emr_client.describe_cluster(cluster_name)
                if response._cluster_status == 'READY':
                    success = True
                    break
                logging.info(
                    "Spinup Initiated for EMR cluster: " + cluster_name + " and its ClusterId is:" + response._cluster_id)
                if counter == 0:
                    # wait for 15 minutes before polling for the cluster status.
                    time.sleep(900)
                if response._cluster_status in ['TERMINATING', 'TERMINATED', 'FAILED']:
                    success = True
                    error = True
                    break
                logging.info("Status of EMR: " + response._cluster_status)
            except Exception as excp:
                    logging.info("Getting exception while getting status of EMR: " + cluster_name + " " + str(excp))
                    logging.info("Retrying to get the status ")

            #removing the wait logic recommended by amazon
            if counter > 7:
                # when the counter is 8 ie, 15 minutes + 14 minutes approx has passed already
                # Use this counter for additional wait time until the limit ie, 1 hour approx based on spot duration
                counter += 1
                wait_time = randint(124, 128)
            else:
                #breaking the logic here for the counter to wait 2 minutes after initial polling of 15 minutes
                counter += 1
                wait_time = randint(120, 124)
            total_wait_time += wait_time
            time.sleep(wait_time)

        if not success:
            logging.info("Tried 10 times already to check if the cluster is up and it's not")
            # terminate the cluster
            self.emr_client.terminate_cluster(cluster_name)
            raise ValueError("Error while getting status of EMR '{0}'".format(self.cluster_name))

        if error:
            logging.info("Cluster cannot be spinned up")
            raise ValueError("Problem spinning up cluster '{0}'".format(self.cluster_name))
        logging.info(
            "Spinup Completed for EMR cluster: " + cluster_name + " and its ClusterId is:" + response._cluster_id)

    def on_failure(self, context):
        logging.error('DAG error requires premature EMR termination')

    def __terminate(self, context):
        """
        Requests Paas  to terminate the EMR cluster.
        """
        self.emr_client.terminate_cluster(self.cluster_name)
        logging.info("EMR TERMINATED (cluster_name {0})".format(self.cluster_name))




class EmrPlugin(AirflowPlugin):
    name = "emr_plugin_v2"
    operators = [EmrOperator]
    flask_blueprints = []
    hooks = []
    executors = []
    admin_views = []
    menu_links = []
