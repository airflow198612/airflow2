# $ export AUTH_TOKEN=<copy_your_auth_token_here>  # export your Nginx Authorization Token
# $ python paas-sdk.py -a $AUTH_TOKEN
# import modules
from emr_client.models.payload import Payload
from emr_client.models.emr import Emr
from emr_client.apis.default_api import DefaultApi
from emr_client.api_client import ApiClient
from emr_client.rest import ApiException

import argparse
import time

apiclient = ApiClient(host="https://r0p4p5l207.execute-api.us-east-1.amazonaws.com/v2")  # Reference #1 in Usage
emr_api = DefaultApi(api_client=apiclient)


# CREATE cluster
def create_cluster(auth_token, cluster_name, project_id, cost_center):
    try:
    ## use this section to customize EMR configuration settings
        config = [{"Classification": "mapred-site",
                   "Properties": {"mapreduce.map.speculative": "true",
                                  "mapreduce.reduce.speculative": "true"}},
                  {"Classification": "hive-site",
                   "Properties": {"hive.mapred.reduce.tasks.speculative.execution": "true"}}]
    ## Variable settings - see notes above
        body = Payload(cluster_name=cluster_name,  # see main function below to set name
            num_task_nodes=4,  # must set to 0 if not used
            num_core_nodes=2,
            classification='bronze',
            task_inst_type='r3.4xlarge',  # optional if num_task_nodes are not set
            core_inst_type='m3.xlarge',
            project_id=project_id,  # Optional - see main function below to set #optional
            cost_center=cost_center,  # Optional - see main function below to set #optional
            emr_configurations=config,  # optional can just take default settings
            emr_version='emr-5.3.0',  # supported 5.0.0 up to version 5.3.1
            applications=["presto", "zeppelin"],  # could also specify ganglia
            group='GCKengineering')  # optional, specify if you are in multiple NGAP2 groups
        response = emr_api.create_cluster(auth_token, body)
        print response
    except ApiException as e:
        print "Exception when calling DefaultApi->create_cluster: %s\n" % e

    # Now wait for the cluster to start before exiting
    while True:
        time.sleep(30)

        try:
            response = emr_api.describe_cluster(cluster_name, auth_token)
            status = response.cluster_status
            if status == 'READY':
                print("Cluster: %s has been successfully started and is ready for use" % cluster_name)
                print response
                break
            else:
                print("Waiting on cluster to start, current status: %s" % status)
        except ApiException as e:
            print "Exception when calling describe cluster: %s\n" % e
            raise RuntimeError(e)


# DESCRIBE cluster
def describe_cluster(cluster_name, auth_token):
    try:
        response = emr_api.describe_cluster(cluster_name, auth_token)
        print response
    except ApiException as e:
        print "Exception when calling describe cluster: %s\n" % e


# LIST clusters
def list_clusters(auth_token):
    try:
        response = emr_api.list_clusters(auth_token)
        print response
    except ApiException as e:
        print "Exception when calling list clusters: %s\n" % e

# RESIZE cluster
def resize_cluster(cluster_name, auth_token):
    try:
        response = emr_api.resize_cluster(cluster_name, auth_token,Emr(core_inst_type=1,task_inst_type=1))
        print response
    except ApiException as e:
        print "Exception when calling resize cluster: %s\n" % e

# TERMINATE cluster
def terminate_cluster(cluster_name, auth_token):
    try:
        response = emr_api.terminate_cluster(cluster_name, auth_token)
        print response
    except ApiException as e:
        print "Exception when calling terminate cluster: %s\n" % e


def main():
    # PASS IN YOUR AUTH_TOKEN
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--auth_token', required=True)
    args = parser.parse_args()
    my_auth_token = args.auth_token

    # MAKE YOUR CHANGES HERE
    my_cluster_name = 'platform-ngap2-r103-testing'   #  do not use underscores
    my_project_id = 'project_id'    #  Optional, will take a default based on your group
    my_cost_center = 'cost_center'  # Optional, will take a default based on your group

    try:
        # COMMENT OUT AS NEEDED - RUN ONLY ONE FUNCTION AT A TIME
        create_cluster(my_auth_token, my_cluster_name, my_project_id, my_cost_center)
        # describe_cluster(my_cluster_name, my_auth_token)
        # list_clusters(my_auth_token)
        # terminate_cluster(my_cluster_name, my_auth_token)
    except ValueError, e:
        raise ValueError(e)


if __name__ == "__main__":
    main()