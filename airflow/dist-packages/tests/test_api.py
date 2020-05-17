from emr_client.models.payload import Payload
from emr_client.models.emr import Emr
from emr_client.apis.default_api import DefaultApi
from emr_client.api_client import ApiClient

apiclient = ApiClient(host="https://pwdiktgsw8.execute-api.us-east-1.amazonaws.com/cerebro")
emr_api = DefaultApi(api_client=apiclient)
authorization = ""

cluster_name = 'cerebro_cdas_test'


def test_emr_create_cluster():
    """
    Test case for emr_post
    """
    body = Payload(cluster_name=cluster_name,  # Reference #2 in Usage - see main function below to set
                   custom_dns=True,  
                   num_task_nodes=1,  # Reference #3 in Usage - must set to 0 if not used
                   num_core_nodes=1,
                   classification='bronze',
                   task_inst_type='m3.xlarge',  # Reference #3 in Usage - optional if num_task_nodes are not set
                   core_inst_type='m3.xlarge',
                   master_inst_type='m3.xlarge',
                   group="alpha",
                   emr_version='emr-5.3.0',  # Reference #6 in Usage - supported up to version 5.3.1
                   applications=["presto", "zeppelin"],  # Reference #7 in Usage - could also specify ganglia
                   #emr_configurations=[{"Classification": "hive-env", "Properties": {}, "Configurations": [{"Classification": "export", "Properties": {"HADOOP_HEAPSIZE": "6144"}, "Configurations": []}]}],
                   auto_scaling=True,  # Reference #10 - optional
                   core_max=5,  # Reference #10a - optional,required only if autoscaling is true
                   core_min=1,  # Reference 10b - optional,required only if autoscaling is true
                   task_max=5,  # Reference #10c - optional,required only if autoscaling is true
                   task_min=1,  # Reference #10d - optional,required only if autoscaling is true
                   core_scale_up=2,  # Reference #10e - optional, default 1
                   core_scale_down=-1,  # Reference #10f - optional,default -1
                   task_scale_up=2,  # Reference #10g - optional,default 2
                   task_scale_down=-1,  # Reference #10h - optional, default -2
                   cerebro_cdas=True,
                   cerebro_hms=False)

    print emr_api.create_cluster(authorization, body)


def test_list_cluster():
    print emr_api.list_clusters(authorization)


def test_describe_cluster():
    print emr_api.describe_cluster(cluster_name, authorization)


def test_terminate_cluster():
    print emr_api.terminate_cluster(cluster_name, authorization)


def test_resize_cluster():
    print emr_api.resize_cluster(authorization,
                                 cluster_name,
                                 Emr(num_task_nodes=1, num_core_nodes=0))


if __name__ == '__main__':
      test_emr_create_cluster()
    #  test_list_cluster()
    #  test_describe_cluster()
    #  test_terminate_cluster()
    #  test_resize_cluster()
