#global imports
import pytest
import vcr
import time
import requests
import json
import sys
from urlparse import urljoin

#local imports
from constants import BASE_URL
import emr_client
from emr_client.models.payload import Payload
from emr_client.models.emr import Emr
from emr_client.apis.default_api import DefaultApi
from emr_client.api_client import ApiClient



timestamp = int(time.time())
#when cluster name is dynamic, tests can't be rerun with existing cassettes since URL is changed!!
cluster_name='test_cluster.{}'.format(timestamp)
#cluster_name='test_cluster_int'


V2_TARGET_URL = urljoin(BASE_URL, 'v2/emr')
V3_TARGET_URL = urljoin(BASE_URL, 'v3/emr')
V2_CLUSTER_URL = urljoin(V2_TARGET_URL, 'emr/{}'.format(cluster_name))
V3_CLUSTER_URL = urljoin(V3_TARGET_URL, 'emr/{}'.format(cluster_name))
V2_API_URL = urljoin(BASE_URL, 'v2')
V3_API_URL = urljoin(BASE_URL, 'v3')


authorization = pytest.config.getoption("--auth")
headers = {"Accept": "application/json", "Authorization": "{auth}".format(auth=authorization), "Content-Type": "application/json", "User-Agent": "Swagger-Codegen/0.3.2/python"}


apiclient = ApiClient(host='{url}'.format(url=V2_API_URL))
emr_api = DefaultApi(api_client=apiclient)




#Tests below
##########################################################################################################
#
#validate json response to request with NO auth token.
@vcr.use_cassette('fixtures/vcr_cassettes/bad_get.yml', filter_headers=['authorization'], record_mode=all)
def test_bad_request_body():
        response = requests.get(V3_API_URL)
        json = {"message":"Missing Authentication Token"}
        assert response.json() == json
#       
# 
##########################################################################################################
#
#Validate http status code for request with NO auth token.
@vcr.use_cassette('fixtures/vcr_cassettes/bad_get.yml', record_mode=all)
def test_bad_request_code():
        response = requests.get(V3_API_URL)
        assert response.status_code == 403
#                
#
##########################################################################################################
#
#Validate http status code for malformed auth token.
@vcr.use_cassette('fixtures/vcr_cassettes/malformed_auth.yml', record_mode=all)
def test_malformed_auth():
 
    payload = {}
    json = {"errorMessage" : 'Unable to verify Token'}
    mheaders = {"Accept": "application/json", "Authorization": "foo".format(auth=authorization), "Content-Type": "application/json", "User-Agent": "Swagger-Codegen/0.3.2/python"}
    response = requests.post(V3_TARGET_URL, headers=mheaders, json=payload)
    print response.status_code
    assert response.status_code == 401
    #assert_equal(response.json(), json)
#
#    
##########################################################################################################
#
#Create cluster and validate response.
@vcr.use_cassette('fixtures/vcr_cassettes/create_v3cluster.yml', filter_headers=['authorization'], record_mode=all)
def test_create_cluster():
 
    payload = {"auto_scaling": False, "num_task_nodes": 1, "num_core_nodes":
      1, "classification": "bronze", "core_inst_type": "m3.xlarge", "core_min": 1,
      "cluster_name": "{}".format(cluster_name), "applications": ["presto", "zeppelin"],
      "core_max": 2, "task_min": 1, "task_max": 2, "task_scale_up": 1, "emr_version":
      "emr-5.3.0", "core_scale_up": 1, "task_scale_down": -1, "core_scale_down": -1,
      "task_inst_type": "m3.xlarge"}
    json = {}
    response = requests.post(V3_TARGET_URL, headers=headers, json=payload)
    print response.status_code
    #In v2 status code is 202!!
    assert response.status_code == 200
    assert response.json() == json
#
#
##########################################################################################################
#
#List clusters and validate response.
@vcr.use_cassette('fixtures/vcr_cassettes/list_clusters.yml', filter_headers=['authorization'], record_mode=all)
def test_list_clusters():
    response = requests.get(V3_TARGET_URL, headers=headers)
    assert response.status_code == 200
#
#
##########################################################################################################
#
#Show cluster details and validate response. TODO: validate response JSON with regex matching for unique values.
@vcr.use_cassette('fixtures/vcr_cassettes/describe_cluster.yml', filter_headers=['authorization'], record_mode=all)
def test_describe_cluster():
    response = requests.get(V3_CLUSTER_URL, headers=headers)
    assert response.status_code == 200
#    
#    
##########################################################################################################
#
#Delete created cluster and validate response
@vcr.use_cassette('fixtures/vcr_cassettes/terminate_cluster.yml', filter_headers=['authorization'], record_mode=all)    
def test_terminate_cluster():
    response = requests.delete(V3_CLUSTER_URL, headers=headers)
    assert response.status_code == 200
#     
#  
##########################################################################################################
#
#Attempt delete nonexisting cluster and validate response
@vcr.use_cassette('fixtures/vcr_cassettes/terminate_invalid_cluster.yml', filter_headers=['authorization'], record_mode=all)    
def test_terminate_invalid_cluster():
    response = requests.delete(V3_CLUSTER_URL, headers=headers)
    assert response.status_code == 404    
#
#
##########################################################################################################
#
#Create another cluster using constructed api call
@vcr.use_cassette('fixtures/vcr_cassettes/create_cluster_via_api.yml', filter_headers=['authorization'], record_mode=all)
def test_create_cluster_via_api():

    body = Payload(cluster_name="{}".format(cluster_name),# Reference #2 in Usage - see main function below to set
                       num_task_nodes=1,  # Reference #3 in Usage - must set to 0 if not used
                       num_core_nodes=1,
                       classification='bronze',
                       task_inst_type='m3.2xlarge',  # Reference #3 in Usage - optional if num_task_nodes are not set
                       core_inst_type='m3.2xlarge',
                       master_inst_type='r3.2xlarge',
                       group="alpha",
                       emr_version='emr-5.3.0',  # Reference #6 in Usage - supported up to version 5.3.1
                       applications=["presto", "zeppelin"],  # Reference #7 in Usage - could also specify ganglia
                       emr_configurations=[{"Classification":"hive-env","Properties":{},"Configurations":[{"Classification":"export","Properties":{"HADOOP_HEAPSIZE":"6144"},"Configurations":[]}]}],
                       auto_scaling=True, # Reference #10 - optional
                       core_max=5, # Reference #10a - optional,required only if autoscaling is true
                       core_min=1, # Reference 10b - optional,required only if autoscaling is true
                       task_max=5, # Reference #10c - optional,required only if autoscaling is true
                       task_min=1, # Reference #10d - optional,required only if autoscaling is true
                       core_scale_up=2, # Reference #10e - optional, default 1
                       core_scale_down=-1, # Reference #10f - optional,default -1
                       task_scale_up=2, # Reference #10g - optional,default 2
                       task_scale_down=-1) # Reference #10h - optional, default -2
    
    emr_api.create_cluster(authorization,body)
    #assert response.status_code == 200
#
#
##########################################################################################################
#
#Show cluster details and validate response.
@vcr.use_cassette('fixtures/vcr_cassettes/describe_api_cluster.yml', filter_headers=['authorization'], record_mode=all)
def test_describe_api_cluster():
    response = requests.get(V3_CLUSTER_URL, headers=headers)
    assert response.status_code == 200
    assert response.json()["cluster_name"] == cluster_name
#    
#    
##########################################################################################################
#
#Show cluster details via api. 
@vcr.use_cassette('fixtures/vcr_cassettes/api_describe_cluster.yml', filter_headers=['authorization'], record_mode=all)
def test_api_describe_cluster():
    emr_api.describe_cluster(cluster_name,authorization)
#    
#    
##########################################################################################################
#
#List clusters via api.
@vcr.use_cassette('fixtures/vcr_cassettes/api_list_clusters.yml', filter_headers=['authorization'], record_mode=all)
def test_api_list_clusters():
    emr_api.list_clusters(authorization)
#    
#    
##########################################################################################################
#    
#    
#Positive case for cluster resize via http, v3 only.
@vcr.use_cassette('fixtures/vcr_cassettes/resize_cluster.yml', filter_headers=['authorization'], record_mode=all)
def test_resize_clusters():
    text = u' {\n  "errorMessage" : "server error - Resize operation is accepted only when the cluster status is READY."\n }'
    payload = {"num_task_nodes": 1, "num_core_nodes": 1}
    response = requests.put(V3_CLUSTER_URL, headers=headers, json=payload)
    assert response.text == text
#
#
##########################################################################################################
#
#Delete api_created cluster and validate response
@vcr.use_cassette('fixtures/vcr_cassettes/terminate_api_cluster.yml', filter_headers=['authorization'], record_mode=all)    
def test_terminate_api_cluster():
    emr_api.terminate_cluster(cluster_name,authorization)
#     
#  
##########################################################################################################
#
#Negative case for cluster resize via http, v3 only
@vcr.use_cassette('fixtures/vcr_cassettes/resize_nonexistant_cluster.yml', filter_headers=['authorization'], record_mode=all)
def test_resize_nonexistant_clusters():
    text = u' {\n  "errorMessage" : "Cannot find an active cluster with Name: %s"\n }' % (cluster_name)
    payload = {"num_task_nodes": 1, "num_core_nodes": 1}
    response = requests.put(V3_CLUSTER_URL, headers=headers, json=payload)
    assert response.text == text
#     
#  
##########################################################################################################
#
#Create cluster using v2 URL
@vcr.use_cassette('fixtures/vcr_cassettes/create_v2cluster.yml', filter_headers=['authorization'], record_mode=all)
def test_create_v2_cluster():
 
    payload = {"auto_scaling": False, "num_task_nodes": 1, "num_core_nodes":
      1, "classification": "bronze", "core_inst_type": "m3.xlarge", "core_min": 1,
      "cluster_name": "{}".format(cluster_name), "applications": ["presto", "zeppelin"],
      "core_max": 2, "task_min": 1, "task_max": 2, "task_scale_up": 1, "emr_version":
      "emr-5.3.0", "core_scale_up": 1, "task_scale_down": -1, "core_scale_down": -1,
      "task_inst_type": "m3.xlarge"}
    json = {}
    response = requests.post(V2_TARGET_URL, headers=headers, json=payload)
    print response.status_code
    assert response.status_code == 202
    assert response.json() == json
#
#
##########################################################################################################
#
#Delete created V2 cluster and validate response
@vcr.use_cassette('fixtures/vcr_cassettes/terminate_v2cluster.yml', filter_headers=['authorization'], record_mode=all)    
def test_terminate_v2_cluster():
    response = requests.delete(V3_CLUSTER_URL, headers=headers)
    assert response.status_code == 200
#     
#  
##########################################################################################################