from emr_client.api_client import ApiClient
from emr_client.apis.default_api import DefaultApi


class EmrApiHook(object):
    def __init__(self,conn):
        self.conn = conn
        self.auth = conn.extra_dejson.get('auth_token')
        self.host = conn.host
        emr_client = ApiClient(host=self.host)
        self.emr_api = DefaultApi(api_client=emr_client)

    def create_cluster(self, payload):
        api_response = self.emr_api.create_cluster(self.auth, payload)
        return api_response

    def terminate_cluster(self, cluster_name):
        api_response = self.emr_api.terminate_cluster(cluster_name, self.auth)
        return api_response

    def describe_cluster(self, cluster_name):
        api_response = self.emr_api.describe_cluster(cluster_name, self.auth)
        return api_response

