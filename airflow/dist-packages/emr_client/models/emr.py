# coding: utf-8

"""
    EMR REST API - CEREBRO

    Used to spin up,describe,resize,list,terminate EMR

    OpenAPI spec version: 0.3.8
    Contact: Lst-DigitalTech.NGAP.Developers@nike.com
    Generated by: https://github.com/swagger-api/swagger-codegen.git

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
"""

from pprint import pformat
from six import iteritems
import re


class Emr(object):
    """
    NOTE: This class is auto generated by the swagger code generator program.
    Do not edit the class manually.
    """
    def __init__(self, cluster_id=None, custom_dns=None,cluster_status=None, dns_sync_status=None, cluster_name=None, created_by=None, owned_by=None, created_at=None, master_inst_type=None, num_core_nodes=None, core_inst_type=None, num_task_nodes=None, task_inst_type=None, resize_by=None, resize_at=None, error_messages=None, warnings=None, emr_applications=None, cerebro=None, version=None, environment=None, master_ip=None, applications=None):
        """
        Emr - a model defined in Swagger

        :param dict swaggerTypes: The key is attribute name
                                  and the value is attribute type.
        :param dict attributeMap: The key is attribute name
                                  and the value is json key in definition.
        """
        self.swagger_types = {
            'cluster_id': 'str',
            'custom_dns':'str',
            'cluster_status': 'str',
            'dns_sync_status': 'str',
            'cluster_name': 'str',
            'created_by': 'str',
            'owned_by': 'str',
            'created_at': 'str',
            'master_inst_type': 'str',
            'num_core_nodes': 'str',
            'core_inst_type': 'str',
            'num_task_nodes': 'str',
            'task_inst_type': 'str',
            'resize_by': 'str',
            'resize_at': 'str',
            'error_messages': 'str',
            'warnings': 'list[str]',
            'emr_applications': 'str',
            'cerebro': 'str',
            'version': 'str',
            'environment': 'str',
            'master_ip': 'str',
            'applications': 'EmrApplications'
        }

        self.attribute_map = {
            'cluster_id': 'cluster_id',
            'custom_dns': 'custom_dns',
            'cluster_status': 'cluster_status',
            'dns_sync_status': 'dns_sync_status',
            'cluster_name': 'cluster_name',
            'created_by': 'created_by',
            'owned_by': 'owned_by',
            'created_at': 'created_at',
            'master_inst_type': 'master_inst_type',
            'num_core_nodes': 'num_core_nodes',
            'core_inst_type': 'core_inst_type',
            'num_task_nodes': 'num_task_nodes',
            'task_inst_type': 'task_inst_type',
            'resize_by': 'resize_by',
            'resize_at': 'resize_at',
            'error_messages': 'error_messages',
            'warnings': 'warnings',
            'emr_applications': 'emr_applications',
            'cerebro': 'cerebro',
            'version': 'version',
            'environment': 'environment',
            'master_ip': 'master_ip',
            'applications': 'applications'
        }

        self._cluster_id = cluster_id
        self._custom_dns = custom_dns
        self._cluster_status = cluster_status
        self._dns_sync_status = dns_sync_status
        self._cluster_name = cluster_name
        self._created_by = created_by
        self._owned_by = owned_by
        self._created_at = created_at
        self._master_inst_type = master_inst_type
        self._num_core_nodes = num_core_nodes
        self._core_inst_type = core_inst_type
        self._num_task_nodes = num_task_nodes
        self._task_inst_type = task_inst_type
        self._resize_by = resize_by
        self._resize_at = resize_at
        self._error_messages = error_messages
        self._warnings = warnings
        self._emr_applications = emr_applications
        self._cerebro = cerebro
        self._version = version
        self._environment = environment
        self._master_ip = master_ip
        self._applications = applications

    @property
    def cluster_id(self):
        """
        Gets the cluster_id of this Emr.
        ClusterId associated to cluster name

        :return: The cluster_id of this Emr.
        :rtype: str
        """
        return self._cluster_id

    @cluster_id.setter
    def cluster_id(self, cluster_id):
        """
        Sets the cluster_id of this Emr.
        ClusterId associated to cluster name

        :param cluster_id: The cluster_id of this Emr.
        :type: str
        """

        self._cluster_id = cluster_id

    @property
    def custom_dns(self):
        """
        Gets the custom_dns of this Emr.
        ClusterId associated to cluster name

        :return: The custom_dns of this Emr.
        :rtype: str
        """
        return self._custom_dns

    @custom_dns.setter
    def custom_dns(self, custom_dns):
        """
        Sets the custom_dns of this Emr.
        ClusterId associated to cluster name

        :param custom_dns: The custom_dns of this Emr.
        :type: str
        """

        self._custom_dns = custom_dns

    @property
    def cluster_status(self):
        """
        Gets the cluster_status of this Emr.
        status of the EMR

        :return: The cluster_status of this Emr.
        :rtype: str
        """
        return self._cluster_status

    @cluster_status.setter
    def cluster_status(self, cluster_status):
        """
        Sets the cluster_status of this Emr.
        status of the EMR

        :param cluster_status: The cluster_status of this Emr.
        :type: str
        """

        self._cluster_status = cluster_status

    @property
    def dns_sync_status(self):
        """
        Gets the dns_sync_status of this Emr.
        status of the EMR DNS Sync

        :return: The dns_sync_status of this Emr.
        :rtype: str
        """
        return self._dns_sync_status

    @dns_sync_status.setter
    def dns_sync_status(self, dns_sync_status):
        """
        Sets the dns_sync_status of this Emr.
        status of the EMR DNS Sync

        :param dns_sync_status: The dns_sync_status of this Emr.
        :type: str
        """

        self._dns_sync_status = dns_sync_status

    @property
    def cluster_name(self):
        """
        Gets the cluster_name of this Emr.
        Name of the EMR cluster

        :return: The cluster_name of this Emr.
        :rtype: str
        """
        return self._cluster_name

    @cluster_name.setter
    def cluster_name(self, cluster_name):
        """
        Sets the cluster_name of this Emr.
        Name of the EMR cluster

        :param cluster_name: The cluster_name of this Emr.
        :type: str
        """

        self._cluster_name = cluster_name

    @property
    def created_by(self):
        """
        Gets the created_by of this Emr.
        EMR created by user

        :return: The created_by of this Emr.
        :rtype: str
        """
        return self._created_by

    @created_by.setter
    def created_by(self, created_by):
        """
        Sets the created_by of this Emr.
        EMR created by user

        :param created_by: The created_by of this Emr.
        :type: str
        """

        self._created_by = created_by

    @property
    def owned_by(self):
        """
        Gets the owned_by of this Emr.
        EMR created by user group

        :return: The owned_by of this Emr.
        :rtype: str
        """
        return self._owned_by

    @owned_by.setter
    def owned_by(self, owned_by):
        """
        Sets the owned_by of this Emr.
        EMR created by user group

        :param owned_by: The owned_by of this Emr.
        :type: str
        """

        self._owned_by = owned_by

    @property
    def created_at(self):
        """
        Gets the created_at of this Emr.
        EMR created at time

        :return: The created_at of this Emr.
        :rtype: str
        """
        return self._created_at

    @created_at.setter
    def created_at(self, created_at):
        """
        Sets the created_at of this Emr.
        EMR created at time

        :param created_at: The created_at of this Emr.
        :type: str
        """

        self._created_at = created_at

    @property
    def master_inst_type(self):
        """
        Gets the master_inst_type of this Emr.
        EMR master instance type

        :return: The master_inst_type of this Emr.
        :rtype: str
        """
        return self._master_inst_type

    @master_inst_type.setter
    def master_inst_type(self, master_inst_type):
        """
        Sets the master_inst_type of this Emr.
        EMR master instance type

        :param master_inst_type: The master_inst_type of this Emr.
        :type: str
        """

        self._master_inst_type = master_inst_type

    @property
    def num_core_nodes(self):
        """
        Gets the num_core_nodes of this Emr.
        Core nodes in EMR

        :return: The num_core_nodes of this Emr.
        :rtype: str
        """
        return self._num_core_nodes

    @num_core_nodes.setter
    def num_core_nodes(self, num_core_nodes):
        """
        Sets the num_core_nodes of this Emr.
        Core nodes in EMR

        :param num_core_nodes: The num_core_nodes of this Emr.
        :type: str
        """

        self._num_core_nodes = num_core_nodes

    @property
    def core_inst_type(self):
        """
        Gets the core_inst_type of this Emr.
        EMR core instance type

        :return: The core_inst_type of this Emr.
        :rtype: str
        """
        return self._core_inst_type

    @core_inst_type.setter
    def core_inst_type(self, core_inst_type):
        """
        Sets the core_inst_type of this Emr.
        EMR core instance type

        :param core_inst_type: The core_inst_type of this Emr.
        :type: str
        """

        self._core_inst_type = core_inst_type

    @property
    def num_task_nodes(self):
        """
        Gets the num_task_nodes of this Emr.
        Task nodes in EMR

        :return: The num_task_nodes of this Emr.
        :rtype: str
        """
        return self._num_task_nodes

    @num_task_nodes.setter
    def num_task_nodes(self, num_task_nodes):
        """
        Sets the num_task_nodes of this Emr.
        Task nodes in EMR

        :param num_task_nodes: The num_task_nodes of this Emr.
        :type: str
        """

        self._num_task_nodes = num_task_nodes

    @property
    def task_inst_type(self):
        """
        Gets the task_inst_type of this Emr.
        EMR task instance type

        :return: The task_inst_type of this Emr.
        :rtype: str
        """
        return self._task_inst_type

    @task_inst_type.setter
    def task_inst_type(self, task_inst_type):
        """
        Sets the task_inst_type of this Emr.
        EMR task instance type

        :param task_inst_type: The task_inst_type of this Emr.
        :type: str
        """

        self._task_inst_type = task_inst_type

    @property
    def resize_by(self):
        """
        Gets the resize_by of this Emr.
        EMR resized by user

        :return: The resize_by of this Emr.
        :rtype: str
        """
        return self._resize_by

    @resize_by.setter
    def resize_by(self, resize_by):
        """
        Sets the resize_by of this Emr.
        EMR resized by user

        :param resize_by: The resize_by of this Emr.
        :type: str
        """

        self._resize_by = resize_by

    @property
    def resize_at(self):
        """
        Gets the resize_at of this Emr.
        EMR resized at times

        :return: The resize_at of this Emr.
        :rtype: str
        """
        return self._resize_at

    @resize_at.setter
    def resize_at(self, resize_at):
        """
        Sets the resize_at of this Emr.
        EMR resized at times

        :param resize_at: The resize_at of this Emr.
        :type: str
        """

        self._resize_at = resize_at

    @property
    def error_messages(self):
        """
        Gets the error_messages of this Emr.
        Any error message while creating EMR

        :return: The error_messages of this Emr.
        :rtype: str
        """
        return self._error_messages

    @error_messages.setter
    def error_messages(self, error_messages):
        """
        Sets the error_messages of this Emr.
        Any error message while creating EMR

        :param error_messages: The error_messages of this Emr.
        :type: str
        """

        self._error_messages = error_messages

    @property
    def warnings(self):
        """
        Gets the warnings of this Emr.
        Any warnings while creating EMR

        :return: The warnings of this Emr.
        :rtype: list[str]
        """
        return self._warnings

    @warnings.setter
    def warnings(self, warnings):
        """
        Sets the warnings of this Emr.
        Any warnings while creating EMR

        :param warnings: The warnings of this Emr.
        :type: list[str]
        """

        self._warnings = warnings

    @property
    def emr_applications(self):
        """
        Gets the emr_applications of this Emr.
        applications in EMR 

        :return: The emr_applications of this Emr.
        :rtype: str
        """
        return self._emr_applications

    @emr_applications.setter
    def emr_applications(self, emr_applications):
        """
        Sets the emr_applications of this Emr.
        applications in EMR 

        :param emr_applications: The emr_applications of this Emr.
        :type: str
        """

        self._emr_applications = emr_applications

    @property
    def cerebro(self):
        """
        Gets the cerebro of this Emr.
        cerebro in EMR 

        :return: The cerebro of this Emr.
        :rtype: str
        """
        return self._cerebro

    @cerebro.setter
    def cerebro(self, cerebro):
        """
        Sets the cerebro of this Emr.
        cerebro in EMR 

        :param cerebro: The cerebro of this Emr.
        :type: str
        """

        self._cerebro = cerebro

    @property
    def version(self):
        """
        Gets the version of this Emr.
        version of EMR 

        :return: The version of this Emr.
        :rtype: str
        """
        return self._version

    @version.setter
    def version(self, version):
        """
        Sets the version of this Emr.
        version of EMR 

        :param version: The version of this Emr.
        :type: str
        """

        self._version = version

    @property
    def environment(self):
        """
        Gets the environment of this Emr.
        environment 

        :return: The environment of this Emr.
        :rtype: str
        """
        return self._environment

    @environment.setter
    def environment(self, environment):
        """
        Sets the environment of this Emr.
        environment 

        :param environment: The environment of this Emr.
        :type: str
        """

        self._environment = environment

    @property
    def master_ip(self):
        """
        Gets the master_ip of this Emr.
        master ip of EMR 

        :return: The master_ip of this Emr.
        :rtype: str
        """
        return self._master_ip

    @master_ip.setter
    def master_ip(self, master_ip):
        """
        Sets the master_ip of this Emr.
        master ip of EMR 

        :param master_ip: The master_ip of this Emr.
        :type: str
        """

        self._master_ip = master_ip

    @property
    def applications(self):
        """
        Gets the applications of this Emr.


        :return: The applications of this Emr.
        :rtype: EmrApplications
        """
        return self._applications

    @applications.setter
    def applications(self, applications):
        """
        Sets the applications of this Emr.


        :param applications: The applications of this Emr.
        :type: EmrApplications
        """

        self._applications = applications

    def to_dict(self):
        """
        Returns the model properties as a dict
        """
        result = {}

        for attr, _ in iteritems(self.swagger_types):
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = list(map(
                    lambda x: x.to_dict() if hasattr(x, "to_dict") else x,
                    value
                ))
            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()
            elif isinstance(value, dict):
                result[attr] = dict(map(
                    lambda item: (item[0], item[1].to_dict())
                    if hasattr(item[1], "to_dict") else item,
                    value.items()
                ))
            else:
                result[attr] = value

        return result

    def to_str(self):
        """
        Returns the string representation of the model
        """
        return pformat(self.to_dict())

    def __repr__(self):
        """
        For `print` and `pprint`
        """
        return self.to_str()

    def __eq__(self, other):
        """
        Returns true if both objects are equal
        """
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """
        Returns true if both objects are not equal
        """
        return not self == other