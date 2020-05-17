#!/usr/bin/python2.7

#######################################################################################################################
# slack_plugin.py
# Description: Slack operator to send message on a channel
#######################################################################################################################

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

import logging
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
from airflow.configuration import conf
import requests
import json


class SlackOperator(BaseOperator):
    template_fields = tuple()
    ui_color = '#ededef'

    @apply_defaults
    def __init__(self,
                 channel=None,
                 message=None,
                 *args, **kwargs):
        super(SlackOperator, self).__init__(*args, **kwargs)
        if channel is None or message is None:
            if channel is None:
                logging.error('Need to provide slack channel')
            else:
                logging.error('Need to provide message')
            raise AirflowException("Missing channel/message for Slack Operator")

        self.channel = "#" + channel
        self.message = message

    def get_slack_url(self):
        return conf.get('slack', 'url')

    def get_cluster_stamp(self):
        return "[from airflow-{cluster}] ".format(cluster=conf.get('core', 'cluster'))

    def send_slack_message(self, webhook_url, slack_data):
        response = requests.post(
            webhook_url, data=json.dumps(slack_data),
            headers={'Content-Type': 'application/json'}
        )
        if response.status_code != 200:
            raise AirflowException(
                'Request to slack returned an error %s, the response is:\n%s'
                % (response.status_code, response.text)
            )


    def execute(self, context):
        webhook_url = self.get_slack_url()
        cluster_stamp = self.get_cluster_stamp()
        message = str(cluster_stamp) + self.message
        slack_data = {'text': message, 'channel': self.channel}
        logging.info('sending slack message {message} to {channel}'.format(message=self.message, channel=self.channel))
        self.send_slack_message(webhook_url, slack_data)
        logging.info('message sent successfully')


class SlackPlugin(AirflowPlugin):
    name = "slack_plugin"
    operators = [SlackOperator]
