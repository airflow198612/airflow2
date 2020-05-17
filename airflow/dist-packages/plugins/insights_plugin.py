
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


# Will show up under airflow.operators.PluginOperator
class InsightsOperator(BaseOperator):
    """
    CREATE BY : Bharani Manavalan
    Connects to the URL for an event trigger.
    :param event_name: reference to a specific Metric Insights Event
    """
    ui_color = '#94A8CF'

    @apply_defaults
    def __init__(self, trigger_id, conn_id='insights_url', *args, **kwargs):
        super(InsightsOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.trigger_id = trigger_id
        self.insights_url = None
        self.final_url = None

    def execute(self, context):
        #Getting URL for the conn_id
        self.insights_url = self.get_insights_url()
        logging.info('Generic Metric Insights URL is: ' + str(self.insights_url))
        logging.info('Event Name to be triggered is: ' + str(self.trigger_id))
        #Forming final URL.
        self.final_url = str(self.insights_url) + str(self.trigger_id)
        logging.info('Final Url to be pinged: ' + self.final_url)

        self.open_insights_url()

    def open_insights_url(self):
        from urllib2 import urlopen, URLError, HTTPError
        import ssl
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        try:
            # Triggering Metrics insights url for data collection.
            response = urlopen(self.final_url, context=ctx)
        except HTTPError, e:
            logging.info(e)
            raise ValueError('The server could not fulfill the request. Reason:', str(e.code))
        except URLError, e:
            logging.info(e)
            raise ValueError('We failed to reach a server. Reason:', str(e.reason))
        else:
            html = response.info()
            response.close()
            logging.info(html)
            logging.info('SUCCESS: Event Trigger Issued. Please check the Metrics Insights url for further status.')

    def get_insights_url(self):
        """
        Returns a metrics insights url as configured in connection.
        """
        from airflow.hooks.dbapi_hook import DbApiHook

        conn_config = DbApiHook.get_connection(self.conn_id)
        url = conn_config.host
        return url

class InsightsPlugin(AirflowPlugin):
    name = "insights_plugin"
    operators = [InsightsOperator]
