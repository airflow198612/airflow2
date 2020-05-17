from datetime import datetime
import time
from airflow.exceptions import AirflowException
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

extractstart = datetime.today()

class TabExtractRefreshOperator(BaseOperator):
    """
    Refreshes the Tableau Extract
    :param conn_id: reference to a specific Tableau Connection
    :param conn_id_pg: reference to a specific Tableau Postgres Connection
    :param tabsiteid: reference to tableau site name
    :param tabproj: reference to tableau project
    :param tabwkbk:  reference to tableau workbook
    :param tabdatasource:  reference to tableau datasource
    """
    ui_color = '#ededef'
    @apply_defaults
    def __init__(
            self,
            # python_callable=None,
            conn_id='Tableau',
            conn_id_pg='Tableau_Postgres',
            tabsiteid=None,
            tabproj=None,
            tabwkbk=None,
            tabdatasource=None,
            *args, **kwargs):
        super(TabExtractRefreshOperator, self).__init__(*args, **kwargs)
        # self.python_callable = None
		# self.op_args = op_args or []
        # self.op_kwargs = op_kwargs or {}
        # self.provide_context = provide_context
        # self.templates_dict = templates_dict
        self.conn_id = conn_id
        self.conn_id_pg = conn_id_pg
        self.numofloops = None
        self.waitingtime = None
        self.tabworkingdir = None
        self.conn = None
        self.connpg = None
        self.tablibpath = None
        self.tablogpath = None
        self.tabsiteid = tabsiteid
        self.tabserverurl = None
        self.tabuser = None
        self.tabpwd = None
        self.tab_pg_host = None

        if tabproj:
            self.tabproj = tabproj
            logging.info("Project Name Provided {0}".format(self.tabproj))
        else:
            raise AirflowException("Please specify a Project Name")

        if tabwkbk:
            self.tabwkbk = tabwkbk
            logging.info("WorkBook Name Provided {0}".format(self.tabwkbk))
        elif tabdatasource:
            self.tabdatasource = tabdatasource
            logging.info("DataSource Name Provided {0}".format(self.tabdatasource))
        else:
            raise AirflowException("Please specify a Data Source or a Workbook to Refresh")

    def execute(self, context):
        # Getting the tableau Details by calling the function
        self.get_tableau_details()
        # Getting the tableau Postgres Details by calling the function
        self.get_tableau_pg_details()
        # Login to tableau
        logging.info("tableau_serverurl={0}, tableau_user={1}".format(self.tabserverurl, self.tabuser))
        self.tab_login()
        # Wait : and execute the tableau extract refresh
        time.sleep(float(self.waitingtime))
        self.tab_refreshextracts()
        return_value = 'success'
        return return_value

    def tabcmd(self, argstring):
        import os, sys, subprocess, logging
        cmd = """java -Xmx64m -Xss2048k -Djsse.enableSNIExtension=false -Dpid=$$\
              -Dlog.file={1}/.tabcmd/tabcmd.log\
              -Dsession.file={1}/.tabcmd/tabcmd-session.xml\
              -Din.progress.dir={1}/.tabcmd\
              -Dconsole.codepage=$LANG\
              -Dconsole.cols=$COLUMNS\
              -cp "{0}/*"\
              com.tableausoftware.tabcmd.Tabcmd {2} """.format(self.tablibpath, self.tablogpath, argstring)

        proc=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        output=proc.communicate()[0]
        logging.info(output)
        rc=proc.returncode
        return rc

    def tab_login(self):
        if self.tabsiteid in ('Default','default',''):
                self.tabsiteid = ''

        logincmd = "login -s \'{0}\' -t \'{1}\' -u \'{2}\' -p \'{3}\' --no-certcheck".format(self.tabserverurl, self.tabsiteid, self.tabuser, self.tabpwd)
        # logincmd = "login -s \"{0}\" -u \"{1}\" -p \"{2}\" " \
                   # "--no-certcheck".format(self.tabserverurl, self.tabuser, self.tabpwd)
        return_code = self.tabcmd(logincmd)

        if return_code is not 0:
            raise AirflowException("Login Command was not successful, please check if the parameters provided are correct and re-run the script.")


			
    def tab_refreshextracts(self):
        if hasattr(self,'tabproj') and hasattr(self,'tabwkbk'):

            refreshwkbkcmd ="refreshextracts --synchronous --project \'{0}\' --workbook \'{1}\' --no-certcheck".format(self.tabproj, self.tabwkbk)
            logging.info(refreshwkbkcmd)

            return_code = self.tabcmd(refreshwkbkcmd)
            logging.info("Refresh Command Return Code is: ")
            logging.info(return_code)

            if return_code is not 0:
                logging.info("The refresh extract command was not successful. At this point it is uncertain if this is due to Synchronous connection failure or the input parameters provided are wrong")
                logging.info("Proceeding to connect to the backend database to check the status")
            self.tab_ref_status()

        elif hasattr(self,'tabproj') and hasattr(self,'tabdatasource'):

            refreshdscmd ="refreshextracts --synchronous --project \'{0}\' --datasource \'{1}\' --no-certcheck".format(self.tabproj, self.tabdatasource)
            logging.info(refreshdscmd)
			
            return_code = self.tabcmd(refreshdscmd)
            logging.info("Refresh Command Return Code is: ")
            logging.info(return_code)

            if return_code is not 0:
                logging.info("The refresh extract command was not successful. At this point it is uncertain if this is due to Synchronous connection failure or the input parameters provided are wrong")
                logging.info("Proceeding to connect to the backend database to check the status")
            self.tab_ref_status()

        else:

            raise AirflowException("Please provide the name of the Project and WorkBook or Project "
                                   "and DataSource that has to be refreshed.")

    def tab_ref_status(self):

        for x in range(int(self.numofloops)):

            logging.info("Initiating wait time of {0} seconds before connecting to the database to check the actual status".format(self.waitingtime))
            time.sleep(float(self.waitingtime))
            queryres = self.tab_pg_bg_status_chk()
            if queryres:

                progress = queryres[1]
                finish_code = queryres[2]
                statmsg = queryres[3]

                logging.info("Record in the database indicates..")

                if (progress < 100 ):
                    logging.info("Tableau extract refresh is in process")
                    continue
                elif(progress == 100 and finish_code == 1):
                    logging.info("Tableau extract refresh failed")
                    raise AirflowException(statmsg)
                elif(progress == 100 and finish_code == 0):
                    logging.info("Tableau extract refresh successful")
                    break
                else:
                    raise AirflowException(statmsg)

            else:
                logging.info("There is no record in the backend database")
                raise AirflowException("Please check if the name provided for Project, Workbook or Data source is correct and re-run the script.")

    def tab_pg_bg_status_chk(self):

        import psycopg2
        import sys

        try:
            conn = psycopg2.connect(host=self.tab_pg_host, dbname=self.tab_pg_dbname, user=self.tab_pg_user, password=self.tab_pg_password, port=self.tab_pg_port)
        except psycopg2.Error, e:
            logging.info(e)
            raise AirflowException("Could not connect to the backend postgres database to check the status")
        cur = conn.cursor()
		
        if hasattr(self,'tabwkbk'):
			psqlquery = "SELECT title,progress,finish_code,notes FROM public.background_jobs WHERE (job_name = 'Refresh Extracts' AND title = '{0}' AND created_at >= '{1}') ORDER BY created_at DESC".format(self.tabwkbk, extractstart)
        elif hasattr(self,'tabdatasource'):
			psqlquery = "SELECT title,progress,finish_code,notes FROM public.background_jobs WHERE (job_name = 'Refresh Extracts' AND title = '{0}' AND created_at >= '{1}') ORDER BY created_at DESC".format(self.tabdatasource, extractstart)
				
        cur.execute(psqlquery)
        queryres = cur.fetchone()
        logging.info("Postgres query output is")
        logging.info(queryres)
        conn.close()
        return queryres

    def get_tableau_details(self):
        """
        tableau details.
        """
        from airflow.hooks.dbapi_hook import DbApiHook
        self.conn = DbApiHook.get_connection(self.conn_id)
        logging.info("tableau_url_link={0}, username={1}, numofloops={3}, waitingtime={4}, tabworkingdir={5}"
                     .format(self.conn.host, self.conn.login, self.conn.password
                             , self.conn.extra_dejson.get('noofloops', "500")
                             , self.conn.extra_dejson.get('waitingtime', "30")
                             , self.conn.extra_dejson.get('tabworkingdir', "SAMPLE")))
        self.tabserverurl = self.conn.host
        self.tabdefaultsite = self.conn.extra_dejson.get('tabsiteid', "")
        self.tabuser = self.conn.login
        self.tabpwd = self.conn.password
        self.numofloops = self.conn.extra_dejson.get('noofloops', None)
        self.waitingtime = self.conn.extra_dejson.get('waitingtime', None)
        print ("**********"+self.numofloops+"************")
        self.tabworkingdir = self.conn.extra_dejson.get('tabworkingdir', "SAMPLE")
        self.tablibpath = self.conn.extra_dejson.get('tablibpath', "SAMPLE")
        self.tablogpath = self.conn.extra_dejson.get('tablogpath', "SAMPLE")
        # Set the Site Name as per the parameters
        if self.tabsiteid:
                self.tabsiteid = self.tabsiteid
        else:
                self.tabsiteid = self.tabdefaultsite


    def get_tableau_pg_details(self):
        """
        tableau postgres details.
        """
        from airflow.hooks.dbapi_hook import DbApiHook
        self.connpg = DbApiHook.get_connection(self.conn_id_pg)
        logging.info("tab-pg_host={0}, username={1}, port={3}, schema={4}"
                     .format(self.connpg.host, self.connpg.login, self.connpg.password
                             , self.connpg.port, self.connpg.schema))
        self.tab_pg_host = self.connpg.host
        self.tab_pg_user = self.connpg.login
        self.tab_pg_password = self.connpg.password
        self.tab_pg_port = self.connpg.port
        self.tab_pg_dbname = self.connpg.schema


class TableauPlugin(AirflowPlugin):
    name = "tableau_plugin"
    operators = [TabExtractRefreshOperator]
