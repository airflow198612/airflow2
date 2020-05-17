import logging
import json
import boto3
from pprint import pformat


class DMError(object):
    def __init__(self, function, jobId, currentInterval, exception, *args, **kwargs):
        self.function = function
        self.operation = "updateJobInstance"
        self.status = "ERROR"
        self.jobId = jobId
        self.currentInterval = currentInterval
        self.exception = exception
        logging.info('Current Interval is %s', self.currentInterval)

    @classmethod
    def on_failure(cls, context, exception):
        logging.info('failure: context = %s', pformat(context))
        currentInterval = context['execution_date'].strftime('%Y-%m-%dT%H:%M')
        function = context['task'].env['function']
        jobId = context['task'].env['jobId']
        DMError(function, jobId, currentInterval, exception).update()

    def createPayload(self):
        logging.info("job id = " + self.jobId)
        logging.info("in create payload operation= " + self.operation)
        # payload="{'operation': 'getJobs', 'job_id': {0} }".format( self.jobId)
        if self.status is not None:
            payload = '{"operation": "%s", "job_id": "%s", "interval_id": "%s" ,"run_status": "%s"}' % (
                self.operation, self.jobId, self.currentInterval, self.status)
        else:
            payload = '{"operation": "%s", "job_id": "%s", "interval_id": "%s" }' % (
                self.operation, self.jobId, self.currentInterval)
        self.payload = payload

    def callLambda(self):
        # invoke lambda function
        client = boto3.client('lambda')
        self.createPayload()
        logging.info("Invoking lambda function '%s' with request payload:\n%s", self.function,
                     pformat(json.loads(self.payload)))
        response = client.invoke(
            FunctionName=self.function,
            Payload=self.payload)
        # check HTTP status code returned from Lambda call, 200 is good
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logging.info("Lambda return 200 - successful")
        else:
            logging.info("Lambda call returns bad result")
            raise self.exception("Error calling Lambda function")
        return response

    def callDM(self):
        response = self.callLambda()
        logging.info('Response:\n%s', pformat(response))

        # getting the payload from response to get the
        stream = response['Payload']
        r = stream.read()
        j = json.loads(r)
        if isinstance(j, list):
            logging.info("Response payload:\n%s", pformat(j[0]))
            return j[0]
        else:
            logging.info("Response payload:\n%s", pformat(j))
            return j

    def update(self):
        logging.info('calling DM to updateJobInstance %s to %s', (self.jobId, self.status))
        try:
            response = self.callDM()
        except Exception:
            raise self.exception("error update the job to %s in DM for interval %s",
                                   (self.operation, self.currentInterval))

        try:
            success = response['success']
        except KeyError:
            success = False

        if not success:
            raise self.exception("Update the job %s, to %s in DM not successful for interval %s",
                                   (self.jobId, self.status, self.currentInterval))
        return response
