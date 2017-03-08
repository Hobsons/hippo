import boto3
import logging
from data_sources.hippo_base import HippoDataSource


class SqsQueue(HippoDataSource):
    namespace = 'sqs'
    label = 'SQS Queue'
    inputs = {
        'awskey': {'input':'text','label':'AWS Key Id'},
        'awssecret': {'input':'text','label':'AWS Secret'},
        'awsregion': {'input':'text','label':'AWS Region','default':'us-east-1'},
        'queuename': {'input':'text','label':'SQS Queue Name'}
    }

    def __init__(self, *args):
        super().__init__(*args, namespace=SqsQueue.namespace, inputs=SqsQueue.inputs)

    def process(self):
        if not self.awskey or not self.queuename or not self.awssecret:
            return

        session = boto3.Session(aws_access_key_id = self.awskey,
                          aws_secret_access_key = self.awssecret,
                          region_name = self.awsregion)

        sqs = session.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=self.queuename)

        max_msg = min(self.new_task_limit,10)
        if max_msg > 0:
            messages = queue.receive_messages(MaxNumberOfMessages=max_msg)

            if messages:
                self.create_tasks([m.body for m in messages])

            for m in messages:
                m.delete()
