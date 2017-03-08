import boto3
import logging
from data_sources.hippo_base import HippoDataSource


class S3Bucket(HippoDataSource):
    namespace = 's3bucket'
    label = 'S3 Bucket Key List'
    inputs = {
        'awskey': {'input':'text','label':'AWS Key Id'},
        'awssecret': {'input':'text','label':'AWS Secret'},
        'awsregion': {'input':'text','label':'AWS Region','default':'us-east-1'},
        'bucket_name': {'input':'text','label':'Bucket Name'},
        'earliest_unix_tstamp': {'input':'text','label':'Earliest Unix Timestamp','default':''}
    }

    def __init__(self, *args):
        super().__init__(*args, namespace=S3Bucket.namespace, inputs=S3Bucket.inputs)

    def process(self):
        if not self.awskey or not self.bucket_name or not self.awssecret:
            return

        session = boto3.Session(aws_access_key_id = self.awskey,
                          aws_secret_access_key = self.awssecret,
                          region_name = self.awsregion)

        s3 = session.resource('s3')
        queue = sqs.get_queue_by_name(QueueName=self.queuename)

        max_msg = min(self.new_task_limit,10)
        if max_msg > 0:
            messages = queue.receive_messages(MaxNumberOfMessages=max_msg)

            if messages:
                self.create_tasks([m.body for m in messages])

            for m in messages:
                m.delete()
