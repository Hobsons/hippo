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
        if not self.awskey or not self.bucket_name or not self.awssecret or self.new_task_limit < 1:
            return

        session = boto3.Session(aws_access_key_id = self.awskey,
                          aws_secret_access_key = self.awssecret,
                          region_name = self.awsregion)

        bname = self.bucket_name.split('/')[0]
        bprefix = self.bucket_name.split('/',1)[1] if '/' in self.bucket_name else ''

        s3 = session.resource('s3')
        bucket = s3.Bucket(bname)

        key_tstamp_tuples = []

        if bprefix:
            biterator = bucket.objects.filter(Prefix=bprefix)
        else:
            biterator = bucket.objects.all()

        last_s3key_processed_tstamp = self.hippo_queue.definition['queue'].get('last_s3key_processed_tstamp',self.earliest_unix_tstamp)

        for key in biterator:
            tstamp = key.last_modified.timestamp()
            if not last_s3key_processed_tstamp or int(last_s3key_processed_tstamp) < tstamp:
                key_tstamp_tuples.append((key.key,tstamp))

        if key_tstamp_tuples:
            logging.warning("s3 triggers present")
            logging.warning(key_tstamp_tuples)
            logging.warning('latest_s3key_processed_tstamp ' + str(last_s3key_processed_tstamp))
            key_tstamp_tuples.sort(key=lambda x: x[1])
            key_tstamp_tuples = key_tstamp_tuples[:self.new_task_limit]
            self.create_tasks([kt[0] for kt in key_tstamp_tuples])
            last_processed_tstamp = key_tstamp_tuples[-1][1]
            self.hippo_queue.definition['queue']['last_s3key_processed_tstamp'] = last_processed_tstamp

