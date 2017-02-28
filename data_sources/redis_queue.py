import time
import redis
from data_sources.hippo_base import HippoDataSource

class RedisQueue(HippoDataSource):
    def __init__(self, hippo_queue):
        super().__init__(hippo_queue)
        self.host = self.definition['queue'].get('redis',{}).get('host')
        self.port = int(self.definition['queue'].get('redis',{}).get('port','6379'))
        self.db = int(self.definition['queue'].get('redis',{}).get('db',0))
        self.name = self.definition['queue'].get('redis',{}).get('name')

    def create_tasks(self):
        if not self.name or not self.host:
            return

        redis_client = redis.StrictRedis(host=self.host, port=int(self.port), db=int(self.db))
        

        super().create_tasks()
