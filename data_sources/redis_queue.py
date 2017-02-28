import redis
from data_sources.hippo_base import HippoDataSource


class RedisQueue(HippoDataSource):
    def __init__(self, hippo_queue, working_count):
        super().__init__(hippo_queue, working_count)
        self.host = self.definition['queue'].get('redis',{}).get('host')
        self.port = int(self.definition['queue'].get('redis',{}).get('port','6379'))
        self.db = int(self.definition['queue'].get('redis',{}).get('db',0))
        self.name = self.definition['queue'].get('redis',{}).get('name')

    def process(self):
        if not self.name or not self.host:
            return

        redis_client = redis.StrictRedis(host=self.host, port=int(self.port), db=int(self.db))

        count = 0
        list_name = self.name
        limbo_list_name = 'hippo:queue:' + list_name + '_limbo'

        limbo_items = redis_client.lrange(limbo_list_name,0,-1)
        if limbo_items:
            count = len(limbo_items)
            self.create_tasks(limbo_items)

        items = []
        while count < self.new_task_limit:
            i = redis_client.rpoplpush(list_name,limbo_list_name)
            if i:
                items.append(i)
                count += 1
            else:
                break

        if items:
            self.create_tasks(items)
        redis_client.delete(limbo_list_name)