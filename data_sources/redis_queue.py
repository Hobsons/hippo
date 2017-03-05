import redis
from data_sources.hippo_base import HippoDataSource


class RedisQueue(HippoDataSource):
    namespace = 'redis'
    label = 'Redis Queue'
    inputs = {
        'host': {'input':'text','label':'Redis Host'},
        'port': {'input':'number','label':'Redis Port','default':6379},
        'db'  : {'input':'number','label':'Redis DB','default':0},
        'name': {'input':'text','label':'Redis Queue Key Name'}
    }

    def __init__(self, *args):
        super().__init__(*args, namespace=RedisQueue.namespace, inputs=RedisQueue.inputs)

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