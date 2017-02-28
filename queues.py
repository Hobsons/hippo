import json
import uuid
import time
import config
from threading import Thread
from queue import Queue
from cerberus import Validator
from tasks import TASK_SCHEMA
from data_sources import *


class HippoQueue(object):
    def __init__(self, id=None, definition=None, redis_client=None):
        self.id = id
        self.definition = definition
        self.redis = redis_client
        if id is None:
            self.id = self.definition.get('id','hippo-queue') + '.' + str(uuid.uuid4())
            self.save()
        elif definition is None:
            self.load()

    @classmethod
    def queues_from_ids(cls, queue_ids, redis_client):
        if not queue_ids:
            return []
        pipe = redis_client.pipeline()
        for qid in queue_ids:
            pipe.get(qid)
        results = pipe.execute()
        result_objects = []
        for qstr in results:
            if qstr:
                result_objects.append(cls(definition=json.loads(qstr),redis_client=redis_client))
        return result_objects

    @classmethod
    def all_queues(cls, redis_client):
        return cls.queues_from_ids(redis_client.lrange('hippo:all_queueid_list',0,-1), redis_client)

    @classmethod
    def process_queues(cls, redis_client):

        def worker():
            while True:
                hq = q.get()
                hq.process_data()
                q.task_done()

        q = Queue()
        for i in range(config.NUM_QUEUE_POLL_WORKERS):
             t = Thread(target=worker)
             t.daemon = True
             t.start()

        def feed_work():
            while True:
                hippo_queues = HippoQueue.all_queues(redis_client)
                if q.qsize() < len(hippo_queues) * 2:
                    for hippo_q in hippo_queues:
                        q.put(hippo_q)
                time.sleep(1)

        Thread(target=feed_work,daemon=True)

    def process_data(self):
        qtype = self.definition['queue']['type']
        processors = HippoDataSource.__subclasses__()
        for p in processors:
            if p.__name__.upper() == qtype.upper():
                dp = p(self)
                if not dp.too_soon():
                    dp.create_tasks()
                break

    def save(self):
        self.redis.set('hippo:queue:' + self.id,json.dumps(self.definition))

    def load(self):
        body = self.redis.get('hippo:queue:' + self.id)
        if not body:
            self.definition = {}
        else:
            self.definition = json.loads(body)

    def validate(self):
        v = Validator(QUEUE_SCHEMA, allow_unknown=True)
        valid = v.validate(self.definition)
        if valid:
            return None
        return str(v.errors)


QUEUE_SCHEMA = TASK_SCHEMA.update(
    ""
)