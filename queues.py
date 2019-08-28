import json
import uuid
import time
import copy
import config
import redis
import logging
from threading import Thread, Lock
from queue import Queue
from cerberus import Validator
from tasks import TASK_SCHEMA, HippoTask
from data_sources import *
from aes import encrypt_str, decrypt_str


class HippoQueue(object):
    __stop_processing = False

    def __init__(self, id=None, definition=None, redis_client=None):
        self.id = id
        self.definition = definition
        self.redis = redis_client
        self.loaded_queue_config = None
        if id is None:
            self.id = self.definition.get('id','')
            if not self.id:
                self.id = 'hippo-queue' + '.' + str(uuid.uuid4())
            self.definition['id'] = self.id
            if 'status' not in self.definition['queue']:
                self.definition['queue']['status'] = 'ENABLED'
            if 'env' in self.definition:
                # fix any env vars that are passed in as numbers instead of strings
                for v in self.definition['env']:
                    self.definition['env'][v] = str(self.definition['env'][v])
            self.save()
            redis_id = 'hippo:queue:' + self.id
            if redis_id not in [i.decode() for i in redis_client.lrange('hippo:all_queueid_list',0,-1)]:
                redis_client.lpush('hippo:all_queueid_list', redis_id)
        elif definition is None:
            self.load()

        self.loaded_queue_config = copy.copy(self.definition.get('queue',{}))

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
                if isinstance(qstr,bytes):
                    qstr = qstr.decode()
                try:
                    qobj = json.loads(decrypt_str(qstr))
                except Exception:
                    qobj = json.loads(qstr)
                result_objects.append(cls(definition=qobj,redis_client=redis_client))
        return result_objects

    @classmethod
    def all_queues(cls, redis_client):
        return cls.queues_from_ids(redis_client.lrange('hippo:all_queueid_list',0,-1), redis_client)

    @classmethod
    def process_queues(cls, redis_client):

        running_queue_ids = {}
        running_queue_lock = Lock()

        def mark_is_running(queue_id, running):
            with running_queue_lock:
                if running:
                    running_queue_ids[queue_id] = True
                elif queue_id in running_queue_ids:
                    del running_queue_ids[queue_id]

        def is_running(queue_id):
            with running_queue_lock:
                return running_queue_ids.get(queue_id,False)

        q = Queue()

        def worker():
            while True:
                hq, working_count = q.get()
                hq.process_data(working_count)
                q.task_done()
                mark_is_running(hq.id,False)

        worker_threads = []
        for i in range(config.NUM_QUEUE_POLL_WORKERS):
            t = Thread(target=worker)
            t.daemon = True
            t.start()
            worker_threads.append(t)

        def workers_alive():
            for t in worker_threads:
                if not t.is_alive():
                    return False
            return True

        def feed_work():
            while workers_alive() and not cls.__stop_processing:
                try:
                    hippo_queues = HippoQueue.all_queues(redis_client)
                    working_count_by_id = HippoTask.working_task_count_by_id(redis_client)
                    for hippo_q in hippo_queues:
                        if not is_running(hippo_q.id):
                            q.put((hippo_q, working_count_by_id.get(hippo_q.id,0)))
                            mark_is_running(hippo_q.id,True)
                except redis.exceptions.ConnectionError:
                    logging.warning('Redis Connection Error in Queue Processing Thread')
                time.sleep(1)

        pq_t = Thread(target=feed_work,daemon=True)
        pq_t.start()
        return pq_t

    def process_data(self, working_count):
        qtype = self.definition['queue']['type']
        processors = HippoDataSource.__subclasses__()
        for p in processors:
            if p.namespace.upper() == qtype.upper():
                dp = p(self, working_count, HippoTask, self.redis)
                if self.is_enabled() and not dp.too_soon():
                    dp.process_source()
                break

    def save(self):
        if self.loaded_queue_config:
            # check for timestamps that are local to the queue that should be saved even if definition was updated
            for k in self.loaded_queue_config:
                if k.startswith('last_') and k not in self.definition['queue']:
                    self.definition['queue'][k] = self.loaded_queue_config[k]
        self.redis.set('hippo:queue:' + self.id,encrypt_str(json.dumps(self.definition)))

    def load(self):
        body = self.redis.get('hippo:queue:' + self.id)
        if not body:
            self.definition = {}
        else:
            if isinstance(body,bytes):
                body = body.decode()
            try:
                self.definition = json.loads(decrypt_str(body))
            except Exception:
                self.definition = json.loads(body)

    def is_enabled(self):
        return 'status' not in self.definition['queue'] or self.definition['queue']['status'] == 'ENABLED'

    def enable(self):
        self.definition['queue']['status'] = 'ENABLED'
        self.save()

    def disable(self):
        self.definition['queue']['status'] = 'DISABLED'
        self.save()

    def delete(self):
        pipe = self.redis.pipeline()
        pipe.lrem('hippo:all_queueid_list',0,'hippo:queue:' + self.id)
        pipe.delete('hippo:queue:' + self.id)
        pipe.execute()

    def validate(self):
        s = copy.copy(QUEUE_SCHEMA)
        s.update(TASK_SCHEMA)
        ns = self.definition['queue'].get('type')
        processors = HippoDataSource.__subclasses__()
        for p in processors:
            if p.namespace.upper() == ns.upper() and getattr(p,'inputs',None):
                print('VALIDATOR - MATCHED SCHEMA', p.namespace)
                ns_schema = {}
                for input_name in p.inputs:
                    itype = "string" if p.inputs[input_name]['input'] in ['text','password'] else 'integer'
                    ns_schema[input_name] = {
                        "type":itype
                    }
                    if p.inputs[input_name].get('default') is None:
                        ns_schema[input_name]['required'] = True
                        if itype == 'string':
                            ns_schema[input_name]['empty'] = False
                s['queue']['schema'][ns] = {
                    "type":"dict",
                    "required":True,
                    "schema":ns_schema
                }
        print('VALIDATOR - SCHEMA', s)
        print('VALIDATOR - DEFINITION', self.definition)
        v = Validator(s, allow_unknown=True)
        valid = v.validate(self.definition)
        if valid:
            return None
        return str(v.errors)


QUEUE_SCHEMA = {
    "queue": {
        "type": "dict",
        "required":True,
        "schema": {
            "type": {
                "type":"string",
                "required":True,
                "empty": False
            },
            "name": {
                "type":"string",
                "required":True,
                "empty": False
            },
            "max_concurrent": {
                "type":"integer",
                "min":0,
                "max":64000,
            },
            "batch_size": {
                "type":"integer",
                "min":1,
                "max":1000,
            },
            "batch_separator": {
                "type":"string",
            },
            "frequency_seconds": {
                "type":"integer",
                "min":0,
                "max":86400,
            },
            "last_run_tstamp": {
                "type":"integer"
            },
            "status": {
                "type":"string",
                "allowed":["ENABLED","DISABLED"]
            }


        }
    }
}