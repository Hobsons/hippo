import copy
import json
import time
import uuid
import logging
import config
from addict import Dict
from cerberus import Validator
from aes import encrypt_str, decrypt_str


class HippoTask(object):
    def __init__(self, mesos_id=None, definition=None, redis_client=None):
        self.mesos_id = mesos_id
        self.definition = definition
        self.redis = redis_client

        if not definition:
            self.load()

        if 'task_retries' not in self.definition:
            self.definition['task_retries'] = 0

        if 'system_retries' not in self.definition:
            self.definition['system_retries'] = 2

        if mesos_id is None:
            if 'mesos_id' in self.definition:
                self.mesos_id = self.definition['mesos_id']
            else:
                self.mesos_id = self.definition.get('id','hippo') + '.' + str(uuid.uuid4())
                self.definition['mesos_id'] = self.mesos_id
                self.definition['mesos_state'] = 'WAITING_ON_OFFERS'
                self.definition['mesos_agent'] = ''
                if 'env' in self.definition:
                    # fix any env vars that are passed in as numbers instead of strings
                    for v in self.definition['env']:
                        self.definition['env'][v] = str(self.definition['env'][v])
                self.save()

    @classmethod
    def tasks_from_ids(cls, task_ids, redis_client):
        if not task_ids:
            return []
        pipe = redis_client.pipeline()
        for tid in task_ids:
            pipe.get(tid)
        results = pipe.execute()
        result_objects = []
        for tstr in results:
            if tstr:
                if isinstance(tstr,bytes):
                    tstr = tstr.decode()
                try:
                    tobj = json.loads(decrypt_str(tstr))
                except Exception:
                    tobj = json.loads(tstr)
                result_objects.append(cls(definition=tobj,redis_client=redis_client))
        return result_objects

    @classmethod
    def all_tasks(cls, redis_client):
        return cls.tasks_from_ids(redis_client.zrange('hippo:all_taskid_list',0,-1), redis_client)

    @classmethod
    def cleanup_old_tasks(cls, redis_client):
        tasks = cls.all_tasks(redis_client)

        deleted_count = 0
        count_by_id = {}

        # max life for completed tasks
        cutoff = time.time() - config.TASK_RETENTION_SECONDS
        # max life for running tasks
        cutoff_running = time.time() - (config.TASK_RETENTION_SECONDS * 3)

        for t in tasks:
            if 'status_tstamp' not in t.definition or \
               t.definition['status_tstamp'] < cutoff_running or \
               (t.definition['status_tstamp'] < cutoff and t.definition.get('mesos_state','') in MESOS_FINAL_STATES):
                t.delete()
                deleted_count += 1
            else:
                count_by_id.setdefault(t.definition_id(),[]).append(t)

        for tid in count_by_id:
            if len(count_by_id[tid]) > config.TASK_RETENTION_COUNT:
                # sort by status_tstamp to put oldest first
                count_by_id[tid].sort(key=lambda x: x.definition['status_tstamp'])
                to_del_count = len(count_by_id[tid]) - config.TASK_RETENTION_COUNT
                deleted_this_id_count = 0
                # start deleting completed tasks, break if we hit a running task or count goes below limit
                for t in count_by_id[tid]:
                    if deleted_this_id_count >= to_del_count or t.definition.get('mesos_state','') not in MESOS_FINAL_STATES:
                        break
                    t.delete()
                    deleted_this_id_count += 1
                    deleted_count += 1

        logging.info('Deleted %d old tasks' % deleted_count)

    @classmethod
    def kill_tasks(cls, redis_client):
        return cls.tasks_from_ids(redis_client.lrange('hippo:kill_taskid_list',0,-1), redis_client)

    @classmethod
    def waiting_tasks(cls, redis_client):
        return cls.tasks_from_ids(redis_client.lrange('hippo:waiting_taskid_list',0,-1), redis_client)

    @classmethod
    def working_tasks(cls, redis_client):
        return cls.tasks_from_ids(redis_client.lrange('hippo:working_taskid_list',0,-1), redis_client)

    @classmethod
    def working_task_count_by_id(cls, redis_client):
        working_tasks = cls.working_tasks(redis_client)
        working_count_by_id = {}
        for t in working_tasks:
            working_count_by_id.setdefault(t.definition_id(),0)
            working_count_by_id[t.definition_id()] += 1
        return working_count_by_id

    def delete(self):
        pipe = self.redis.pipeline()
        pipe.lrem('hippo:waiting_taskid_list',0,self.mesos_id)
        pipe.lrem('hippo:working_taskid_list',0,self.mesos_id)
        pipe.lrem('hippo:kill_taskid_list',0,self.mesos_id)
        pipe.zrem('hippo:all_taskid_list',self.mesos_id)
        pipe.delete(self.mesos_id)
        pipe.execute()

    def load(self):
        body = self.redis.get(self.mesos_id)
        if not body:
            self.definition = {}
        else:
            if isinstance(body,bytes):
                body = body.decode()
            try:
                self.definition = json.loads(decrypt_str(body))
            except Exception:
                self.definition = json.loads(body)

    def save(self):
        self.definition['status_tstamp'] = time.time()
        self.redis.set(self.mesos_id,encrypt_str(json.dumps(self.definition)))

    def queue(self):
        self.redis.lpush('hippo:waiting_taskid_list',self.mesos_id)
        self.redis.zadd('hippo:all_taskid_list',{self.mesos_id: int(time.time())})

    def work(self):
        pipe = self.redis.pipeline()
        pipe.lpush('hippo:working_taskid_list',self.mesos_id)
        pipe.lrem('hippo:waiting_taskid_list',0,self.mesos_id)
        pipe.execute()
        self.definition['mesos_state'] = 'TASK_STAGING'
        self.save()

    def finish(self):
        self.redis.lrem('hippo:working_taskid_list',0,self.mesos_id)

    def retry(self):
        new_def = copy.copy(self.definition)
        del new_def['mesos_id']
        if self.definition['mesos_state'] in MESOS_SYSTEM_ERRORS and self.definition['system_retries'] > 0:
            new_def['system_retries'] -= 1
        elif self.definition['mesos_state'] in MESOS_TASK_ERRORS and self.definition['task_retries'] > 0:
            new_def['task_retries'] -= 1
        else:
            new_def = None
        if new_def:
            t = HippoTask(definition=new_def,redis_client=self.redis)
            t.queue()

    def kill(self):
        self.redis.lpush('hippo:kill_taskid_list',self.mesos_id)
        self.definition['system_retries'] = 0
        self.definition['task_retries'] = 0
        self.save()

    def kill_complete(self):
        self.redis.lrem('hippo:kill_taskid_list',0,self.mesos_id)

    def definition_id(self):
        return self.definition.get('id','')

    def cpus(self):
        return self.definition.get('cpus',0.1)

    def mem(self):
        return self.definition.get('mem',256)

    def max_concurrent(self):
        return self.definition.get('max_concurrent',10)

    def constraints_ok(self,offer):
        offer_attributes = {}
        for a in offer.get('attributes',[]):
            offer_attributes[a['name']] = a.get('text','')

        for constraint_tuple in self.definition.get('constraints',[]):
            if len(constraint_tuple) == 3:
                attr, op, val = constraint_tuple
                if op in ['CLUSTER','EQUAL','LIKE'] and offer_attributes.get(attr) != val:
                    return False
                if op in ['UNEQUAL','UNLIKE'] and offer_attributes.get(attr) == val:
                    return False
        return True

    def validate(self):
        v = Validator(TASK_SCHEMA, allow_unknown=True)
        valid = v.validate(self.definition)
        if valid:
            return None
        return str(v.errors)

    def mesos_launch_definition(self, offer):
        d = Dict()
        d.task_id.value = self.mesos_id
        d.agent_id.value = offer.agent_id.value
        d.name = self.definition_id()
        d.command.value = self.definition['cmd']
        d.command.environment.variables = [dict(name='HIPPO_TASK_NAME', value=self.definition['id'])]
        if self.definition.get('env'):
            d.command.environment.variables += [
                dict(name=e[0],value=e[1]) for e in self.definition['env'].items()
            ]
        d.container.type='DOCKER'
        d.container.docker.image=self.definition['container']['docker']['image']
        d.resources = [
            dict(name='cpus', type='SCALAR', scalar={'value': self.cpus()}),
            dict(name='mem', type='SCALAR', scalar={'value': self.mem()}),
        ]
        if self.definition['container'].get('volumes'):
            d.container.volumes = [
                dict(mode=v['mode'],container_path=v['containerPath'],host_path=v['hostPath']) for v in self.definition['container']['volumes']
            ]
        return d


MESOS_SYSTEM_ERRORS = ['TASK_KILLED','TASK_LOST','TASK_ERROR','TASK_DROPPED',
                       'TASK_UNREACHABLE','TASK_GONE','TASK_GONE_BY_OPERATOR',
                       'TASK_UNKNOWN']


MESOS_TASK_ERRORS = ['TASK_FAILED']


MESOS_FINAL_STATES = MESOS_TASK_ERRORS + MESOS_SYSTEM_ERRORS + ['TASK_FINISHED']


TASK_SCHEMA = {
    "id": {
        "type":"string",
        "required":True,
        "empty": False
    },
    "container": {
        "type": "dict",
        "required":True,
        "schema": {
            "docker": {
                "type":"dict",
                "required":True,
                "schema": {
                    "image" : {
                        "type":"string",
                        "required":True,
                        "empty": False
                    },
                    "network": {
                        "type":"string",
                        "allowed":["BRIDGE","HOST"]
                    }
                }
            },
            "volumes": {
                "type":"list",
                "schema":{
                    "type":"dict",
                    "schema": {
                        "hostPath": {
                            "type":"string",
                            "required":True
                        },
                        "containerPath": {
                            "type":"string",
                            "required":True
                        },
                        "mode": {
                            "type":"string",
                            "required":True,
                            "allowed":["RO","RW"]
                        }
                    }
                }
            }
        }
    },
    "mem": {
        "type":"integer",
        "min":4,
        "max":64000,
        "required":True
    },
    "cpus": {
        "type":"float",
        "min":0.01,
        "max":12.0,
        "required":True
    },
    "cmd": {
        "type":"string",
        "required":True
    },
    "env": {
        "type":"dict"
    },
    "constraints": {
        "type":"list",
        "schema": {
            "type":"list"
        }
    },
    "max_concurrent": {
        "type":"integer",
        "min":0,
        "max":64000,
    },
    "task_retries": {
        "type":"integer",
        "min":0,
        "max":100
    },
    "system_retries": {
        "type":"integer",
        "min":0,
        "max":100
    }
}



