import copy
import json
import time
import uuid
from addict import Dict
from cerberus import Validator


class HippoTask(object):
    def __init__(self, mesos_id=None, definition=None, redis_client=None):
        self.mesos_id = mesos_id
        self.definition = definition
        self.redis = redis_client

        if not definition:
            self.load()

        if 'task_retries' not in definition:
            self.definition['task_retries'] = 0

        if 'system_retries' not in definition:
            self.definition['system_retries'] = 2

        if mesos_id is None:
            if 'mesos_id' in definition:
                self.mesos_id = definition['mesos_id']
            else:
                self.mesos_id = self.definition.get('id','hippo') + '.' + str(uuid.uuid4())
                self.definition['mesos_id'] = self.mesos_id
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
                result_objects.append(cls(definition=json.loads(tstr),redis_client=redis_client))
        return result_objects

    @classmethod
    def all_tasks(cls, redis_client):
        return cls.tasks_from_ids(redis_client.zrange('hippo:all_taskid_list',0,-1), redis_client)

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
        pipe.rem(self.mesos_id)
        pipe.execute()

    def load(self):
        body = self.redis.get(self.mesos_id)
        if not body:
            self.definition = {}
        else:
            self.definition = json.loads(body)

    def save(self):
        self.redis.set(self.mesos_id,json.dumps(self.definition))

    def queue(self):
        self.redis.lpush('hippo:waiting_taskid_list',self.mesos_id)
        self.redis.zadd('hippo:all_taskid_list',int(time.time()),self.mesos_id)

    def work(self):
        pipe = self.redis.pipeline()
        pipe.lpush('hippo:working_taskid_list',self.mesos_id)
        pipe.lrem('hippo:waiting_taskid_list',0,self.mesos_id)
        pipe.execute()

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

    def kill_complete(self):
        self.redis.lrem('hippo:kill_taskid_list',0,self.mesos_id)

    def definition_id(self):
        return self.definition.get('id','')

    def cpus(self):
        return self.definition.get('cpus',0.1)

    def mem(self):
        return self.definition.get('mem',256)

    def max_concurrent(self):
        return self.definition.get('max_concurrent',10000)

    def constraints_ok(self,offer):
        offer_attributes = {}
        for a in offer.get('attributes',[]):
            offer_attributes[a['name']] = a.get('text','')

        for constraint_tuple in self.definition.get('contraints',[]):
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
        if self.definition.get('env'):
            d.command.environment.variables = [
                dict(name=e[1],value=e[2]) for e in self.definition['env'].items()
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


TASK_SCHEMA = {
    "id": {
        "type":"string",
        "required":True
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
                        "required":True
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



