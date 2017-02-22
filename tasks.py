import json
import uuid
from cerberus import Validator


class HippoTask(object):
    def __init__(self, id=None, definition=None, redis_client=None):
        self.id = id
        self.definition = definition
        self.redis = redis_client
        if id is None:
            self.id = self.definition.get('mesos_id','hippo') + '.' + str(uuid.uuid4())
        elif definition is None:
            self.load()
        self.definition['mesos_id'] = self.id

    @classmethod
    def all_tasks(cls, redis_client):
        task_ids = redis_client.zrange('hippo:all_taskid_list',0,-1)
        if not task_ids:
            task_ids = []
        return [cls(id=tid,redis_client=redis_client) for tid in task_ids]

    @classmethod
    def waiting_tasks(cls, redis_client):
        task_ids = redis_client.lrange('hippo:waiting_taskid_list',0,-1)
        return [cls(id=tid,redis_client=redis_client) for tid in task_ids]

    def delete(self):
        self.redis.lrem('hippo:waiting_taskid_list',self.id)
        self.redis.zrem('hippo:all_taskid_list',self.id)

    def load(self):
        body = self.redis.get(self.id)
        if not body:
            self.definition = {}
        else:
            self.definition = json.loads(body)

    def save(self):
        self.redis.set(self.id,json.dumps(self.definition))

    def work(self):
        self.redis.lpush('hippo:waiting_taskid_list',self.id)

    def validate(self):
        v = Validator(TASK_SCHEMA, allow_unknown=True)
        valid = v.validate(self.definition)
        if valid:
            return None
        return str(v.errors)


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
        "type":"list",
        "schema":{
            "type":"dict"
        }
    },
    "constraints": {
        "type":"list",
        "schema": {
            "type":"list"
        }
    }
}



