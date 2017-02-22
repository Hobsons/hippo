import uuid
import logging
from addict import Dict
from pymesos import Scheduler, encode_data
from tasks import HippoTask

TASK_CPU = 0.1
TASK_MEM = 32
EXECUTOR_CPUS = 0.1
EXECUTOR_MEM = 32

class HippoScheduler(Scheduler):

    def __init__(self, redis_client):
        self.redis = redis_client
    
    def resourceOffers(self, driver, offers):
        filters = {'refuse_seconds': 5}

        mesos_launch_tasks = []
        waiting_tasks = HippoTask.waiting_tasks(self.redis)

        for offer in offers:
            logging.debug('Offer',offer)
            cpus = self.getResource(offer.resources, 'cpus')
            mem = self.getResource(offer.resources, 'mem')
            if cpus < TASK_CPU or mem < TASK_MEM:
                continue

            task = Dict()
            task_id = str(uuid.uuid4())
            task.task_id.value = task_id
            task.agent_id.value = offer.agent_id.value
            task.name = 'task {}'.format(task_id)
            #task.executor = self.executor
            task.command.value = "echo facemar && ls -l /mnt/tmp"
            task.command.
            task.container.type='DOCKER'
            task.container.docker.image='busybox:latest'
            task.container.volumes = [
                dict(mode='RW',container_path='/mnt/tmp',host_path='/Users/mdemaray/java'),
            ]
            task.data = encode_data('Hello from task {}!'.format(task_id))

            task.resources = [
                dict(name='cpus', type='SCALAR', scalar={'value': TASK_CPU}),
                dict(name='mem', type='SCALAR', scalar={'value': TASK_MEM}),
            ]

            driver.launchTasks(offer.id, [task], filters)


    def getResource(self, res, name):
        for r in res:
            if r.name == name:
                return r.scalar.value
        return 0.0

    def statusUpdate(self, driver, update):
        logging.debug('Status update TID %s %s',
                      update.task_id.value,
                      update.state)