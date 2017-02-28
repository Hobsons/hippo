import uuid
import logging
from pymesos import Scheduler, encode_data
from tasks import HippoTask


class HippoScheduler(Scheduler):
    def __init__(self, redis_client):
        self.redis = redis_client
    
    def resourceOffers(self, driver, offers):
        filters = {'refuse_seconds': 5}

        working_count_by_id = HippoTask.working_task_count_by_id(self.redis)

        waiting_tasks = HippoTask.waiting_tasks(self.redis)
        waiting_tasks.reverse()

        waiting_tasks = [t for t in waiting_tasks if t.max_concurrent() > working_count_by_id.get(t.definition_id(),0)]

        for offer in offers:
            logging.debug('Offer: ' + str(offer))
            cpus_available = self.getResource(offer.resources, 'cpus')
            mem_available = self.getResource(offer.resources, 'mem')

            matched_tasks = []
            for task in waiting_tasks:
                if (task.cpus() <= cpus_available and
                   task.mem() <= mem_available and
                   working_count_by_id.get(task.definition_id(),0) < task.max_concurrent() and
                   task.constraints_ok(offer)):

                    matched_tasks.append(task.mesos_launch_definition(offer))
                    task.work()
                    working_count_by_id.setdefault(task.definition_id(),0)
                    working_count_by_id[task.definition_id()] += 1
                    cpus_available -= task.cpus()
                    mem_available -= task.mem()

            driver.launchTasks(offer.id, matched_tasks, filters)

    def getResource(self, res, name):
        for r in res:
            if r.name == name:
                return r.scalar.value
        return 0.0

    def statusUpdate(self, driver, update):
        t = HippoTask(mesos_id=update.task_id.value,redis_client=self.redis)
        t.definition['mesos_status'] = update.state
        t.save()
        if update.state in ['TASK_FINISHED','TASK_FAILED','TASK_LOST','TASK_ERROR','TASK_DROPPED',
                            'TASK_KILLED','TASK_UNREACHABLE','TASK_GONE','TASK_GONE_BY_OPERATOR']:
            t.finish()
        logging.debug('Status update TID %s %s',
                      update.task_id.value,
                      update.state)
        logging.debug(str(update))