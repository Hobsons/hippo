import time
import redis
import logging
from pymesos import Scheduler, encode_data
from tasks import HippoTask


class HippoScheduler(Scheduler):
    def __init__(self, redis_client):
        self.redis = redis_client
        self.host_recent_queue = {}

    def __log_recent_queue(self,host):
        self.host_recent_queue.setdefault(host,[]).append(time.time())

    def __get_recent_queue_count(self,host):
        if host not in self.host_recent_queue:
            return 0
        five_mins_ago = time.time() - 60 * 5
        self.host_recent_queue[host] = [t for t in self.host_recent_queue[host] if t > five_mins_ago]
        return len(self.host_recent_queue[host])
    
    def resourceOffers(self, driver, offers):
        filters = {'refuse_seconds': 5}

        try:
            working_count_by_id = HippoTask.working_task_count_by_id(self.redis)

            waiting_tasks = HippoTask.waiting_tasks(self.redis)
            waiting_tasks.reverse()
        except redis.exceptions.ConnectionError:
            logging.warning('Redis Connection Error in Scheduler resourceOffers')
            for offer in offers:
                driver.launchTasks(offer.id, [], filters)
            return

        waiting_tasks = [t for t in waiting_tasks if t.max_concurrent() > working_count_by_id.get(t.definition_id(),0)]

        #logging.info("Got %d offers" % len(offers))
        #logging.info([o.hostname for o in offers])

        used_mem_by_offer_id = {}
        used_cpu_by_offer_id = {}
        this_run_host_queue_count = {}
        matched_tasks_by_offer_id = {}
        host_by_offer_id = {}

        offers = list(offers)

        for task in waiting_tasks:
            offers.sort(key=lambda x:self.__get_recent_queue_count(x.hostname))
            for offer in offers:
                if this_run_host_queue_count.get(offer.hostname,0) > 1:
                    # don't launch more than two tasks per offer
                    continue
                host_by_offer_id[offer.id.value] = offer.hostname
                cpus_available = self.getResource(offer.resources, 'cpus') - used_cpu_by_offer_id.get(offer.id.value,0)
                mem_available = self.getResource(offer.resources, 'mem') - used_mem_by_offer_id.get(offer.id.value,0)
                if (task.cpus() <= cpus_available and
                   task.mem() <= mem_available and
                   working_count_by_id.get(task.definition_id(),0) < task.max_concurrent() and
                   task.constraints_ok(offer)):
                    matched_tasks_by_offer_id.setdefault(offer.id.value,[]).append(task.mesos_launch_definition(offer))
                    task.work()
                    working_count_by_id.setdefault(task.definition_id(),0)
                    working_count_by_id[task.definition_id()] += 1
                    used_cpu_by_offer_id.setdefault(offer.id.value,0)
                    used_cpu_by_offer_id[offer.id.value] += task.cpus()
                    used_mem_by_offer_id.setdefault(offer.id.value,0)
                    used_mem_by_offer_id[offer.id.value] += task.mem()
                    this_run_host_queue_count.setdefault(offer.hostname,0)
                    this_run_host_queue_count[offer.hostname] += 1
                    self.__log_recent_queue(offer.hostname)

        for offer_id in matched_tasks_by_offer_id:
            logging.info("Launching %d tasks on offer id %s, host %s" % (len(matched_tasks_by_offer_id[offer_id]),offer_id, host_by_offer_id[offer_id]))
            driver.launchTasks({'value':offer_id}, matched_tasks_by_offer_id[offer_id], filters)

    def getResource(self, res, name):
        for r in res:
            if r.name == name:
                return r.scalar.value
        return 0.0

    def statusUpdate(self, driver, update):
        try:
            t = HippoTask(mesos_id=update.task_id.value,redis_client=self.redis)
            t.definition['mesos_state'] = update.state
            t.save()
            if update.state in ['TASK_FINISHED','TASK_FAILED','TASK_LOST','TASK_ERROR','TASK_DROPPED',
                                'TASK_KILLED','TASK_UNREACHABLE','TASK_GONE','TASK_GONE_BY_OPERATOR']:
                t.finish()
            if update.state != 'TASK_FINISHED':
                t.retry()
        except redis.exceptions.ConnectionError:
            logging.warning('Redis Connection Error in Scheduler statusUpdate')
        logging.info('Status update TID %s %s',
                      update.task_id.value,
                      update.state)

    def error(self, driver, message):
        logging.warning('MESOS Error: ' + message)
        if message == 'Framework has been removed':
            logging.warning('Clearing Saved Framework ID and Dying!')
            self.redis.delete('hippo:saved_framework_id')
            exit()


