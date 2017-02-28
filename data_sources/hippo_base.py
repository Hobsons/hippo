import time

class HippoDataSource(object):
    def __init__(self, hippo_queue):
        self.hippo_queue = hippo_queue
        self.definition = self.hippo_queue.definition
        self.last_run_tstamp = self.definition['queue'].get('last_run_tstamp')
        self.frequency_seconds = self.definition['queue'].get('frequency_seconds',1)

    def too_soon(self):
        cur_tstamp = int(time.time())
        if self.last_run_tstamp and cur_tstamp < self.last_run_tstamp + self.frequency_seconds:
            return True

    def create_tasks(self):
        self.hippo_queue.definition['last_run_tstamp'] = int(time.time())
        self.hippo_queue.save()