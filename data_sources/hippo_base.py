import time
import base64


class HippoDataSource(object):
    def __init__(self, hippo_queue, working_count, task_class, hippo_redis):
        self.hippo_queue = hippo_queue
        self.hippo_redis = hippo_redis
        self.working_count = working_count
        self.definition = self.hippo_queue.definition
        self.last_run_tstamp = self.definition['queue'].get('last_run_tstamp')
        self.frequency_seconds = self.definition['queue'].get('frequency_seconds',1)
        self.max_concurrent = self.definition['queue'].get('max_concurrent',self.definition.get('max_concurrent',10000))
        self.batch_size = self.definition['queue'].get('batch_size',1)
        self.batch_separator = self.definition['queue'].get('batch_separator','|')
        self.new_task_limit = self.max_concurrent * self.batch_size - working_count
        self.task_class = task_class

    def too_soon(self):
        cur_tstamp = int(time.time())
        if self.last_run_tstamp and cur_tstamp < self.last_run_tstamp + self.frequency_seconds:
            return True

    def process(self):
        # stub, this should be implemented by child classes
        pass

    def process_source(self):
        self.process()
        self.hippo_queue.definition['last_run_tstamp'] = int(time.time())
        self.hippo_queue.save()

    def create_tasks(self, items):
        chunks = [items[i:i + self.batch_size] for i in range(0, len(items), self.batch_size)]
        for batch in chunks:
            data = self.batch_separator.join(batch)
            b64data = base64.b64encode(data)
            task_def = self.definition
            del task_def['queue']
            task_def['max_concurrent'] = self.max_concurrent
            task_def['cmd'] = task_def['cmd'].replace('$HIPPO_DATA_BASE64',b64data).replace('$HIPPO_DATA',data)
            if 'env' in task_def:
                for env_name in task_def['env']:
                    task_def['env'][env_name] = task_def['env'][env_name].replace('$HIPPO_DATA_BASE64',b64data).replace('$HIPPO_DATA',data)

            task = self.task_class(definition=task_def, redis_client=self.hippo_redis)
            task.queue()