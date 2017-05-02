import time
import datetime
import logging
import cronex
from data_sources.hippo_base import HippoDataSource


class CronQueue(HippoDataSource):
    namespace = 'cron'
    label = 'Cron Scheduler'
    inputs = {
        'cronstring': {'input':'text','label':'Cron String (UTC)','default':'* * * * *'},
        'maxbacklog': {'input':'number','label':'Maximum Job Backlog','default':1}
    }

    def __init__(self, *args):
        super().__init__(*args, namespace=CronQueue.namespace, inputs=CronQueue.inputs)

    def process(self):
        if self.last_task_queued_tstamp and time.time() - self.last_task_queued_tstamp < 60:
            # finest granularity is one minute
            return
        cur_tstamp = int(time.time())
        dt = datetime.datetime.fromtimestamp(cur_tstamp)
        cur_dt_tuple = (dt.year,dt.month,dt.day,dt.hour,dt.minute)
        cex = cronex.CronExpression(self.cronstring)
        needs_to_run = cex.check_trigger(cur_dt_tuple)

        if not needs_to_run and self.last_task_queued_tstamp and cur_tstamp - self.last_task_queued_tstamp < 86400 * 3:
            # make sure we didn't miss last run if last queuing was less than three days ago
            s_tstamp = self.last_task_queued_tstamp + 60
            while s_tstamp < cur_tstamp:
                last_qdt = datetime.datetime.fromtimestamp(int(s_tstamp))
                last_qdt_tuple = (last_qdt.year,last_qdt.month,last_qdt.day,last_qdt.hour,last_qdt.minute)
                if cex.check_trigger(last_qdt_tuple):
                    needs_to_run = True
                    break
                s_tstamp += 60

        if needs_to_run:
            self.create_tasks([str(cur_tstamp)])
