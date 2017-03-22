import os
import time
import config
import redis
import socket
import signal
import logging
from threading import Thread
from addict import Dict
from scheduler import HippoScheduler
from tasks import HippoTask
from queues import HippoQueue
from pymesos import MesosSchedulerDriver
from kazoo.client import KazooClient
from kazoo.recipe.election import Election


def reconcile(driver, redis_client):
    # reconcile tasks every 15 minutes
    def _rcile():
        # give time for driver to connect first
        time.sleep(5)
        while True:
            try:
                running_task_ids = [dict(task_id={'value':t.mesos_id}) for t in HippoTask.working_tasks(redis_client)]
                if running_task_ids:
                    logging.info('Reconciling %d tasks' % len(running_task_ids))
                    driver.reconcileTasks(running_task_ids)
            except redis.exceptions.ConnectionError:
                logging.warning('Redis Connection Error in Reconcile Thread')
            time.sleep(60 * 15)
    t = Thread(target=_rcile,args=(),daemon=True)
    t.start()
    return t


def kill_task(driver, redis_client):
    # check for tasks to kill every 2 seconds
    def _kt():
        while True:
            try:
                kill_tasks = HippoTask.kill_tasks(redis_client)
                for t in kill_tasks:
                    logging.info('Killing task %s' % t.mesos_id)
                    driver.killTask({'value':t.mesos_id})
                    t.kill_complete()
            except redis.exceptions.ConnectionError:
                logging.warning('Redis Connection Error in Kill Task Thread')
            time.sleep(2)
    t = Thread(target=_kt,args=(),daemon=True)
    t.start()
    return t


def leader():
    logging.info('Elected as leader, starting work...')

    redis_client = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB, password=config.REDIS_PW)

    saved_framework_id = redis_client.get('hippo:saved_framework_id')

    framework = Dict()
    framework.user = 'root'
    framework.name = "Hippo"
    framework.hostname = os.getenv('HOST',socket.gethostname())
    framework.checkpoint = True
    framework.failover_timeout = 86400.0
    if saved_framework_id:
        framework.id = {"value":saved_framework_id.decode()}


    driver = MesosSchedulerDriver(
        HippoScheduler(redis_client),
        framework,
        config.MESOS_HOST if config.MESOS_HOST else config.ZK_URI,
        use_addict=True,
    )

    def signal_handler(signal, frame):
        driver.stop(failover=True)

    def run_driver_thread():
        driver.run()

    driver_thread = Thread(target=run_driver_thread, args=())
    driver_thread.start()
    logging.info('Started mesos schedule driver thread')

    signal.signal(signal.SIGINT, signal_handler)

    # reconcile will run every 15 minutes in it's own thread
    reconcile_thread = reconcile(driver, redis_client)
    logging.info('Started reconcile task thread')


    # kill task will run every 2 seconds in it's own thread to kill any tasks that need killin'
    kill_thread = kill_task(driver, redis_client)
    logging.info('Started kill task thread')


    # hippo queue will run a thread pool to monitor queues for work and create tasks
    process_queue_thread = HippoQueue.process_queues(redis_client)
    logging.info('Started queue processing thread')

    # delete any ancient tasks so that we don't have them clog things up forever
    HippoTask.cleanup_old_tasks(redis_client)

    while driver_thread.is_alive() and \
          reconcile_thread.is_alive() and \
          kill_thread.is_alive() and \
          process_queue_thread.is_alive():
        # main thread just sleeps as long as all the child threads are still running
        time.sleep(1)

        # if registered and not using a saved framework id, save this one
        if driver.framework_id and not saved_framework_id:
            saved_framework_id = driver.framework_id
            redis_client.set('hippo:saved_framework_id',saved_framework_id)
            logging.info('saving framework id ' + driver.framework_id)

    logging.info('...Exiting')
    exit(0)


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

    incomplete_config = False
    if not config.REDIS_HOST:
        incomplete_config = True
        logging.error("Redis host must be present in REDIS_HOST variable")

    if not config.ZK_URI:
        incomplete_config = True
        logging.error("Zookeeper uri must be present in ZK_URI variable")

    if not incomplete_config:
        zk = KazooClient(hosts=config.ZK_URI.replace('zk://','').replace('/mesos',''))
        leader_election = None

        def zk_listen(state):
            from kazoo.protocol.states import KazooState
            logging.info('zkstate '+ str(state))
            global leader_election
            if state in [KazooState.SUSPENDED, KazooState.LOST] and leader_election is not None and not leader_election.lock.is_acquired:
                logging.info('lost zk connection while waiting to be leader, exiting')
                leader_election.cancel()

        zk.add_listener(zk_listen)
        zk.start()

        hostname = os.getenv('HOST',socket.gethostname())
        leader_election = Election(zk,'hippoleader',hostname)
        logging.info('Contending to be the hippo leader...')
        logging.info('contenders: ' + str(leader_election.contenders()))

        leader_election.run(leader)

    logging.info('Exiting main thread!')
