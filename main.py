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
from pymesos import MesosSchedulerDriver
from kazoo.client import KazooClient
from kazoo.recipe.election import Election


def leader():
    logging.debug('Elected as leader, starting work...')

    redis_client = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB, password=config.REDIS_PW)

    framework = Dict()
    framework.user = 'root'
    framework.name = "Hippo"
    framework.hostname = os.getenv('HOST',socket.gethostname())

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

    signal.signal(signal.SIGINT, signal_handler)

    running_task_ids = [dict(task_id=t.id()) for t in HippoTask.working_tasks(redis_client=redis_client)]
    if running_task_ids:
        logging.debug('Reconciling %d tasks' % len(running_task_ids))
        driver.reconcileTasks(running_task_ids)

    while driver_thread.is_alive():
        time.sleep(1)

    logging.debug('...Exiting')


if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

    incomplete_config = False
    if not config.REDIS_HOST:
        incomplete_config = True
        logging.error("Redis host must be present in REDIS_HOST variable")

    if not config.ZK_URI:
        incomplete_config = True
        logging.error("Zookeeper uri must be present in ZK_URI variable")

    if not incomplete_config:
        zk = KazooClient(hosts=config.ZK_URI.replace('zk://','').replace('/mesos',''))
        zk.start()

        hostname = os.getenv('HOST',socket.gethostname())

        leader_election = Election(zk,'hippoleader',hostname)
        logging.debug('Contending to be the hippo leader...')
        logging.debug('contenders: ' + str(leader_election.contenders()))
        leader_election.run(leader)
