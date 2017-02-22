import os
import sys
import time
import socket
import signal
import logging
from threading import Thread
from os.path import abspath, join, dirname
from addict import Dict
from scheduler import HippoScheduler
from pymesos import MesosSchedulerDriver




def main(master):

    framework = Dict()
    framework.user = 'root'
    framework.name = "Hippo"
    framework.hostname = os.getenv('HOST',socket.gethostname())

    driver = MesosSchedulerDriver(
        HippoScheduler(),
        framework,
        master,
        use_addict=True,
    )

    def signal_handler(signal, frame):
        driver.stop()

    def run_driver_thread():
        driver.run()

    driver_thread = Thread(target=run_driver_thread, args=())
    driver_thread.start()

    print('Scheduler running, Ctrl+C to quit.')
    signal.signal(signal.SIGINT, signal_handler)

    while driver_thread.is_alive():
        time.sleep(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    if len(sys.argv) != 2:
        print("Usage: {} <mesos_master>".format(sys.argv[0]))
        sys.exit(1)
    else:
        main(sys.argv[1])