import os
import hashlib

REDIS_HOST = os.getenv('REDIS_HOST','127.0.0.1')
if ':' in REDIS_HOST:
    REDIS_HOST, REDIS_PORT = REDIS_HOST.split(':')
else:
    REDIS_PORT = int(os.getenv('REDIS_PORT',6379))
REDIS_DB = int(os.getenv('REDIS_DB',0))
REDIS_PW = os.getenv('REDIS_PW')


ZK_URI = os.getenv('ZK_URI')

MESOS_HOST = os.getenv('MESOS_HOST')

NUM_QUEUE_POLL_WORKERS = int(os.getenv('NUM_QUEUE_POLL_WORKERS',8))

# will keep completed tasks for a maximum of TASK_RETENTION_SECONDS
TASK_RETENTION_SECONDS = int(os.getenv('TASK_RETENTION_SECONDS',86400))

# will keep maximum of TASK_RETENTION_COUNT completed tasks for each id
TASK_RETENTION_COUNT = int(os.getenv('TASK_RETENTION_COUNT',100))

AES_CIPHER_KEY = os.getenv('AES_CIPHER_KEY','SOMECIPHER99993u3nfnnslfeiwjf3rjj3jsnefjnw3f')
HASHED_KEY = hashlib.sha256(AES_CIPHER_KEY.encode()).digest()