import os

REDIS_HOST = os.getenv('REDIS_HOST')
if ':' in REDIS_HOST:
    REDIS_HOST, REDIS_PORT = REDIS_HOST.split(':')
else:
    REDIS_PORT = int(os.getenv('REDIS_PORT',6379))
REDIS_DB = int(os.getenv('REDIS_DB',0))
REDIS_PW = os.getenv('REDIS_PW')


ZK_URI = os.getenv('ZK_URI')

