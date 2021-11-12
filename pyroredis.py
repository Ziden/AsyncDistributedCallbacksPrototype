from aioredis import Redis

JOBS_KEY = "jobs"


class Pyroredis:

    def __init__(self, redis: Redis):
        self.redis = redis
        self.timeout = 1

    def blocking_read_job(self):
        job = self.redis.blpop(JOBS_KEY, timeout=self.timeout)
