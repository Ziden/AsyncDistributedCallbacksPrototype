import abc
from contextlib import contextmanager, asynccontextmanager
from typing import Dict, Optional, List, Union

import aioredis
from aioredis import Redis
from aioredis.client import Pipeline


class StreamObject:
    stream_id: bytes


class OneKeyObject:

    @abc.abstractmethod
    def get_object_key(self):
        pass

    @abc.abstractmethod
    def to_dict(self):
        return vars(self)

    @abc.abstractmethod
    def from_dict(self, input_dict: Dict):
        decoded = {}
        for k, v in input_dict.items():
            try:
                decoded_val = v.decode()
            except (UnicodeDecodeError, AttributeError):
                decoded[k.decode()] = v
                continue
            if decoded_val.isdigit():
                decoded_val = int(decoded_val)
            decoded[k.decode()] = decoded_val
        vars(self).update(decoded)

    def get_expiry_seconds(self):
        return 0

    async def serialize(self, io_writer: Union[Redis, Pipeline]):
        key = self.get_object_key()
        await io_writer.hmset(key, self.to_dict())
        expiry = self.get_expiry_seconds()
        if expiry:
            await io_writer.expire(key, expiry)

    async def deserialize(self, io_reader: Redis):
        self.from_dict(await io_reader.hgetall(self.get_object_key()))
        return self


class _RedisDao:
    _connection: Redis
    _pipeline: Optional[Redis]

    def __init__(self):
        self._pipeline = None
        self._connection = None

    def connect(self):
        self._connection = aioredis.Redis.from_url(
            "redis://localhost", max_connections=10, encoding='utf-8'
        )
        return self._connection

    def get_connection(self):
        return self._connection or self.connect()

    def writer(self):
        return self._pipeline or self.get_connection()

    def reader(self):
        return self.get_connection()

    def set_transaction_pipeline(self, pipe: Pipeline):
        self._pipeline = pipe

    def is_in_transaction(self):
        return self._pipeline


_dao = _RedisDao()


@asynccontextmanager
async def async_transaction(*watched_keys):
    pipe = _dao.writer().pipeline(True)
    if watched_keys:
        await pipe.watch(*watched_keys)
    pipe.multi()
    _dao.set_transaction_pipeline(pipe)
    yield
    await pipe.execute(True)
    _dao.set_transaction_pipeline(None)


@contextmanager
def require_transaction():
    if not _dao.is_in_transaction():
        raise Exception("Not in transaction")
    yield


async def connect():
    await _dao.connect()


async def wipe():
    await _dao.writer().flushdb(asynchronous=False)


def writer():
    return _dao.writer()


def reader():
    return _dao.reader()