from dataclasses import dataclass
from typing import Union, Dict
from uuid import UUID

import msgpack


class ScheduledCall:
    call_id: UUID
    call_timestamp_millis: int
    call_action: int
    call_params: Dict[str, Union[str, int]]

    def __init__(self):
        self.call_id = None
        self.call_timestamp_millis: int = 0
        self.call_action: int = 0
        self.call_params: Dict[str, Union[str, int]] = {}

    def pack(self) -> bytes:
        data = {}
        data.update(vars(self))
        data["call_id"] = data["call_id"].bytes
        return msgpack.packb(data)

    @classmethod
    def unpack(cls, bytes: bytes):
        instance = cls()
        data = msgpack.unpackb(bytes)
        vars(instance).update(data)
        instance.call_id = UUID(instance.call_id)
        return instance
