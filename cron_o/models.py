from copy import copy
from typing import Union, Dict

import msgpack

from cron_o.dao import OneKeyObject, StreamObject
from cron_o.time_utils import is_due


class ScheduledCall(OneKeyObject, StreamObject):
    call_id: bytes
    queue_id: bytes
    call_timestamp_millis: int
    call_params: Dict[str, Union[str, int]]

    def __init__(self, call_id: bytes = 0):
        self.call_id = call_id
        self.call_timestamp_millis: int = 0
        self.call_action: int = 0
        self.queue_id = None
        self.call_params: Dict[str, Union[str, int]] = {}

    def get_object_key(self) -> str:
        return f"call:{self.call_id}"

    def is_due(self):
        return is_due(self.call_timestamp_millis)

    def from_dict(self, input_dict: Dict):
        super().from_dict(input_dict)
        self.call_params = msgpack.unpackb(input_dict[b"call_params"])
        self.call_timestamp_millis = int(self.call_timestamp_millis)
        return self

    def to_dict(self):
        dict = copy(vars(self))
        dict["call_params"] = msgpack.packb(self.call_params)
        return dict
