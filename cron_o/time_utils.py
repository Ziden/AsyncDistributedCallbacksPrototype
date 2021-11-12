import time


def get_current_time_millis() -> int:
    return time.time_ns() // 1000000


def is_due(timestamp: int):
    return timestamp > get_current_time_millis()