from enum import Enum


class StatusCode(Enum):
    SUCCESS = 200
    FORBIDDEN = 403
    TOO_MANY_REQUESTS = 429
