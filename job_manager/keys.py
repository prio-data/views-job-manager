
from operator import add
from functools import partial
from . import settings

error = partial(add, settings.REDIS_ERROR_KEY_PREFIX)
job = partial(add, settings.REDIS_JOB_KEY_PREFIX)
