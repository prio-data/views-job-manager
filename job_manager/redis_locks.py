import asyncio
from typing import Tuple, List, Optional, Set, Dict
import re
import logging
from datetime import datetime
import asyncio_redis

logger = logging.getLogger(__name__)

class RedisLocks():
    """
    RedisLocks
    ==========

    parameters:
        host (str):         Redis hostname
        port (int):         Redis port
        db (int):           Redis DB

        error_prefix (str): Key prefix to add to error entries
        job_prefix (str):   Key prefix to add to job entries
    """

    def __init__(self,
            host: str,
            port: int,
            db: int,
            error_prefix: str = "jobman/errors:",
            job_prefix: str = "jobman/jobs:"):

        self._connection: asyncio_redis.Pool = asyncio.run(self._connect())

        self._host: str                       = host
        self._port: int                       = port
        self._db: int                         = db

        self._has_locked: Set[str]            = set()

        self._job_prefix: str                 = job_prefix
        self._error_prefix: str               = error_prefix

        self._error_expiry_time: int          = 400
        self._job_expiry_time: int            = 400


    def close(self):
        """
        close
        =====

        Close the redis connection. Remember to do this!
        """
        self._connection.close()

    async def jobs(self) -> List[str]:
        """
        jobs
        ====

        List current jobs

        returns:
            List[str]
        """

        return self._unpack_keys(await self._connection.keys(self._jobname("*")))

    async def lock(self, job)-> bool:
        """
        lock
        ====

        Try to lock a job

        parameters:
            job (str): The name of the job to lock

        returns:
            bool:      Successfully aquired lock?

        """
        did_lock = await self._connection.set(
                self._jobname(job),
                str(datetime.now()),
                only_if_not_exists = True,
                expire = self._job_expiry_time)

        if (success := did_lock is not None):
            self._has_locked = self._has_locked | {job}
        return success

    async def unlock(self, job: str, force: bool = False)-> bool:
        """
        unlock
        ======

        parameters:
            job (str):    Name of the job to unlock
            force (bool): Unlock the job even if the job wasn't created with this client.

        Unlock a job
        Only works if the job was created by this client, unless force parameter is passed.
        """
        if job in self._has_locked and not force:
            await self._connection.delete([self._jobname(job)])
            self._has_locked = self._has_locked - {job}
            return True
        else:
            return False

    async def cleanup(self):
        """
        cleanup
        =======

        Unlock all jobs that have been locked with this client.
        """
        for job in self._has_locked:
            await self.unlock(job)

    async def error_keys(self)-> List[str]:
        error_keys = await self._connection.keys(self._errorname("*"))
        return [*error_keys]

    async def errors(self)-> Dict[str, Dict[str, str]]:
        """
        errors
        ====

        Returns all current errors in the format [{"code": $HTTP_CODE, "message": $ERROR_MESSAGE}, ...]

        returns:
            Dict[str, Dict[str, str]]
        """
        error_keys = await self.error_keys()

        errors: Dict[str, Dict[str, str]] = {}
        for e in error_keys:
            code_and_message = await self.error_code_and_message(e)
            if code_and_message is not None:
                code, message = code_and_message
                error_message = {"code": code, "message": message}
                errors[e] = error_message
            else:
                pass

        return errors

    async def clear_errors(self)-> None:
        """
        clear_errors
        ============

        Clear all error flag messages

        """
        keys = await self.error_keys()
        await self._connection.delete(keys)

    async def get_error(self, job: str)-> Optional[str]:
        """
        get_error
        =========

        parameters:
            job (str):     The name of the job to check error condition for

        returns:
            Optional[str]: Raw error message if exists
        """
        return self._connection.get(self._errorname(job))

    async def error_code_and_message(self, job: str) -> Optional[Tuple[int, str]]:
        """
        error_code_and_message
        ======================

        parameters:
            job (str):                 The name of the job to check error condition for

        returns:
            Optional[Tuple[int, str]]: HTTP status code : message if error exists
        """
        if (raw_message := await self.get_error(job)):
            if (code_search := re.search("[0-9]{3}", raw_message)) is not None:
                http_code = int(code_search[0])
            else:
                http_code = 500

            try:
                _, message = raw_message.split(":")
            except ValueError:
                message = raw_message
            return http_code, message

        else:
            return None

    async def set_error(self, job: str, status: int, message: str)-> None:
        """
        set_error
        =========

        parameters:
            job (str):     The name of the job
            status (int):  The HTTP error code to associate with the error flag
            message (str): The message to post with the error flag

        Flag an error condition for a job

        """
        logger.critical(f"Job {job} returned error {status}: {message}")
        return self._connection.set(self._errorname(job), "{status}: message", expire = self._error_expiry_time)

    async def _connect(self)-> asyncio_redis.Connection:
        return await asyncio_redis.Pool.create(host = self._host, port = self._port, db = self._db)

    def _jobname(self, jobname: str):
        return self._job_prefix + jobname

    def _errorname(self, errorname: str):
        return self._error_prefix + errorname

    def _unpack_keys(self, keys):
        return [*keys]
