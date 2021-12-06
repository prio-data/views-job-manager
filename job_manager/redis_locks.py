from typing import Tuple, List, Optional, Set, Dict
import re
import logging
from datetime import datetime
import aioredis

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

        self._active_connection = None

        self._host = host
        self._port = port
        self._db = db

        self._has_locked: Set[str]            = set()

        self._job_prefix: str                 = job_prefix
        self._error_prefix: str               = error_prefix

        self._error_expiry_time: int          = 400
        self._job_expiry_time: int            = 400

    async def close(self):
        """
        close
        =====

        Close the redis connection. Remember to do this!
        """
        connection = await self._connection()

        await connection.close()

    async def jobs(self) -> List[str]:
        """
        jobs
        ====

        List current jobs

        returns:
            List[str]
        """
        connection = await self._connection()

        return self._unpack_keys(await connection.keys(self._jobname("*")))

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
        connection = await self._connection()

        did_lock = await connection.set(
                self._jobname(job),
                str(datetime.now()),
                nx = True,
                ex = self._job_expiry_time)

        if (success := did_lock is not None):
            self._has_locked = self._has_locked | {job}
            logging.debug(f"Locked job {job}")
        else:
            logging.debug(f"Failed to lock {job}")
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
        connection = await self._connection()

        if job in self._has_locked and not force:
            await connection.delete(self._jobname(job))
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
        """
        error_keys
        ==========

        returns:
            List[str]: A list of currently defined error keys
        """
        connection = await self._connection()

        error_keys = await connection.keys(self._errorname("*"))
        logger.debug(f"Found {len(error_keys)} errors")
        return [self._strip_error_prefix(k.decode()) for k in error_keys]

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
            logger.debug(f"Fetching error for {e}")
            code_and_message = await self.error_code_and_message(e)
            if code_and_message is not None:
                code, message = code_and_message
                error_message = {"code": code, "message": message}
                errors[e] = error_message
            else:
                logger.debug(f"Found no error message for {e}")

        return errors

    async def clear_errors(self)-> None:
        """
        clear_errors
        ============

        Clear all error flag messages

        """
        connection = await self._connection()

        keys = await self.error_keys()

        for k in keys:
            await connection.delete(self._errorname(k))

    async def get_error(self, job: str)-> Optional[str]:
        """
        get_error
        =========

        parameters:
            job (str):     The name of the job to check error condition for

        returns:
            Optional[str]: Raw error message if exists
        """
        connection = await self._connection()
        raw_error = await connection.get(self._errorname(job))
        if raw_error:
            return raw_error.decode()
        else:
            return None

    async def error_code_and_message(self, name: str) -> Optional[Tuple[int, str]]:
        """
        error_code_and_message
        ======================

        parameters:
            name (str):                 The name of the job to check error condition for

        returns:
            Optional[Tuple[int, str]]: HTTP status code : message if error exists
        """
        raw_message = await self.get_error(name)
        if raw_message is not None:
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
        connection = await self._connection()

        logger.critical(f"Job {job} returned error {status}: {message}")
        return await connection.set(self._errorname(job), f"{status}: {message}", ex = self._error_expiry_time)

    def _jobname(self, jobname: str):
        return self._job_prefix + jobname

    def _errorname(self, errorname: str):
        return self._error_prefix + errorname

    def _strip_error_prefix(self, key: str)-> str:
        return key.replace(self._error_prefix,"")

    def _unpack_keys(self, keys):
        return [*keys]

    async def _connection(self):
        if self._active_connection is None:
            url = f"redis://{self._host}:{self._port}/{self._db}"
            logger.debug(f"Connecting to Redis at {url}")
            self._active_connection = await aioredis.Redis.from_url(url)
        return self._active_connection
