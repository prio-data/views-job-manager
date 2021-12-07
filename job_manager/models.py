
import datetime
from pydantic import BaseModel, validator

class Error(BaseModel):
    _RETRYABLE_CODES = [
            503
        ]

    http_status_code: int
    message:          str
    posted_at:        datetime.datetime
    retries:          int = 0

    @property
    def retryable(self):
        return self.http_status_code in self._RETRYABLE_CODES

    """
    @validator("http_status_code")
    def _code_is_error(self, v):
        assert str(v)[0] == "5"
        return v
        """
