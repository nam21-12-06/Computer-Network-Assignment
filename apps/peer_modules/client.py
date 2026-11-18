"""HTTP client giao tiep voi tracker."""

import asyncio
from functools import partial
import json
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class TrackerClientError(Exception):
    """Tracker khong tra ve ket qua thanh cong."""

    def __init__(self, message: str, status: Optional[int] = None):
        super().__init__(message)
        self.status = status


class TrackerClient:
    """HTTP client duoc dua vao thread de khong chan asyncio event loop."""

    def __init__(self, base_url: str, timeout: float = 5.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _post(self, path: str, payload: dict) -> dict:
        request = Request(
            self.base_url + path,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            raise TrackerClientError(detail, error.code) from error
        except (OSError, URLError, ValueError) as error:
            raise TrackerClientError(str(error)) from error

    async def post(self, path: str, payload: dict) -> dict:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, partial(self._post, path, payload))
