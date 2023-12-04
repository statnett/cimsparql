from __future__ import annotations

from collections.abc import Callable, Coroutine
from http import HTTPStatus
from typing import Any

import httpx
import tenacity
from SPARQLWrapper import JSON, SPARQLWrapper
from SPARQLWrapper.SPARQLExceptions import (
    EndPointInternalError,
    EndPointNotFound,
    QueryBadFormed,
    Unauthorized,
    URITooLong,
)

exceptions = {
    400: QueryBadFormed,
    401: Unauthorized,
    404: EndPointNotFound,
    414: URITooLong,
    500: EndPointInternalError,
}

http_task = Coroutine[Any, Any, httpx.Response]


async def retry_task(
    task_generator: Callable[[], http_task], num_retries: int, max_delay_seconds: int
) -> http_task.Response:
    async for attempt in tenacity.AsyncRetrying(
        stop=tenacity.stop_after_attempt(num_retries),
        wait=tenacity.wait_exponential(max=max_delay_seconds),
    ):
        with attempt:
            resp = await task_generator()
            resp.raise_for_status()
    return resp


class AsyncSparqlWrapper(SPARQLWrapper):
    def __init__(
        self,
        *args: Any,
        ca_bundle: str | None = None,
        num_retries: int = 0,
        max_delay_seconds: int = 60,
        **kwargs: dict[str, str | None],
    ) -> None:
        super().__init__(*args, **kwargs)
        self.ca_bundle = ca_bundle
        self.num_retries = num_retries
        self.max_delay_seconds = max_delay_seconds

    async def queryAndConvert(self) -> dict:  # noqa N802
        if self.returnFormat != JSON:
            raise NotImplementedError("Async client only support JSON return format")

        request = self._createRequest()
        url = request.get_full_url()
        method = request.get_method()

        kwargs = {"verify": self.ca_bundle} if self.ca_bundle else {}
        async with httpx.AsyncClient(timeout=self.timeout, **kwargs) as client:
            response = await retry_task(
                lambda: client.request(method, url, headers=request.headers, content=request.data),
                self.num_retries,
                self.max_delay_seconds,
            )

        status = response.status_code
        if status != HTTPStatus.OK:
            raise exceptions.get(status, Exception)(response.content)

        return response.json()
