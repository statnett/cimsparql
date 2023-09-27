from __future__ import annotations

from http import HTTPStatus
from typing import Any

import httpx
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


class AsyncSparqlWrapper(SPARQLWrapper):
    def __init__(self, *args: Any, **kwargs: dict[str, str | None]) -> None:
        self.ca_bundle: str | None = kwargs.pop("ca_bundle", None)
        super().__init__(*args, **kwargs)

    async def queryAndConvert(self) -> dict:  # noqa N802
        if self.returnFormat != JSON:
            raise NotImplementedError("Async client only support JSON return format")

        request = self._createRequest()
        url = request.get_full_url()
        method = request.get_method()

        kwargs = {"verify": self.ca_bundle} if self.ca_bundle else {}
        async with httpx.AsyncClient(timeout=self.timeout, **kwargs) as client:
            response = await client.request(
                method, url, headers=request.headers, content=request.data
            )

        status = response.status_code
        if status != HTTPStatus.OK:
            raise exceptions.get(status, Exception)(response.content)

        return response.json()
