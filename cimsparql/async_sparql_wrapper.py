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
    async def queryAndConvert(self) -> dict:
        if self.returnFormat != JSON:
            raise NotImplementedError("Async client only support JSON return format")

        request = self._createRequest()
        url = request.get_full_url()
        method = request.get_method()

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.request(method, url, headers=request.headers, data=request.data)

        status = response.status_code
        if status != 200:
            raise exceptions.get(status, Exception)(response.content)

        return response.json()
