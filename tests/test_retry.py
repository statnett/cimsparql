import json
from contextlib import AbstractContextManager, nullcontext
from http import HTTPStatus

import pytest
import tenacity
from pytest_httpserver import HTTPServer
from SPARQLWrapper import JSON
from werkzeug import Request, Response

from cimsparql.async_sparql_wrapper import AsyncSparqlWrapper
from cimsparql.graphdb import GraphDBClient, RestApi, ServiceConfig
from cimsparql.sparql_result_json import SparqlResultJsonFactory


class FailFirstOkSecondHandler:
    def __init__(self) -> None:
        self.num_calls = 0

    def request_handler(self, _: Request) -> Response:
        self.num_calls += 1
        if self.num_calls == 1:
            return Response(status=HTTPStatus.NOT_FOUND)
        result = SparqlResultJsonFactory.build()
        return Response(
            json.dumps(result.model_dump(mode="json", by_alias=True)),
            status=HTTPStatus.OK,
            mimetype="application/json",
        )


def fail_first_ok_second_server(httpserver: HTTPServer) -> str:
    handler = FailFirstOkSecondHandler()
    httpserver.expect_request("/sparql").respond_with_handler(handler.request_handler)
    return httpserver.url_for("/sparql")


@pytest.mark.asyncio
@pytest.mark.parametrize("validate", [True, False])
@pytest.mark.parametrize(
    "num_retries,exception_context",
    [
        (0, pytest.raises(tenacity.RetryError)),  # Expect error because no retry
        (1, nullcontext()),  # Expect no error because we have retry
    ],
)
async def test_retry_async(
    httpserver: HTTPServer,
    num_retries: int,
    exception_context: AbstractContextManager,
    validate: bool,
):
    url = fail_first_ok_second_server(httpserver)
    sparql_wrapper = AsyncSparqlWrapper(url, num_retries=num_retries, validate=validate)
    sparql_wrapper.setReturnFormat(JSON)
    with exception_context:
        await sparql_wrapper.query_and_convert()


@pytest.mark.parametrize(
    "num_retries,exception_context", [(0, pytest.raises(tenacity.RetryError)), (1, nullcontext())]
)
def test_retry_sync(
    httpserver: HTTPServer, num_retries: int, exception_context: AbstractContextManager
):
    url = fail_first_ok_second_server(httpserver)

    config = ServiceConfig(
        server=url, rest_api=RestApi.DIRECT_SPARQL_ENDPOINT, num_retries=num_retries
    )

    client = GraphDBClient(config)

    with exception_context:
        client._exec_query("select * where {?s ?p ?o}")