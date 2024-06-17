from typing import Any

import pytest
from SPARQLWrapper import SPARQLWrapper

from cimsparql.graphdb import GraphDBClient, ServiceConfig
from cimsparql.retry_cb import RetryCallback
from cimsparql.sparql_result_json import SparqlResultJsonFactory


def test_query_name_registered_in_pre_call():
    retry_callback = RetryCallback()
    retry_callback.pre_call("# Name: some random name\nselect * where {?s ?p ?o}")
    assert retry_callback.query_name == "some random name"


class FailFirstSparqlWrapper(SPARQLWrapper):
    num_calls = 0

    def queryAndConvert(self) -> dict[str, Any]:  # noqa: N802
        self.num_calls += 1
        if self.num_calls == 1:
            raise ValueError("Something always goes wrong on the first attempt")
        return SparqlResultJsonFactory.build().model_dump(mode="json")


def test_after_callback(caplog: pytest.LogCaptureFixture):
    wrapper = FailFirstSparqlWrapper("http://some-sparql-endpint")
    client = GraphDBClient(service_cfg=ServiceConfig(num_retries=1), sparql_wrapper=wrapper)
    client.exec_query("# Name: Select everything\nselect * where {?s ?p ?o}")

    # Expect one message to contain be logged with the query name
    substrings = ["Select everything:", "Something always goes wrong"]
    assert sum(all(s in msg for s in substrings) for msg in caplog.messages) == 1
