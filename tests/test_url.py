import os

import pytest

from cimsparql.graphdb import GraphDBClient, ServiceConfig


@pytest.fixture
def gdbc(graphdb_service: ServiceConfig) -> GraphDBClient:
    return GraphDBClient(graphdb_service)


@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER", None) is None, reason="Need graphdb server to run")
def test_get_prefixes(gdbc: GraphDBClient):
    for prefix in ["rdf", "cim", "SN"]:
        assert prefix in gdbc.prefixes


@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER", None) is None, reason="Need graphdb server to run")
def test_prefix_ns(gdbc: GraphDBClient):
    ns = gdbc.prefixes
    assert ns["rdf"] == "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    assert ns["cim"] == "http://iec.ch/TC57/2013/CIM-schema-cim16#"
