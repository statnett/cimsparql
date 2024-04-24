import os

import pytest
import t_utils.custom_models as t_custom

from cimsparql.graphdb import GraphDBClient


@pytest.fixture
def gdbc() -> GraphDBClient:
    if not (os.getenv("GRAPHDB_SERVER") and os.getenv("GRAPHDB_TOKEN")):
        pytest.skip("Need graphdb server to run")
    return GraphDBClient(t_custom.combined_graphdb_service())


def test_get_prefixes(gdbc: GraphDBClient):
    for prefix in ["rdf", "cim", "SN"]:
        assert prefix in gdbc.prefixes


def test_prefix_ns(gdbc: GraphDBClient):
    ns = gdbc.prefixes
    assert ns["rdf"] == "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    assert ns["cim"] == "http://iec.ch/TC57/2013/CIM-schema-cim16#"
