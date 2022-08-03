import os

import pytest

from cimsparql.graphdb import GraphDBClient, ServiceConfig
from cimsparql.url import Prefix

prefixes = [
    "rdf",
    "alg",
    "fn",
    "sesame",
    "ALG",
    "owl",
    "cim",
    "SN",
    "pti",
    "md",
    "ENTSOE",
    "entsoe2",
    "wgs",
    "gn",
    "xsd",
    "rdfs",
]


def test_set_cim_version():
    pre = Prefix({})
    for nr in range(10):
        pre.prefixes = {"cim": f"CIM-schema-cim{nr}#"}
        assert pre.cim_version == nr


@pytest.fixture
def gdbc(graphdb_service: ServiceConfig) -> GraphDBClient:
    return GraphDBClient(graphdb_service)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_get_prefixes(gdbc: GraphDBClient):
    for prefix in ["rdf", "cim", "SN"]:
        assert prefix in gdbc.prefixes.prefixes


def test_header_str_missing_prefixes():
    pre = Prefix({})
    assert pre.header_str("") == ""


def test_header_str():
    pre = Prefix({"cim": "cim_url#", "sn": "sn_url#", "ALG": "alg_url#"})
    ref_prefix = {"PREFIX cim:<cim_url#>", "PREFIX sn:<sn_url#>"}
    assert set(pre.header_str("cim:Var sn:Var").split("\n")) == ref_prefix


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_prefix_ns(gdbc: GraphDBClient):
    ns = gdbc.prefixes.ns
    assert ns["rdf"] == "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    assert ns["cim"] == "http://iec.ch/TC57/2010/CIM-schema-cim15#"


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_prefix_inverse(gdbc: GraphDBClient):
    cim_inv = gdbc.prefixes.inverse_ns
    assert isinstance(cim_inv, dict)
    assert set(cim_inv.values()).difference(prefixes) == set()
