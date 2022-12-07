import os
from copy import deepcopy
from pathlib import Path
from string import Template
from typing import Optional

import pytest

from cimsparql.graphdb import GraphDBClient, RestApi


def skip_rdf4j_test(rdf4j_gdb: Optional[GraphDBClient]):
    if os.getenv("CI"):
        # On CI in GitHub these tests should always run
        return False
    return rdf4j_gdb is None


@pytest.mark.parametrize(
    "query, expect",
    [
        ("select ?name where {ex:Picasso foaf:firstName ?name}", [{"name": "Pablo"}]),
        (
            "select ?city {ex:Picasso ex:homeAddress ?address . ?address ex:city ?city}",
            [{"city": "Madrid"}],
        ),
    ],
)
def test_rdf4j_picasso_data(rdf4j_gdb: Optional[GraphDBClient], query, expect):
    if skip_rdf4j_test(rdf4j_gdb):
        pytest.skip("Require access to RDF4J service")

    prefixes = Template("PREFIX ex:<${ex}>\nPREFIX foaf:<${foaf}>").substitute(rdf4j_gdb.prefixes)
    result = rdf4j_gdb.exec_query(f"{prefixes}\n{query}")
    assert result == expect


def test_rdf4j_prefixes(rdf4j_gdb: Optional[GraphDBClient]):
    if skip_rdf4j_test(rdf4j_gdb):
        pytest.skip("Require access to RDF4J service")

    assert set(rdf4j_gdb.prefixes.keys()).issuperset({"ex", "foaf"})


def test_upload_rdf_xml(rdf4j_gdb: Optional[GraphDBClient]):
    if skip_rdf4j_test(rdf4j_gdb):
        pytest.skip("Require access to RDF4J service")

    xml_file = Path(__file__).parent / "data/demo.xml"
    rdf4j_gdb.upload_rdf(xml_file, "rdf/xml")

    prefixes = Template("PREFIX rdf:<${rdf}>\nPREFIX md:<${md}>").substitute(rdf4j_gdb.prefixes)
    df = rdf4j_gdb.exec_query(f"{prefixes}\nSELECT * WHERE {{?s rdf:type md:FullModel}}")
    assert len(df) == 1


def test_get_table_default_arg(rdf4j_gdb: Optional[GraphDBClient]):
    if skip_rdf4j_test(rdf4j_gdb):
        pytest.skip("Require access to RDF4J service")

    df = rdf4j_gdb.get_table("SELECT * {?s ?o ?p}")[0]
    assert len(df) == 6


def test_namespaces(rdf4j_gdb: Optional[GraphDBClient]):
    if skip_rdf4j_test(rdf4j_gdb):
        pytest.skip("Require access to RDF4J service")

    ns = "http://mynamepace.org"
    rdf4j_gdb.set_namespace("myns", ns)
    fetched_ns = rdf4j_gdb.get_namespace("myns")
    assert ns == fetched_ns


def test_upload_with_context(rdf4j_gdb):
    if skip_rdf4j_test(rdf4j_gdb):
        pytest.skip("Require access to RDF4J service")

    xml_file = Path(__file__).parent / "data/demo.xml"
    graph = "<http://mygraph.com/demo/1/1>"
    rdf4j_gdb.upload_rdf(xml_file, "rdf/xml", {"context": graph})

    prefixes = Template("PREFIX rdf:<${rdf}>\nPREFIX md:<${md}>").substitute(rdf4j_gdb.prefixes)
    df = rdf4j_gdb.exec_query(
        f"{prefixes}\nSELECT * WHERE {{GRAPH {graph} {{?s rdf:type md:FullModel}}}}"
    )
    assert len(df) == 1


def test_direct_sparql_endpoint(rdf4j_gdb):
    if skip_rdf4j_test(rdf4j_gdb):
        pytest.skip("Require access to RDF4J service")

    service_cfg_direct = deepcopy(rdf4j_gdb.service_cfg)
    service_cfg_direct.server = rdf4j_gdb.service_cfg.url
    service_cfg_direct.rest_api = RestApi.DIRECT_SPARQL_ENDPOINT

    gdb_direct = GraphDBClient(service_cfg_direct)
    query = "SELECT * {?s ?p ?o}"
    res_direct = gdb_direct.exec_query(query)
    res_rdf4j = rdf4j_gdb.exec_query(query)
    assert res_direct == res_rdf4j
