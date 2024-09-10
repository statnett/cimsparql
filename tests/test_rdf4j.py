from __future__ import annotations

import dataclasses
import os
from pathlib import Path
from string import Template
from typing import TYPE_CHECKING

import pytest

import tests.t_utils.common as t_common
from cimsparql.graphdb import GraphDBClient, RestApi

if TYPE_CHECKING:
    from collections.abc import Generator


def skip_rdf4j_test(rdf4j_gdb: GraphDBClient | None) -> bool:
    # On CI in GitHub these tests should always run
    return not (rdf4j_gdb or os.getenv("CI"))


@pytest.fixture(scope="module")
def rdf4j_gdb() -> GraphDBClient:
    try:
        return t_common.initialized_rdf4j_repo()
    except Exception as exc:
        if os.getenv("CI"):
            pytest.fail(f"{exc}")
        else:
            pytest.skip(f"{exc}")


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
def test_rdf4j_picasso_data(rdf4j_gdb: GraphDBClient, query: str, expect: list[dict[str, str]]):
    prefixes = Template("PREFIX ex:<${ex}>\nPREFIX foaf:<${foaf}>").substitute(rdf4j_gdb.prefixes)
    result = rdf4j_gdb.exec_query(f"{prefixes}\n{query}").results.values_as_dict()
    assert result == expect


def test_rdf4j_prefixes(rdf4j_gdb: GraphDBClient):
    assert set(rdf4j_gdb.prefixes.keys()).issuperset({"ex", "foaf"})


@pytest.fixture
def upload_client() -> Generator[GraphDBClient, None, None]:
    client = None
    try:
        yield t_common.init_repo_rdf4j(t_common.rdf4j_url(), "upload")
    except Exception as exc:
        if os.getenv("CI"):
            pytest.fail(f"{exc}")
        else:
            pytest.skip(f"{exc}")
    finally:
        if client:
            client.delete_repo()


def test_upload_rdf_xml(upload_client: GraphDBClient):
    xml_file = Path(__file__).parent / "data/demo.xml"
    upload_client.upload_rdf(xml_file, "rdf/xml")

    prefixes = Template("PREFIX rdf:<${rdf}>\nPREFIX md:<${md}>").substitute(upload_client.prefixes)
    sparql_result = upload_client.exec_query(f"{prefixes}\nSELECT * WHERE {{?s rdf:type md:FullModel}}")
    assert len(sparql_result.results.bindings) == 1


def test_get_table_default_arg(rdf4j_gdb: GraphDBClient):
    df = rdf4j_gdb.get_table("SELECT * {?s ?o ?p}")[0]
    assert len(df) == 6


def test_namespaces(rdf4j_gdb: GraphDBClient):
    ns = "http://mynamepace.org"
    rdf4j_gdb.set_namespace("myns", ns)
    fetched_ns = rdf4j_gdb.get_namespace("myns")
    assert ns == fetched_ns


def test_upload_with_context(upload_client: GraphDBClient):
    xml_file = Path(__file__).parent / "data/demo.xml"
    graph = "<http://mygraph.com/demo/1/1>"
    upload_client.upload_rdf(xml_file, "rdf/xml", {"context": graph})

    prefixes = Template("PREFIX rdf:<${rdf}>\nPREFIX md:<${md}>").substitute(upload_client.prefixes)
    sparql_result = upload_client.exec_query(
        f"{prefixes}\nSELECT * WHERE {{GRAPH {graph} {{?s rdf:type md:FullModel}}}}"
    )
    assert len(sparql_result.results.bindings) == 1


def test_direct_sparql_endpoint(rdf4j_gdb: GraphDBClient):
    service_cfg_direct = dataclasses.replace(
        rdf4j_gdb.service_cfg,
        server=rdf4j_gdb.service_cfg.url,
        rest_api=RestApi.DIRECT_SPARQL_ENDPOINT,
    )

    gdb_direct = GraphDBClient(service_cfg_direct)
    query = "SELECT * {?s ?p ?o}"
    res_direct = gdb_direct.exec_query(query)
    res_rdf4j = rdf4j_gdb.exec_query(query)
    assert res_direct == res_rdf4j
