import os
from pathlib import Path

import pytest


def skip_rdf4j_test(rdf4j_gdb):
    if os.getenv("CI"):
        # On CI in GitHub these tests should always run
        return False
    return rdf4j_gdb is None


@pytest.mark.parametrize(
    "query, expect",
    [
        (r"select ?name where {ex:Picasso foaf:firstName ?name}", [{"name": "Pablo"}]),
        (
            r"select ?city {ex:Picasso ex:homeAddress ?address . ?address ex:city ?city}",
            [{"city": "Madrid"}],
        ),
    ],
)
def test_rdf4j_picasso_data(rdf4j_gdb, query, expect):
    if skip_rdf4j_test(rdf4j_gdb):
        pytest.skip("Require access to RDF4J service")

    result = rdf4j_gdb.exec_query(query)
    assert result == expect


def test_rdf4j_prefixes(rdf4j_gdb):
    if skip_rdf4j_test(rdf4j_gdb):
        pytest.skip("Require access to RDF4J service")

    assert set(rdf4j_gdb.prefixes.prefixes.keys()) == {"ex", "foaf"}


def test_upload_rdf_xml(rdf4j_gdb):
    if skip_rdf4j_test(rdf4j_gdb):
        pytest.skip("Require access to RDF4J service")

    xml_file = Path(__file__).parent / "data/demo.xml"
    rdf4j_gdb.upload_rdf(xml_file, "rdf/xml")

    df = rdf4j_gdb.exec_query("SELECT * WHERE {?s rdf:type md:FullModel}")
    assert len(df) == 1


def test_get_table_default_arg(rdf4j_gdb):
    if skip_rdf4j_test(rdf4j_gdb):
        pytest.skip("Require access to RDF4J service")

    df = rdf4j_gdb.get_table("SELECT * {?s ?o ?p}")[0]
    assert len(df) == 6


def test_namespaces(rdf4j_gdb):
    if skip_rdf4j_test(rdf4j_gdb):
        pytest.skip("Require access to RDF4J service")

    ns = "http://mynamepace.org"
    rdf4j_gdb.set_namespace("myns", ns)
    fetched_ns = rdf4j_gdb.get_namespace("myns")
    assert ns == fetched_ns
