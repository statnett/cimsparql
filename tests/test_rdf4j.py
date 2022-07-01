import os

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

    result = rdf4j_gdb.client.exec_query(query)
    assert result == expect


def test_rdf4j_prefixes(rdf4j_gdb):
    if skip_rdf4j_test(rdf4j_gdb):
        pytest.skip("Require access to RDF4J service")

    assert set(rdf4j_gdb.client.prefixes.prefixes.keys()) == {"ex", "foaf"}
