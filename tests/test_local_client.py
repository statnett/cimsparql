from pathlib import Path

import pytest

from cimsparql.local_client import InvalidResultTypeError, LocalClient, oxigraph_format


def nl_eq_path() -> Path:
    return Path(__file__).parent / "data/micro/20171002T0930Z_NL_EQ_3.xml"


@pytest.mark.parametrize("pass_bytes", [True, False])
def test_upload_rdf(pass_bytes: bool):
    data_path = nl_eq_path()
    params = {"base_iri": "http://example.com/"}
    client = LocalClient()
    if pass_bytes:
        with data_path.open("rb") as infile:
            content = infile.read()
        client.upload_rdf(content, "rdf/xml", params)
    else:
        client.upload_rdf(data_path, "rdf/xml", params)

    result = client.exec_query("SELECT (count(*) as ?num) WHERE {?s ?p ?o}")
    assert int(result.results.bindings[0]["num"].value) > 0


def test_upload_to_named_graph():
    data_path = nl_eq_path()
    client = LocalClient()
    params = {"base_iri": "http://base.com/", "graph": "http://nl-eq-graph"}
    client.upload_rdf(data_path, "rdf/xml", params)
    assert {str(node) for node in client.store.named_graphs()} == {"<http://nl-eq-graph>"}


def test_error_on_wrong_query_type():
    client = LocalClient()
    with pytest.raises(InvalidResultTypeError):
        client.exec_query("ASK {?s ?p ?o}")


def test_delete_repo_not_implemented():
    with pytest.raises(NotImplementedError):
        LocalClient().delete_repo()


def test_get_prefixes():
    assert len(LocalClient().get_prefixes()) > 0


def test_update_query():
    with pytest.raises(NotImplementedError):
        LocalClient().update_query("")


def test_namespaces_round_trip():
    client = LocalClient()
    client.set_namespace("ns", "http://namespace1")
    assert client.get_namespace("ns") == "http://namespace1"


def test_oxigraph_raises_on_unknown():
    with pytest.raises(ValueError, match="format must be one of"):
        oxigraph_format("json-ldd")
