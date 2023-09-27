from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from cimsparql.graphdb import GraphDBClient
from cimsparql.model import Model, ModelConfig, get_federated_cim_model, query_name
from cimsparql.templates import sparql_folder


def test_map_data_types(monkeypatch: pytest.MonkeyPatch):
    def cim_init(self: Model, *_: Any) -> None:
        self.mapper = Mock(have_cim_version=Mock(return_value=True))
        self.clients = {"default": Mock(prefixes={"cim": None})}

    monkeypatch.setattr(Model, "__init__", cim_init)
    cim_model = Model()
    assert cim_model.map_data_types


def test_not_map_data_types(monkeypatch: pytest.MonkeyPatch):
    def cim_init(self: Model, *_: Any) -> None:
        self.mapper = Mock(have_cim_version=Mock(return_value=False))
        self.clients = {"default": Mock(prefixes={"cim": None})}

    monkeypatch.setattr(Model, "__init__", cim_init)
    cim_model = Model()
    assert not cim_model.map_data_types


@pytest.mark.parametrize("sparql_query", sparql_folder.glob("*.sparql"))
def test_name_in_header(sparql_query: Path):
    with open(sparql_query) as infile:
        line = infile.readline()
    assert line.startswith("# Name: ")


def test_unique_headers():
    headers = []
    for f in sparql_folder.glob("*.sparql"):
        with open(f) as infile:
            headers.append(infile.readline())

    assert len(headers) == len(set(headers))


def test_query_name():
    query = """# Name: name of the query
    select * {?s ?p ?o}
    """
    assert query_name(query) == "name of the query"


def test_query_without_name():
    query = "select * {?s ?p ?o}"
    assert query_name(query) == ""


def test_multi_client_model_defined_clients_exist():
    class MockTypeMapper:
        pass

    eq = GraphDBClient()
    tpsv = GraphDBClient()
    m_cfg = ModelConfig()
    model = get_federated_cim_model(eq, tpsv, m_cfg, MockTypeMapper())

    query_names = []
    for f in sparql_folder.glob("*.sparql"):
        with open(f) as infile:
            query_names.append(query_name(infile.read()))

    queries_with_client = set(model.clients.keys())

    # Verify that all queries with a special client is one of the pre-defined queries
    assert queries_with_client.issubset(query_names)
