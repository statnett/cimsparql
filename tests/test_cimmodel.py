from http import HTTPStatus
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
import werkzeug
from pytest_httpserver import HTTPServer

from cimsparql.adaptions import is_uuid
from cimsparql.graphdb import GraphDBClient, RestApi, ServiceConfig
from cimsparql.model import Model, ModelConfig, get_federated_cim_model, query_name
from cimsparql.sparql_result_json import SparqlResultJsonFactory
from cimsparql.templates import sparql_folder
from cimsparql.type_mapper import TypeMapper


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


class LocalTypeMapper(TypeMapper):
    def get_map(self) -> dict[str, Any]:
        return {}


class CorrelationIdPicker:
    def __init__(self) -> None:
        self.correlation_id = None

    def extract_correlation_id(self, request: werkzeug.Request) -> werkzeug.Response:
        self.correlation_id = request.headers.get(GraphDBClient.x_correlation_id)
        result = SparqlResultJsonFactory.build().model_dump_json()
        return werkzeug.Response(result, status=HTTPStatus.OK, mimetype="application/json")


def test_correlation_id(httpserver: HTTPServer):
    correlation_picker = CorrelationIdPicker()
    httpserver.expect_request("/sparql").respond_with_handler(
        correlation_picker.extract_correlation_id
    )
    config = ServiceConfig(
        server=httpserver.url_for("/sparql"), rest_api=RestApi.DIRECT_SPARQL_ENDPOINT
    )

    model = Model(
        {"name1": GraphDBClient(config), "name2": GraphDBClient(config)},
        mapper=LocalTypeMapper(config),
    )
    query1 = "# Name: name1\nselect * {?s ?p ?o}"
    query2 = "# Name: name2\nselect * {?s ?p ?o}"

    model.get_table_and_convert(query1)  # Runs from first client
    assert correlation_picker.correlation_id is None
    model.get_table_and_convert(query2)  # Runs from second client
    assert correlation_picker.correlation_id is None

    # Run from within context
    with model:
        model.get_table_and_convert(query1)  # Runs from first client
        assert is_uuid(correlation_picker.correlation_id)

        c_id = correlation_picker.correlation_id
        model.get_table_and_convert(
            query2
        )  # Runs from second client (but should have same correlation id)
        assert correlation_picker.correlation_id == c_id

    # Run again and confirm that correlation id is removed
    model.get_table_and_convert(query1)  # Runs from first client
    assert correlation_picker.correlation_id is None
    model.get_table_and_convert(query2)  # Runs from second client
    assert correlation_picker.correlation_id is None
