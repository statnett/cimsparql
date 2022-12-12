import pytest
from mock import Mock

from cimsparql.model import Model
from cimsparql.templates import sparql_folder


def test_map_data_types(monkeypatch):
    def cim_init(self, *args):
        self.mapper = Mock(have_cim_version=Mock(return_value=True))
        self.client = Mock(prefixes={"cim": None})

    monkeypatch.setattr(Model, "__init__", cim_init)
    cim_model = Model()
    assert cim_model.map_data_types


def test_not_map_data_types(monkeypatch):
    def cim_init(self, *args):
        self.mapper = Mock(have_cim_version=Mock(return_value=False))
        self.client = Mock(prefixes={"cim": None})

    monkeypatch.setattr(Model, "__init__", cim_init)
    cim_model = Model()
    assert not cim_model.map_data_types


@pytest.mark.parametrize("sparql_query", sparql_folder.glob("*.sparql"))
def test_name_in_header(sparql_query):
    with open(sparql_query, "r") as infile:
        line = infile.readline()
    assert line.startswith("# Name: ")


def test_unique_headers():
    headers = []
    for f in sparql_folder.glob("*.sparql"):
        with open(f, "r") as infile:
            headers.append(infile.readline())

    assert len(headers) == len(set(headers))
