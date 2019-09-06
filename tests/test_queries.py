import pytest

from mock import MagicMock, call

from cimsparql import queries


def test_region_query_empty():
    assert queries.region_query(region=None, container="") == []


def test_region_query():
    assert len(queries.region_query(region="NO", container="")) == 4


def test_connectivity_mrid_sparql():
    assert isinstance(queries.connectivity_mrid(), str)


def test_connectivity_mrid_list():
    assert isinstance(queries.connectivity_mrid(sparql=False), list)


def test_connectivity_mrid_list_length():
    assert len(queries.connectivity_mrid(sparql=False)) == 2


def test_connectivity_mrid_list_length_with_sequence_numbers():
    assert len(queries.connectivity_mrid(sparql=False, sequence_numbers=range(10))) == 10


def test_load_query_raises_value_error(monkeypatch):
    with pytest.raises(ValueError):
        queries.load_query(load_type=["non_type"], connectivity=None)


def test_load_query_raises_value_error_empty_list(monkeypatch):
    with pytest.raises(ValueError):
        queries.load_query(load_type=[], connectivity=None)


def test_load_query_conform(monkeypatch):
    _combine_statements_mock = MagicMock()
    _group_query_mock = MagicMock()

    monkeypatch.setattr(queries, "combine_statements", _combine_statements_mock)
    monkeypatch.setattr(queries, "group_query", _group_query_mock)

    queries.load_query(load_type=["ConformLoad"], connectivity=None)
    assert _combine_statements_mock.call_args_list[0] == call(
        "?mrid rdf:type cim:ConformLoad", group=False, split="\n} UNION \n {"
    )


def test_load_query_combined(monkeypatch):
    _combine_statements_mock = MagicMock()
    _group_query_mock = MagicMock()

    monkeypatch.setattr(queries, "combine_statements", _combine_statements_mock)
    monkeypatch.setattr(queries, "group_query", _group_query_mock)

    queries.load_query(load_type=["ConformLoad", "NonConformLoad"], connectivity=None)
    assert _combine_statements_mock.call_args_list[0] == call(
        "?mrid rdf:type cim:ConformLoad",
        "?mrid rdf:type cim:NonConformLoad",
        group=True,
        split="\n} UNION \n {",
    )
