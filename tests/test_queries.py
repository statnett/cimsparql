import pytest

from mock import MagicMock, call

from cimsparql import queries


def test_region_query_empty():
    assert queries.region_query(region=None, sub_region=False, container="") == []


def test_region_query():
    assert len(queries.region_query(region="NO", sub_region=False, container="")) == 4


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


def test_load_query_no_connectivity():
    assert "connectivity_mrid" not in queries.load_query(
        load_type=["ConformLoad"], connectivity=None
    )


def test_load_query_with_connectivity():
    assert "connectivity_mrid" in queries.load_query(load_type=["ConformLoad"])


def test_load_query_with_region():
    assert "SubGeographicalRegion" in queries.load_query(load_type=["ConformLoad"])


def test_load_query_with_no_region():
    assert "SubGeographicalRegion" not in queries.load_query(load_type=["ConformLoad"], region=None)


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


def test_default_terminal_where_query():
    assert len(queries.terminal_where_query()) == 3


def test_terminal_where_query_no_var():
    assert len(queries.terminal_where_query(var=None)) == 2


def test_terminal_where_query_no_var_with_sequence():
    assert len(queries.terminal_where_query(cim_version=15, var=None, with_sequence_number=1)) == 3


def test_bus_data_default():
    assert "Substation" in queries.bus_data()


def test_bus_data_no_region():
    assert "Substation" not in queries.bus_data(region=None)
