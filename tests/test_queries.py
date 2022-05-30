import os
from typing import Dict

import pytest
from mock import MagicMock, call, patch

from cimsparql import queries
from cimsparql.cim import GEO_REG
from cimsparql.constants import con_mrid_str as c_mrid
from cimsparql.constants import mrid_variable, union_split


@pytest.fixture()
def load_query_kwargs():
    return {
        "load_type": ["ConformLoad"],
        "sub_region": False,
        "load_vars": None,
        "region": "NO",
        "connectivity": None,
        "station_group_optional": False,
        "with_sequence_number": False,
        "network_analysis": False,
        "station_group": False,
        "cim_version": 15,
        "mrid": mrid_variable,
    }


def test_load_query_raises_value_error(load_query_kwargs):
    load_query_kwargs["load_type"] = ["non_type"]
    with pytest.raises(ValueError):
        queries.load_query(**load_query_kwargs)


def test_load_query_raises_value_error_empty_list(load_query_kwargs):
    load_query_kwargs["load_type"] = []
    with pytest.raises(ValueError):
        queries.load_query(**load_query_kwargs)


def test_load_query_no_connectivity(load_query_kwargs):
    load = queries.load_query(**load_query_kwargs)
    assert "connectivity_mrid" not in load


def test_load_query_with_connectivity(load_query_kwargs):
    load_query_kwargs["connectivity"] = c_mrid
    assert c_mrid in queries.load_query(**load_query_kwargs)


def test_load_query_with_region(load_query_kwargs):
    assert GEO_REG in queries.load_query(**load_query_kwargs)


def test_load_query_with_no_region(load_query_kwargs):
    load_query_kwargs["region"] = None
    assert GEO_REG not in queries.load_query(**load_query_kwargs)


def test_load_query_with_station_group(load_query_kwargs):
    load_query_kwargs["station_group"] = True
    assert "station_group" in queries.load_query(**load_query_kwargs)


def test_load_query_with_no_station_group(load_query_kwargs):
    assert "station_group" not in queries.load_query(**load_query_kwargs)


@patch("cimsparql.query_support.group_query", new=MagicMock)
@patch("cimsparql.query_support.combine_statements")
def test_load_query_conform(_combine_statements_mock, load_query_kwargs):
    queries.load_query(**load_query_kwargs)
    call_args = [f"?mrid rdf:type cim:{load_type}" for load_type in load_query_kwargs["load_type"]]
    call_wargs = {"group": False, "split": union_split}
    assert _combine_statements_mock.call_args_list[0] == call(*call_args, **call_wargs)


@patch("cimsparql.query_support.group_query", new=MagicMock)
@patch("cimsparql.query_support.combine_statements")
def test_load_query_combined(_combine_statements_mock, load_query_kwargs):
    load_query_kwargs["load_type"] = ["ConformLoad", "NonConformLoad"]
    queries.load_query(**load_query_kwargs)
    call_args = [f"?mrid rdf:type cim:{load_type}" for load_type in load_query_kwargs["load_type"]]
    call_wargs = {"group": True, "split": union_split}
    assert _combine_statements_mock.call_args_list[0] == call(*call_args, **call_wargs)


@pytest.fixture(scope="module")
def bus_data_kwargs() -> Dict[str, str]:
    return {"mrid": "?mrid", "name": "?name", "sub_region": False}


def test_bus_data_with_region(bus_data_kwargs: Dict[str, str]):
    assert GEO_REG in queries.bus_data(region="NO", **bus_data_kwargs)


def test_bus_data_no_region(bus_data_kwargs: Dict[str, str]):
    assert GEO_REG not in queries.bus_data(region=None, **bus_data_kwargs)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_bus_data_with_nodes(gdb_cli):
    data = gdb_cli.bus_data()
    assert data.index.name == "mrid"
    assert list(data.columns) == ["name"]
    assert list(data.dtypes) == ["string"]
    assert data.index.dtype == "string"
