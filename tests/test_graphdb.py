import itertools
from datetime import datetime
from typing import List
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from cimsparql.graphdb import GraphDBClient, data_row
from cimsparql.queries import Islands, three_tx_to_windings, windings_to_tx
from cimsparql.url import service

n_samples = 40


@pytest.fixture(scope="module")
def breakers(gdb_cli: GraphDBClient):
    return gdb_cli.connections(
        rdf_types="cim:Breaker", limit=n_samples, connectivity="connectivity_mrid"
    )


@pytest.fixture(scope="module")
def disconnectors(gdb_cli: GraphDBClient):
    return gdb_cli.connections(
        rdf_types="cim:Disconnector", limit=n_samples, connectivity="connectivity_mrid"
    )


@patch.object(GraphDBClient, "_get_table_and_convert")
def test_date_version(get_table_mock, gdb_cli: GraphDBClient):
    t_ref = datetime(2020, 1, 1)
    get_table_mock.return_value = pd.DataFrame(
        {"col1": [1], "activationDate": [np.datetime64(t_ref)]}
    )
    assert gdb_cli.date_version == t_ref


def test_cimversion(gdb_cli: GraphDBClient):
    assert gdb_cli.cim_version == 15


def test_str_rep(gdb_cli: GraphDBClient, graphdb_repo: str):
    target = f"<GraphDBClient object, service: {service(graphdb_repo)}>"
    assert str(gdb_cli) == target


load_columns = ["connectivity_mrid", "terminal_mrid", "bidzone", "p", "q"]


def test_conform_load(gdb_cli: GraphDBClient):
    load = gdb_cli.loads(load_type=["ConformLoad"], limit=n_samples)
    assert len(load) == n_samples
    assert set(load.columns).issubset(load_columns)


def test_non_conform_load(gdb_cli: GraphDBClient):
    load = gdb_cli.loads(load_type=["NonConformLoad"], limit=n_samples)
    assert len(load) == n_samples
    assert set(load.columns).issubset(load_columns)


def test_series_compensator(gdb_cli: GraphDBClient):
    compensators = gdb_cli.series_compensators(region="NO", limit=3)
    assert compensators.shape == (3, 6)


def test_series_compensator_with_market(gdb_cli: GraphDBClient):
    compensators = gdb_cli.series_compensators(region="NO", limit=3, with_market=True)
    assert compensators.shape == (3, 8)


def test_phase_tap_changer(gdb_cli: GraphDBClient):
    tap_changers = gdb_cli.phase_tap_changers(region=None)
    assert tap_changers.shape == (1, 9)


def test_conform_and_non_conform_load(gdb_cli: GraphDBClient):
    load = gdb_cli.loads(load_type=["ConformLoad", "NonConformLoad"], limit=n_samples)
    assert len(load) == n_samples
    assert set(load.columns).issubset(load_columns)


@pytest.fixture()
def gen_columns() -> List[str]:
    return [
        "allocationMax",
        "allocationWeight",
        "market_code",
        "maxP",
        "minP",
        "name",
        "station_group",
    ]


@pytest.fixture()
def synchronous_machines_columns(gen_columns: List[str]) -> List[str]:
    return gen_columns + ["bidzone", "p", "q", "sn", "terminal_mrid"]


@pytest.fixture()
def wind_units_machines_columns(gen_columns: List[str]) -> List[str]:
    return gen_columns + ["power_plant_mrid"]


def test_synchronous_machines(gdb_cli: GraphDBClient, synchronous_machines_columns: List[str]):
    synchronous_machines = gdb_cli.synchronous_machines(limit=n_samples)
    assert len(synchronous_machines) == n_samples
    assert set(synchronous_machines.columns).difference(synchronous_machines_columns) == set()


def test_wind_generating_units(gdb_cli: GraphDBClient, wind_units_machines_columns: List[str]):
    wind_units_machines = gdb_cli.wind_generating_units(limit=n_samples)
    assert len(wind_units_machines) == n_samples
    assert set(wind_units_machines.columns).difference(wind_units_machines_columns) == set()


def test_regions(gdb_cli: GraphDBClient):
    assert gdb_cli.regions.groupby("region").count()["shortName"]["NO"] > 16


def test_branch(gdb_cli: GraphDBClient):
    lines = gdb_cli.ac_lines(limit=n_samples)
    assert lines.shape == (n_samples, 12)
    assert all(lines[["x", "un"]].dtypes == float)


def test_branch_with_temperatures(gdb_cli: GraphDBClient):
    lines = gdb_cli.ac_lines(limit=n_samples, rates=None, temperatures=range(-30, 30, 10))
    assert lines.shape == (n_samples, 15)
    assert all(lines[["x", "un"]].dtypes == float)


def test_branch_with_two_temperatures(gdb_cli: GraphDBClient):
    lines = gdb_cli.ac_lines(limit=n_samples, rates=None, temperatures=range(-20, 0, 10))
    assert lines.shape == (n_samples, 11)
    assert all(lines[["x", "un"]].dtypes == float)


def test_ac_line_segment_with_market(gdb_cli: GraphDBClient):
    lines = gdb_cli.ac_lines(limit=n_samples, with_market=True, rates=None, temperatures=None)
    assert lines.shape == (n_samples, 11)
    assert all(lines[["x", "un"]].dtypes == float)


def test_branch_with_connectivity(gdb_cli: GraphDBClient):
    lines = gdb_cli.ac_lines(
        limit=n_samples, connectivity="connectivity_mrid", temperatures=range(0, 10, 10)
    )
    assert lines.shape == (n_samples, 15)
    assert all(lines[["x", "un"]].dtypes == float)


def test_transformers_with_connectivity(gdb_cli: GraphDBClient):
    windings = gdb_cli.transformers(
        region="NO01", sub_region=True, connectivity="connectivity_mrid", with_market=True
    )
    two_tx, three_tx = windings_to_tx(windings, pd.DataFrame())
    assert len(two_tx) > 10
    assert set(two_tx.columns).issuperset(["ckt", "x", "un", "bidzone_1", "bidzone_2"])

    cols = [[f"x_{i}", f"un_{i}", f"connectivity_mrid_{i}"] for i in range(1, 4)]
    assert len(three_tx) > 2
    assert set(three_tx.columns).issuperset(itertools.chain.from_iterable(cols))

    cols = ["t_mrid_1", "t_mrid_2", "b", "x", "ckt", "bidzone"]
    dummy_tx = three_tx_to_windings(three_tx, cols)
    assert len(dummy_tx) == 3 * len(three_tx)
    assert set(dummy_tx.columns).difference(cols + ["bidzone_1", "bidzone_2"]) == set()


def test_transformers_with_multiple_sub_regions(gdb_cli: GraphDBClient):
    windings = gdb_cli.transformers(region=[f"NO0{no}" for no in [1, 2, 3]], sub_region=True)
    assert windings.shape[0] > 2
    assert windings.shape[1] == 11


def test_transformers_with_faseshift(gdb_cli: GraphDBClient):
    windings = gdb_cli.transformers(region="SE", connectivity="connectivity_mrid", with_market=True)
    tap_changers = gdb_cli.phase_tap_changers(region="SE")
    columns = set(tap_changers.columns)
    assert "w_mrid_1" not in tap_changers.columns
    windings_to_tx(windings, tap_changers)
    assert "w_mrid_1" in tap_changers.columns


def test_windings(gdb_cli: GraphDBClient):
    windings = gdb_cli.transformers(region="NO01", sub_region=True)
    assert windings.shape[1] == 11


def test_windings_with_market(gdb_cli: GraphDBClient):
    windings = gdb_cli.transformers(region="NO01", sub_region=True, with_market=True)
    assert windings.shape[1] == 12


def test_transformers(gdb_cli: GraphDBClient):
    windings = gdb_cli.transformers(region="NO01", sub_region=True, with_market=True)

    two_tx, three_tx = windings_to_tx(windings, pd.DataFrame())
    assert len(two_tx) > 10
    assert set(two_tx.columns).issuperset(["ckt", "x", "un", "bidzone_1", "bidzone_2"])

    cols = [[f"x_{i}", f"un_{i}", f"connectivity_mrid_{i}"] for i in range(1, 4)]
    assert len(three_tx) > 2
    assert not set(three_tx.columns).issuperset(itertools.chain.from_iterable(cols))


def test_reference_nodes():
    a = pd.DataFrame([[1, 2], [1, 3], [3, 4], [5, 6], [8, 7]])
    nodes_ref = [[1, 1], [2, 1], [3, 1], [4, 1], [5, 5], [6, 5], [7, 8], [8, 8]]
    nodes = Islands(a).reference_nodes()
    pd.testing.assert_frame_equal(
        nodes.sort_index(), pd.DataFrame(nodes_ref, columns=["mrid", "ref_node"]).set_index("mrid")
    )


def test_breaker_length(breakers: pd.DataFrame):
    assert len(breakers) == n_samples


def test_breaker_reference_nodes(breakers: pd.DataFrame):
    connect_columns = [f"connectivity_mrid_{nr}" for nr in [1, 2]]
    node_dict = Islands(breakers[connect_columns]).reference_nodes()
    assert len(node_dict) == len(np.unique(breakers[connect_columns].to_numpy()))


def test_connectors_length(disconnectors: pd.DataFrame):
    assert len(disconnectors) == n_samples


def test_connectors_reference_nodes(disconnectors: pd.DataFrame):
    connect_columns = [f"connectivity_mrid_{nr}" for nr in [1, 2]]
    node_dict = Islands(disconnectors[connect_columns]).reference_nodes()
    assert len(node_dict) == len(np.unique(disconnectors[connect_columns].to_numpy()))


def test_data_row():
    cols = ["a", "b", "c", "d", "e"]
    rows = [{"a": 1, "b": 2}, {"c": 3, "d": 4}, {"a": 5, "b": 6}, {"e": 7}]
    assert not set(data_row(cols, rows)).symmetric_difference(cols)


def test_data_row_missing_column():
    cols = ["a", "b", "c", "d", "e"]
    rows = [{"a": 1, "b": 2}, {"c": 3}, {"a": 5, "b": 6}, {"e": 7}]
    assert set(data_row(cols, rows).keys()).symmetric_difference(cols) == {"d"}
