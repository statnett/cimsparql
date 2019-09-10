import itertools
import pytest

import numpy as np
import pandas as pd


from cimsparql.queries import windings_to_tx, three_tx_to_windings, Islands


n_samples = 40


@pytest.fixture(scope="module")
def breakers(gdb_cli):
    return gdb_cli.connections(
        rdf_type="cim:Breaker", limit=n_samples, connectivity="connectivity_mrid"
    )


@pytest.fixture(scope="module")
def disconnectors(gdb_cli):
    return gdb_cli.connections(
        rdf_type="cim:Disconnector", limit=n_samples, connectivity="connectivity_mrid"
    )


def test_cimversion(gdb_cli):
    assert gdb_cli._cim_version == 15


load_columns = ["connectivity_mrid", "terminal_mrid", "p", "q"]


def test_conform_load(gdb_cli):
    load = gdb_cli.loads(load_type=["ConformLoad"], limit=n_samples)
    assert len(load) == n_samples
    assert set(load.columns).issubset(load_columns)


def test_non_conform_load(gdb_cli):
    load = gdb_cli.loads(load_type=["NonConformLoad"], limit=n_samples)
    assert len(load) == n_samples
    assert set(load.columns).issubset(load_columns)


def test_conform_and_non_conform_load(gdb_cli):
    load = gdb_cli.loads(load_type=["ConformLoad", "NonConformLoad"], limit=n_samples)
    assert len(load) == n_samples
    assert set(load.columns).issubset(load_columns)


def test_synchronous_machines(gdb_cli):
    synchronous_machines = gdb_cli.synchronous_machines(limit=n_samples)
    assert len(synchronous_machines) == n_samples
    assert set(synchronous_machines.columns).difference(["sn", "terminal_mrid", "p", "q"]) == set()


def test_branch(gdb_cli):
    lines = gdb_cli.ac_lines(limit=n_samples).set_index("mrid")
    assert lines.shape == (n_samples, 7)
    assert all(lines[["x", "un"]].dtypes == np.float)


def test_branch_with_connectivity(gdb_cli):
    lines = gdb_cli.ac_lines(limit=n_samples, connectivity="connectivity_mrid").set_index("mrid")
    assert lines.shape == (n_samples, 9)
    assert all(lines[["x", "un"]].dtypes == np.float)


def test_transformers_with_connectivity(gdb_cli):
    windings = gdb_cli.transformers(limit=n_samples, connectivity="connectivity_mrid")

    two_tx, three_tx = windings_to_tx(windings)
    assert len(two_tx) > 10
    assert set(two_tx.columns).issuperset(["ckt", "x", "un"])

    cols = [[f"x_{i}", f"un_{i}", f"connectivity_mrid_{i}"] for i in range(1, 4)]
    assert len(three_tx) > 2
    assert set(three_tx.columns).issuperset(itertools.chain.from_iterable(cols))

    dummy_tx = three_tx_to_windings(three_tx, ["t_mrid_1", "t_mrid_2", "b", "x", "ckt"])
    assert len(dummy_tx) == 3 * len(three_tx)
    assert set(dummy_tx.columns).difference(["t_mrid_1", "t_mrid_2", "b", "x", "ckt"]) == set()


def test_transformers(gdb_cli):
    windings = gdb_cli.transformers(limit=n_samples)

    two_tx, three_tx = windings_to_tx(windings)
    assert len(two_tx) > 10
    assert set(two_tx.columns).issuperset(["ckt", "x", "un"])

    cols = [[f"x_{i}", f"un_{i}", f"connectivity_mrid_{i}"] for i in range(1, 4)]
    assert len(three_tx) > 2
    assert not set(three_tx.columns).issuperset(itertools.chain.from_iterable(cols))


def test_reference_nodes():
    a = pd.DataFrame([[1, 2], [1, 3], [3, 4], [5, 6], [8, 7]])
    nodes_ref = {1: 1, 2: 1, 3: 1, 4: 1, 5: 5, 6: 5, 7: 8, 8: 8}
    nodes = Islands(a).reference_nodes_dict()
    assert nodes_ref == nodes


def test_breaker_length(breakers):
    assert len(breakers) == n_samples


def test_breaker_reference_nodes(breakers):
    connect_columns = [f"connectivity_mrid_{nr}" for nr in [1, 2]]
    node_dict = Islands(breakers[connect_columns]).reference_nodes_dict()
    assert len(node_dict) == len(np.unique(breakers[connect_columns].to_numpy()))
    assert len(set(node_dict.keys())) == len(node_dict)
    assert len(set(node_dict.values())) < len(node_dict)


def test_connectors_length(disconnectors):
    assert len(disconnectors) == n_samples


def test_connectors_reference_nodes(disconnectors):
    connect_columns = [f"connectivity_mrid_{nr}" for nr in [1, 2]]
    node_dict = Islands(disconnectors[connect_columns]).reference_nodes_dict()
    assert len(node_dict) == len(np.unique(disconnectors[connect_columns].to_numpy()))
    assert len(set(node_dict.keys())) == len(node_dict)
    assert len(set(node_dict.values())) < len(node_dict)
