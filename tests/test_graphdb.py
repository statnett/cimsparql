import itertools
import pytest

import numpy as np
import pandas as pd


from cimsparql.queries import reference_nodes, windings_to_tr


n_samples = 40


@pytest.fixture(scope="module")
def breakers(gdb_cli):
    return gdb_cli.connections(rdf_type="cim:Breaker", limit=n_samples)


@pytest.fixture(scope="module")
def disconnectors(gdb_cli):
    return gdb_cli.connections(rdf_type="cim:Disconnector", limit=n_samples)


def test_cimversion(gdb_cli):
    assert gdb_cli._cim_version == 15


def test_conform_load(gdb_cli):
    load = gdb_cli.loads(limit=n_samples)
    assert len(load) == n_samples
    assert set(load.columns).issubset(["connectivity_mrid", "terminal_mrid"])


def test_non_conform_load(gdb_cli):
    load = gdb_cli.loads(conform=False, limit=n_samples)
    assert len(load) == n_samples
    assert set(load.columns).issubset(["connectivity_mrid", "terminal_mrid"])


def test_synchronous_machines(gdb_cli):
    synchronous_machines = gdb_cli.synchronous_machines(limit=n_samples)
    assert len(synchronous_machines) == n_samples
    assert set(synchronous_machines.columns).issubset(["sn", "connectivity_mrid", "terminal_mrid"])


def test_branch(gdb_cli):
    lines = gdb_cli.ac_lines(limit=n_samples)
    assert lines.shape == (n_samples, 9)
    assert all(lines[["x", "un"]].dtypes == np.float)


def test_transformers(gdb_cli):
    windings = gdb_cli.transformers(limit=n_samples)

    two_tr, three_tr = windings_to_tr(windings)
    assert len(two_tr) > 10
    assert set(two_tr.columns).issubset(["mrid", "x", "un"])

    cols = [[f"x_{i}", f"un_{i}", f"connectivity_mrid_{i}"] for i in range(1, 4)]
    assert len(three_tr) > 2
    assert set(three_tr.columns).issubset(itertools.chain.from_iterable(cols))


def test_reference_nodes():
    a = pd.DataFrame([[1, 2], [1, 3], [3, 4], [5, 6], [8, 7]])
    nodes_ref = {1: 1, 2: 1, 3: 1, 4: 1, 5: 5, 6: 5, 7: 8, 8: 8}
    nodes = reference_nodes(a)
    assert nodes_ref == nodes


def test_breaker_length(breakers):
    assert len(breakers) == n_samples


def test_breaker_reference_nodes(breakers):
    connect_columns = [f"connectivity_mrid_{nr}" for nr in [1, 2]]
    node_dict = reference_nodes(breakers[connect_columns])
    assert len(node_dict) == len(np.unique(breakers[connect_columns].to_numpy()))
    assert len(set(node_dict.keys())) == len(node_dict)
    assert len(set(node_dict.values())) < len(node_dict)


def test_connectors_length(disconnectors):
    assert len(disconnectors) == n_samples


def test_connectors_reference_nodes(disconnectors):
    connect_columns = [f"connectivity_mrid_{nr}" for nr in [1, 2]]
    node_dict = reference_nodes(disconnectors[connect_columns])
    assert len(node_dict) == len(np.unique(disconnectors[connect_columns].to_numpy()))
    assert len(set(node_dict.keys())) == len(node_dict)
    assert len(set(node_dict.values())) < len(node_dict)
