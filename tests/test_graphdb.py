import itertools
import pytest

import networkx as nx
import numpy as np
import pandas as pd


from cimsparql.queries import (
    ac_line_query,
    branches,
    connection_query,
    connectivity_mrid,
    load_query,
    reference_nodes,
    synchronous_machines_query,
    transformer_query,
    windings_to_tr,
)


n_samples = 40


@pytest.fixture(scope="module")
def breakers(cim15, gdb_cli):
    query = f"{cim15}\n\n{connection_query('cim:Breaker')} limit {n_samples}"
    return gdb_cli.get_table(query).set_index("mrid")


@pytest.fixture(scope="module")
def disconnectors(cim15, gdb_cli):
    query = f"{cim15}\n\n{connection_query('cim:Disconnector')} limit {n_samples}"
    return gdb_cli.get_table(query).set_index("mrid")


def test_conform_load(cim15, gdb_cli):
    query = f"{cim15}\n\n{load_query()} limit {n_samples}"
    load = gdb_cli.get_table(query).set_index("mrid")
    assert len(load) == n_samples
    assert set(load.columns).issubset(["connectivity_mrid"])


def test_non_conform_load(cim15, gdb_cli):
    query = f"{cim15}\n\n{load_query(conform=False)} limit {n_samples}"
    load = gdb_cli.get_table(query).set_index("mrid")
    assert len(load) == n_samples
    assert set(load.columns).issubset(["connectivity_mrid"])


def test_synchronous_machines(cim15, gdb_cli):
    query = f"{cim15}\n\n{synchronous_machines_query()} limit {n_samples}"
    synchronous_machines = gdb_cli.get_table(query).set_index("mrid")
    assert len(synchronous_machines) == n_samples
    assert set(synchronous_machines.columns).issuperset(["sn", "connectivity_mrid"])


def test_branch(cim15, gdb_cli):
    query = f"{cim15}\n{ac_line_query()} limit {n_samples}"
    lines = gdb_cli.get_table(query).set_index(connectivity_mrid(sparql=False)).astype(float)

    assert len(lines) == n_samples
    assert set(lines.columns).issuperset(["x", "un"])


def test_transformers(cim15, gdb_cli):
    query = f"{cim15}\n{transformer_query()} limit {n_samples}"
    windings = gdb_cli.get_table(query)
    windings["endNumber"] = windings["endNumber"].astype(int)
    windings[["x", "Un"]] = windings[["x", "Un"]].astype(float)

    two_tr, three_tr = windings_to_tr(windings)
    assert len(two_tr) > 10
    assert set(two_tr.columns).issuperset(["mrid", "x", "Un"])

    assert len(three_tr) > 2
    cols = [[f"x_{i}", f"Un_{i}", f"connectivity_mrid_{i}"] for i in range(1, 4)]
    assert set(three_tr.columns).issuperset(itertools.chain.from_iterable(cols))


def test_reference_nodes():
    a = pd.DataFrame([[1, 2], [1, 3], [3, 4], [5, 6], [8, 7]])
    nodes_ref = {1: 1, 2: 1, 3: 1, 4: 1, 5: 5, 6: 5, 7: 8, 8: 8}
    nodes = reference_nodes(a)
    assert nodes_ref == nodes


def test_breaker_length(breakers):
    assert len(breakers) == n_samples


def test_breaker_reference_nodes(breakers):
    node_dict = reference_nodes(breakers)
    assert len(node_dict) == len(np.unique(breakers.to_numpy()))
    assert len(set(node_dict.keys())) == len(node_dict)
    assert len(set(node_dict.values())) < len(node_dict)


def test_connectors_length(disconnectors):
    assert len(disconnectors) == n_samples


def test_connectors_reference_nodes(disconnectors):
    node_dict = reference_nodes(disconnectors)
    assert len(node_dict) == len(np.unique(disconnectors.to_numpy()))
    assert len(set(node_dict.keys())) == len(node_dict)
    assert len(set(node_dict.values())) < len(node_dict)


@pytest.mark.integration
def test_connect_system(cim15, gdb_cli):
    query = f"{cim15}\n\n{connection_query('cim:Disconnector')}"
    disconnector_status = gdb_cli.get_table(query)

    query = f"{cim15}\n\n{connection_query('cim:Breaker')}"
    breakers = gdb_cli.get_table(query)

    # Lines
    columns = connectivity_mrid(sparql=False, sequence_numbers=[1, 2, 3])
    query = f"{cim15}\n{ac_line_query()}"
    lines = gdb_cli.get_table(query).set_index(columns[:2]).astype(float)
    lines.reset_index(inplace=True)

    query = f"{cim15}\n{transformer_query()}"
    windings = gdb_cli.get_table(query)
    windings["endNumber"] = windings["endNumber"].astype(int)
    windings[["x", "Un"]] = windings[["x", "Un"]].astype(float)

    connectors = pd.concat([breakers, disconnector_status]).set_index("mrid")
    lines, two_tr, three_tr = branches(connectors, lines, windings, columns)

    g = nx.Graph()
    for connections in [lines, two_tr]:
        g.add_edges_from(connections.loc[:, columns[:2]].to_numpy())

    for col in columns:
        g.add_edges_from(three_tr.loc[:, ["index", col]].to_numpy())

    for i, group in enumerate(nx.connected_components(g)):
        if i == 0:
            assert len(group) > 4100
        else:
            assert len(group) < 8
    assert i < 65
