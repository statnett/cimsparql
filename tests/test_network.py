import os

import numpy as np
import pandas as pd
import pytest

from cimsparql.network import Islands


def test_reference_nodes():
    a = pd.DataFrame([[1, 2], [1, 3], [3, 4], [5, 6], [8, 7]])
    nodes_ref = [[1, 1], [2, 1], [3, 1], [4, 1], [5, 5], [6, 5], [7, 8], [8, 8]]
    nodes = Islands(a).reference_nodes()
    pd.testing.assert_frame_equal(
        nodes.sort_index(), pd.DataFrame(nodes_ref, columns=["mrid", "ref_node"]).set_index("mrid")
    )


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_breaker_reference_nodes(breakers: pd.DataFrame):
    connect_columns = [f"connectivity_mrid_{nr}" for nr in [1, 2]]
    node_dict = Islands(breakers[connect_columns]).reference_nodes()
    assert len(node_dict) == len(np.unique(breakers[connect_columns].to_numpy()))


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_connectors_reference_nodes(disconnectors: pd.DataFrame):
    connect_columns = [f"connectivity_mrid_{nr}" for nr in [1, 2]]
    node_dict = Islands(disconnectors[connect_columns]).reference_nodes()
    assert len(node_dict) == len(np.unique(disconnectors[connect_columns].to_numpy()))
