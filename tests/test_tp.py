import pytest

from cimsparql import tp_queries as queries
from cimsparql import redland

from conftest import need_cim_tp


@pytest.fixture(scope="module")
def tpmodel(root_dir, tp_profile):
    return redland.Model(root_dir / "data" / f"{tp_profile}.xml", base_uri="http://example.org")


@need_cim_tp
def test_tp_terminal(tpmodel, cim15):
    terminal = queries.tp_terminal(tpmodel, str(cim15))
    assert terminal.shape == (15070, 2)


@need_cim_tp
def test_tp_topological_node(tpmodel, cim15):
    topological_node = queries.tp_topological_node(tpmodel, str(cim15))
    assert topological_node.shape == (4407, 3)
