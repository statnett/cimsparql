import pytest

from cimsparql import redland

from conftest import need_cim_tp


@pytest.fixture(scope="module")
def tpmodel(root_dir, tp_profile):
    return redland.Model(root_dir / "data" / f"{tp_profile}.xml", base_uri="http://example.org")


@need_cim_tp
def test_tp_terminal(tpmodel):
    terminal = tpmodel.terminal(limit=100)
    assert terminal.shape == (100, 2)


@need_cim_tp
def test_tp_topological_node(tpmodel):
    topological_node = tpmodel.topological_node(limit=100)
    assert topological_node.shape == (100, 3)
