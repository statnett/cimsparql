from conftest import need_local_graphdb_cim


@need_local_graphdb_cim
def test_tp_terminal(gcli_cim):
    terminal = gcli_cim.terminal(limit=100)
    assert terminal.shape == (100, 2)


@need_local_graphdb_cim
def test_tp_topological_node(gcli_cim):
    topological_node = gcli_cim.topological_node(limit=2)
    assert len(topological_node) == 0
