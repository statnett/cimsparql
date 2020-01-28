from cimsparql import queries, ssh_queries
from conftest import need_local_graphdb_eq, need_local_graphdb_ssh

n_lim = 100


@need_local_graphdb_eq
def test_connectivity_names(gcli_eq):
    connectivity_names = gcli_eq.get_table(queries.connectivity_names(), index="mrid", limit=n_lim)
    assert connectivity_names.shape == (n_lim, 1)


@need_local_graphdb_ssh
def test_disconnected_disconnectors_and_terminals(gcli_ssh):
    disconnected = gcli_ssh.get_table(
        ssh_queries.disconnected(gcli_ssh.cim_version), index="mrid", limit=n_lim
    )
    assert len(disconnected) == n_lim


@need_local_graphdb_eq
def test_connections_disconnector(gcli_eq):
    rdf_types = ["cim:Disconnector"]
    connections = gcli_eq.get_table(
        queries.connection_query(cim_version=gcli_eq.cim_version, rdf_types=rdf_types, region=None),
        limit=n_lim,
    )
    assert len(connections) == n_lim


@need_local_graphdb_eq
def test_connections_breaker(gcli_eq):
    rdf_types = ["cim:Breaker"]
    connections = gcli_eq.get_table(
        queries.connection_query(cim_version=gcli_eq.cim_version, rdf_types=rdf_types, region=None),
        limit=n_lim,
    )
    assert len(connections) == n_lim


@need_local_graphdb_eq
def test_connections_combined(gcli_eq):
    rdf_types = ["cim:Disconnector", "cim:Breaker"]
    connections = gcli_eq.get_table(
        queries.connection_query(cim_version=gcli_eq.cim_version, rdf_types=rdf_types, region=None),
        limit=n_lim,
    )
    assert len(connections) == n_lim
