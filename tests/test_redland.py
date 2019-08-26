import pytest

from cimsparql import ssh_queries as queries
from cimsparql import redland

from conftest import need_cim_ssh


@pytest.fixture(scope="module")
def sshmodel(root_dir, identifier):
    return redland.Model(root_dir / "data" / f"{identifier}.xml", base_uri="http://example.org")


@need_cim_ssh
def skip_test_model_ssh_components(sshmodel, cim16):
    query = str(cim16) + "\n\nselect distinct ?o \n where { ?s rdf:type ?o . }"

    # print(sshmodel.get_table(query))
    for r in sshmodel.get_table(query)["o"]:
        print(r)


@need_cim_ssh
def test_model_ssh_synchronous_machine(sshmodel, cim16):
    synchronous_machines = queries.ssh_synchronous_machines(sshmodel, str(cim16))
    assert list(synchronous_machines.columns) == ["mrid", "p", "q", "controlEnabled"]
    assert len(synchronous_machines) == 2296


@need_cim_ssh
def test_ssh_disconnected(sshmodel, cim16):
    disconnected = queries.ssh_disconnected(sshmodel, str(cim16))
    assert list(disconnected.columns) == ["mrid"]
    assert len(disconnected) == 7878


@need_cim_ssh
def test_ssh_conformed_load(sshmodel, cim16):
    load = queries.ssh_load(sshmodel, str(cim16))
    assert list(load.columns) == ["mrid", "p", "q"]
    assert len(load) == 1856


@need_cim_ssh
def test_ssh_nonconformed_load(sshmodel, cim16):
    load = queries.ssh_load(sshmodel, str(cim16), conform=False)
    assert list(load.columns) == ["mrid", "p", "q"]
    assert len(load) == 194


@need_cim_ssh
def test_ssh_combined_load(sshmodel, cim16):
    load = queries.ssh_combined_load(sshmodel, str(cim16))
    assert list(load.columns) == ["mrid", "p", "q"]
    assert len(load) == 1856 + 194
