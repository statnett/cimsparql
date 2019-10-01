import numpy as np

from conftest import need_local_graphdb_ssh


def test_ieee118_ssh_synchronous_machines(ieee118):
    synchronous_machines = ieee118.ssh_synchronous_machines()
    assert synchronous_machines.shape == (19, 3)


def test_ieee118_disconnected_all_connected(ieee118):
    assert ieee118.disconnected().empty


def test_ieee118_ssh_conformed_load(ieee118):
    assert ieee118.ssh_load().empty


def test_ieee118_ssh_load_energy_consumer(ieee118):
    load = ieee118.ssh_load(rdf_types=["cim:EnergyConsumer"])
    assert load.shape == (103, 2)


def test_ieee118_powerflow(ieee118):
    powerflow = ieee118.powerflow()
    assert powerflow.shape == (474, 2)


def test_ieee118_synchronous_machine(ieee118):
    synchronous_machines = ieee118.ssh_synchronous_machines()
    assert synchronous_machines.shape == (19, 3)


def test_ieee118_generating_unit_thermal(ieee118):
    thermal_generating_units = ieee118.ssh_generating_unit(rdf_types=["cim:ThermalGeneratingUnit"])
    assert thermal_generating_units.shape == (19, 1)


def test_ieee118_generating_unit_all(ieee118):
    thermal_generating_units = ieee118.ssh_generating_unit()
    assert thermal_generating_units.shape == (19, 1)


def test_ieee118_topological_nodes(ieee118):
    topological_node = ieee118.topological_node()
    assert topological_node.shape == (115, 3)


def test_ieee118_voltage(ieee118):
    voltage = ieee118.voltage()
    assert voltage.shape == (118, 2)


def test_ieee118_tapstep(ieee118):
    tap = ieee118.tapstep()
    assert tap.shape == (10, 1)


def test_ieee118_terminal(ieee118):
    terminal = ieee118.terminal()
    assert terminal.shape == (626, 2)


@need_local_graphdb_ssh
def test_model_ssh_synchronous_machine(gcli_ssh):
    synchronous_machines = gcli_ssh.ssh_synchronous_machines()
    assert list(synchronous_machines.columns) == ["p", "q", "controlEnabled"]
    assert len(synchronous_machines) == 2296


@need_local_graphdb_ssh
def test_disconnected(gcli_ssh):
    disconnected = gcli_ssh.disconnected(limit=100)
    assert list(disconnected.columns) == ["mrid"]
    assert len(disconnected) == 100


@need_local_graphdb_ssh
def test_ssh_conformed_load(gcli_ssh):
    load = gcli_ssh.ssh_load(rdf_types=["cim:ConformLoad"])
    assert list(load.columns) == ["p", "q"]
    assert len(load) == 1856


@need_local_graphdb_ssh
def test_ssh_nonconformed_load(gcli_ssh):
    load = gcli_ssh.ssh_load(rdf_types=["cim:NonConformLoad"])
    assert list(load.columns) == ["p", "q"]
    assert len(load) == 194


@need_local_graphdb_ssh
def test_ssh_combined_load(gcli_ssh):
    load = gcli_ssh.ssh_load()
    assert list(load.columns) == ["p", "q"]
    assert len(load) == 1856 + 194


@need_local_graphdb_ssh
def test_ssh_hydro_generating_unit(gcli_ssh):
    hydro = gcli_ssh.ssh_generating_unit(["cim:HydroGeneratingUnit"])
    assert np.all(hydro == 0.0)
    assert len(hydro) == 2177


@need_local_graphdb_ssh
def test_ssh_thermal_generating_unit(gcli_ssh):
    thermal = gcli_ssh.ssh_generating_unit(["cim:ThermalGeneratingUnit"])
    assert np.all(thermal == 0.0)
    assert len(thermal) == 40


@need_local_graphdb_ssh
def test_ssh_wind_generating_unit(gcli_ssh):
    wind = gcli_ssh.ssh_generating_unit(["cim:WindGeneratingUnit"])
    assert np.all(wind == 0.0)
    assert len(wind) == 71


@need_local_graphdb_ssh
def test_ssh_generating_unit_hydro_thermal(gcli_ssh):
    gen = gcli_ssh.ssh_generating_unit(["cim:HydroGeneratingUnit", "cim:ThermalGeneratingUnit"])
    assert np.all(gen == 0.0)
    assert len(gen) == 40 + 2177


@need_local_graphdb_ssh
def test_ssh_generating_unit_union_all(gcli_ssh):
    gen = gcli_ssh.ssh_generating_unit(
        ["cim:HydroGeneratingUnit", "cim:ThermalGeneratingUnit", "cim:WindGeneratingUnit"]
    )
    assert np.all(gen == 0.0)
    assert len(gen) == 40 + 2177 + 71


@need_local_graphdb_ssh
def test_ssh_generating_unit_union_all_default(gcli_ssh):
    gen = gcli_ssh.ssh_generating_unit()
    assert np.all(gen == 0.0)
    assert len(gen) == 40 + 2177 + 71
