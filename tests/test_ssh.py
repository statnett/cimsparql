import pytest

import numpy as np

from cimsparql import redland

from conftest import need_cim_ssh


@pytest.fixture(scope="module")
def sshmodel(root_dir, ssh_profile):
    return redland.Model(root_dir / "data" / f"{ssh_profile}.xml", base_uri="http://example.org")


def test_ieee118_ssh_synchronous_machines(ieee118):
    synchronous_machines = ieee118.ssh_synchronous_machines()
    assert synchronous_machines.shape == (19, 3)


def test_ieee118_ssh_disconnected_all_connected(ieee118):
    assert ieee118.ssh_disconnected().empty


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


@need_cim_ssh
def test_model_ssh_synchronous_machine(sshmodel):
    synchronous_machines = sshmodel.ssh_synchronous_machines()
    assert list(synchronous_machines.columns) == ["p", "q", "controlEnabled"]
    assert len(synchronous_machines) == 2296


@need_cim_ssh
def test_ssh_disconnected(sshmodel):
    disconnected = sshmodel.ssh_disconnected(limit=100)
    assert list(disconnected.columns) == ["mrid"]
    assert len(disconnected) == 100


@need_cim_ssh
def test_ssh_conformed_load(sshmodel):
    load = sshmodel.ssh_load(rdf_types=["cim:ConformLoad"])
    assert list(load.columns) == ["p", "q"]
    assert len(load) == 1856


@need_cim_ssh
def test_ssh_nonconformed_load(sshmodel):
    load = sshmodel.ssh_load(rdf_types=["cim:NonConformLoad"])
    assert list(load.columns) == ["p", "q"]
    assert len(load) == 194


@need_cim_ssh
def test_ssh_combined_load(sshmodel):
    load = sshmodel.ssh_load()
    assert list(load.columns) == ["p", "q"]
    assert len(load) == 1856 + 194


@need_cim_ssh
def test_ssh_hydro_generating_unit(sshmodel):
    hydro = sshmodel.ssh_generating_unit(["cim:HydroGeneratingUnit"])
    assert np.all(hydro == 0.0)
    assert len(hydro) == 2177


@need_cim_ssh
def test_ssh_thermal_generating_unit(sshmodel):
    thermal = sshmodel.ssh_generating_unit(["cim:ThermalGeneratingUnit"])
    assert np.all(thermal == 0.0)
    assert len(thermal) == 40


@need_cim_ssh
def test_ssh_wind_generating_unit(sshmodel):
    wind = sshmodel.ssh_generating_unit(["cim:WindGeneratingUnit"])
    assert np.all(wind == 0.0)
    assert len(wind) == 71


@need_cim_ssh
def test_ssh_generating_unit_hydro_thermal(sshmodel):
    gen = sshmodel.ssh_generating_unit(["cim:HydroGeneratingUnit", "cim:ThermalGeneratingUnit"])
    assert np.all(gen == 0.0)
    assert len(gen) == 40 + 2177


@need_cim_ssh
def test_ssh_generating_unit_union_all(sshmodel):
    gen = sshmodel.ssh_generating_unit(
        ["cim:HydroGeneratingUnit", "cim:ThermalGeneratingUnit", "cim:WindGeneratingUnit"]
    )
    assert np.all(gen == 0.0)
    assert len(gen) == 40 + 2177 + 71


@need_cim_ssh
def test_ssh_generating_unit_union_all_default(sshmodel):
    gen = sshmodel.ssh_generating_unit()
    assert np.all(gen == 0.0)
    assert len(gen) == 40 + 2177 + 71
