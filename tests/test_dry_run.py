import os

import pytest

from cimsparql.model import CimModel


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_bus_data(cim_model: CimModel):
    bus_data_sql = cim_model.bus_data(limit=10, dry_run=True)
    assert isinstance(bus_data_sql, str)
    assert "PREFIX" in bus_data_sql
    assert "SELECT" in bus_data_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_loads(cim_model: CimModel):
    loads_sql = cim_model.loads(load_type=["ConformLoad"], limit=10, dry_run=True)
    assert isinstance(loads_sql, str)
    assert "PREFIX" in loads_sql
    assert "SELECT" in loads_sql
    assert "ConformLoad" in loads_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_wind_generating_units(cim_model: CimModel):
    wind_generating_units_sql = cim_model.wind_generating_units(limit=10, dry_run=True)
    assert isinstance(wind_generating_units_sql, str)
    assert "PREFIX" in wind_generating_units_sql
    assert "SELECT" in wind_generating_units_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_synchronous_machines(cim_model: CimModel):
    synchronous_machines_sql = cim_model.synchronous_machines(limit=10, dry_run=True)
    assert isinstance(synchronous_machines_sql, str)
    assert "PREFIX" in synchronous_machines_sql
    assert "SELECT" in synchronous_machines_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_connections(cim_model: CimModel):
    connections_sql = cim_model.connections(limit=10, dry_run=True)
    assert isinstance(connections_sql, str)
    assert "PREFIX" in connections_sql
    assert "SELECT" in connections_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_ac_lines(cim_model: CimModel):
    ac_lines_sql = cim_model.ac_lines(limit=10, dry_run=True)
    assert isinstance(ac_lines_sql, str)
    assert "PREFIX" in ac_lines_sql
    assert "SELECT" in ac_lines_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_series_compensators(cim_model: CimModel):
    series_compensators_sql = cim_model.series_compensators(limit=10, dry_run=True)
    assert isinstance(series_compensators_sql, str)
    assert "PREFIX" in series_compensators_sql
    assert "SELECT" in series_compensators_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_transformers(cim_model: CimModel):
    transformers_sql = cim_model.transformers(limit=10, dry_run=True)
    assert isinstance(transformers_sql, str)
    assert "PREFIX" in transformers_sql
    assert "SELECT" in transformers_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_disconnected(cim_model: CimModel):
    disconnected_sql = cim_model.disconnected(limit=10, dry_run=True)
    assert isinstance(disconnected_sql, str)
    assert "PREFIX" in disconnected_sql
    assert "SELECT" in disconnected_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_ssh_synchronous_machines(cim_model: CimModel):
    ssh_synchronous_machines_sql = cim_model.ssh_synchronous_machines(limit=10, dry_run=True)
    assert isinstance(ssh_synchronous_machines_sql, str)
    assert "PREFIX" in ssh_synchronous_machines_sql
    assert "SELECT" in ssh_synchronous_machines_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_ssh_load(cim_model: CimModel):
    ssh_load_sql = cim_model.ssh_load(limit=10, dry_run=True)
    assert isinstance(ssh_load_sql, str)
    assert "PREFIX" in ssh_load_sql
    assert "SELECT" in ssh_load_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_ssh_generating_unit(cim_model: CimModel):
    ssh_generating_unit_sql = cim_model.ssh_generating_unit(limit=10, dry_run=True)
    assert isinstance(ssh_generating_unit_sql, str)
    assert "PREFIX" in ssh_generating_unit_sql
    assert "SELECT" in ssh_generating_unit_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_terminal(cim_model: CimModel):
    terminal_sql = cim_model.terminal(limit=10, dry_run=True)
    assert isinstance(terminal_sql, str)
    assert "PREFIX" in terminal_sql
    assert "SELECT" in terminal_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_topological_node(cim_model: CimModel):
    topological_node_sql = cim_model.topological_node(limit=10, dry_run=True)
    assert isinstance(topological_node_sql, str)
    assert "PREFIX" in topological_node_sql
    assert "SELECT" in topological_node_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_powerflow(cim_model: CimModel):
    powerflow_sql = cim_model.powerflow(limit=10, dry_run=True)
    assert isinstance(powerflow_sql, str)
    assert "PREFIX" in powerflow_sql
    assert "SELECT" in powerflow_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_voltage(cim_model: CimModel):
    voltage_sql = cim_model.voltage(limit=10, dry_run=True)
    assert isinstance(voltage_sql, str)
    assert "PREFIX" in voltage_sql
    assert "SELECT" in voltage_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_tapstep(cim_model: CimModel):
    tapstep_sql = cim_model.tapstep(limit=10, dry_run=True)
    assert isinstance(tapstep_sql, str)
    assert "PREFIX" in tapstep_sql
    assert "SELECT" in tapstep_sql
