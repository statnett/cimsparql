import os

import pytest

from cimsparql.graphdb import GraphDBClient


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_bus_data(gdb_cli: GraphDBClient):
    bus_data_sql = gdb_cli.bus_data(limit=10, dry_run=True)
    assert isinstance(bus_data_sql, str)
    assert "PREFIX" in bus_data_sql
    assert "SELECT" in bus_data_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_loads(gdb_cli: GraphDBClient):
    loads_sql = gdb_cli.loads(load_type=["ConformLoad"], limit=10, dry_run=True)
    assert isinstance(loads_sql, str)
    assert "PREFIX" in loads_sql
    assert "SELECT" in loads_sql
    assert "ConformLoad" in loads_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_wind_generating_units(gdb_cli: GraphDBClient):
    wind_generating_units_sql = gdb_cli.wind_generating_units(limit=10, dry_run=True)
    assert isinstance(wind_generating_units_sql, str)
    assert "PREFIX" in wind_generating_units_sql
    assert "SELECT" in wind_generating_units_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_synchronous_machines(gdb_cli: GraphDBClient):
    synchronous_machines_sql = gdb_cli.synchronous_machines(limit=10, dry_run=True)
    assert isinstance(synchronous_machines_sql, str)
    assert "PREFIX" in synchronous_machines_sql
    assert "SELECT" in synchronous_machines_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_connections(gdb_cli: GraphDBClient):
    connections_sql = gdb_cli.connections(limit=10, dry_run=True)
    assert isinstance(connections_sql, str)
    assert "PREFIX" in connections_sql
    assert "SELECT" in connections_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_ac_lines(gdb_cli: GraphDBClient):
    ac_lines_sql = gdb_cli.ac_lines(limit=10, dry_run=True)
    assert isinstance(ac_lines_sql, str)
    assert "PREFIX" in ac_lines_sql
    assert "SELECT" in ac_lines_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_series_compensators(gdb_cli: GraphDBClient):
    series_compensators_sql = gdb_cli.series_compensators(limit=10, dry_run=True)
    assert isinstance(series_compensators_sql, str)
    assert "PREFIX" in series_compensators_sql
    assert "SELECT" in series_compensators_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_transformers(gdb_cli: GraphDBClient):
    transformers_sql = gdb_cli.transformers(limit=10, dry_run=True)
    assert isinstance(transformers_sql, str)
    assert "PREFIX" in transformers_sql
    assert "SELECT" in transformers_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_disconnected(gdb_cli: GraphDBClient):
    disconnected_sql = gdb_cli.disconnected(limit=10, dry_run=True)
    assert isinstance(disconnected_sql, str)
    assert "PREFIX" in disconnected_sql
    assert "SELECT" in disconnected_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_ssh_synchronous_machines(gdb_cli: GraphDBClient):
    ssh_synchronous_machines_sql = gdb_cli.ssh_synchronous_machines(limit=10, dry_run=True)
    assert isinstance(ssh_synchronous_machines_sql, str)
    assert "PREFIX" in ssh_synchronous_machines_sql
    assert "SELECT" in ssh_synchronous_machines_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_ssh_load(gdb_cli: GraphDBClient):
    ssh_load_sql = gdb_cli.ssh_load(limit=10, dry_run=True)
    assert isinstance(ssh_load_sql, str)
    assert "PREFIX" in ssh_load_sql
    assert "SELECT" in ssh_load_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_ssh_generating_unit(gdb_cli: GraphDBClient):
    ssh_generating_unit_sql = gdb_cli.ssh_generating_unit(limit=10, dry_run=True)
    assert isinstance(ssh_generating_unit_sql, str)
    assert "PREFIX" in ssh_generating_unit_sql
    assert "SELECT" in ssh_generating_unit_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_terminal(gdb_cli: GraphDBClient):
    terminal_sql = gdb_cli.terminal(limit=10, dry_run=True)
    assert isinstance(terminal_sql, str)
    assert "PREFIX" in terminal_sql
    assert "SELECT" in terminal_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_topological_node(gdb_cli: GraphDBClient):
    topological_node_sql = gdb_cli.topological_node(limit=10, dry_run=True)
    assert isinstance(topological_node_sql, str)
    assert "PREFIX" in topological_node_sql
    assert "SELECT" in topological_node_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_powerflow(gdb_cli: GraphDBClient):
    powerflow_sql = gdb_cli.powerflow(limit=10, dry_run=True)
    assert isinstance(powerflow_sql, str)
    assert "PREFIX" in powerflow_sql
    assert "SELECT" in powerflow_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_voltage(gdb_cli: GraphDBClient):
    voltage_sql = gdb_cli.voltage(limit=10, dry_run=True)
    assert isinstance(voltage_sql, str)
    assert "PREFIX" in voltage_sql
    assert "SELECT" in voltage_sql


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_tapstep(gdb_cli: GraphDBClient):
    tapstep_sql = gdb_cli.tapstep(limit=10, dry_run=True)
    assert isinstance(tapstep_sql, str)
    assert "PREFIX" in tapstep_sql
    assert "SELECT" in tapstep_sql
