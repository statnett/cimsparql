import os
from typing import List, Set

import pytest
import requests

from cimsparql.graphdb import (
    GraphDBClient,
    ServiceConfig,
    config_bytes_from_template,
    confpath,
    data_row,
    new_repo,
    repos,
)
from cimsparql.model import CimModel
from cimsparql.type_mapper import TypeMapper

ID_OBJ = "cim:IdentifiedObject"


@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER") is None, reason="Need graphdb server to run")
def test_cimversion(model: CimModel):
    assert model.cim_version == 16


@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER") is None, reason="Need graphdb server to run")
def test_load(model: CimModel):
    load = model.loads()
    assert set(load.columns).issubset(
        {"node", "name", "bidzone", "p", "q", "station", "station_group", "status"}
    )


@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER") is None, reason="Need graphdb server to run")
def test_power_flow(model: CimModel):
    assert not model.powerflow.empty


@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER") is None, reason="Need graphdb server to run")
def test_series_compensator(model: CimModel):
    compensators = model.series_compensators(region="NO")
    assert compensators.shape[1] == 12


@pytest.fixture()
def gen_columns() -> List[str]:
    return {
        "name",
        "allocationmax",
        "node",
        "status",
        "station_group",
        "station_group_name",
        "station",
        "market_code",
        "maxP",
        "minP",
        "MO",
        "bidzone",
        "sn",
        "p",
    }


@pytest.fixture()
def synchronous_machines_columns(gen_columns: Set[str]) -> Set[str]:
    return gen_columns | {"bidzone", "p", "q", "sn", "t_mrid", "station"}


@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER") is None, reason="Need graphdb server to run")
def test_synchronous_machines(model: CimModel, synchronous_machines_columns: Set[str]):
    synchronous_machines = model.synchronous_machines()
    assert not synchronous_machines.empty
    assert set(synchronous_machines.columns).difference(synchronous_machines_columns) == set()


@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER") is None, reason="Need graphdb server to run")
def test_wind_generating_units(model: CimModel):
    wind_units_machines = model.wind_generating_units()
    assert not wind_units_machines.empty


@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER") is None, reason="Need graphdb server to run")
def test_connections(model: CimModel):
    assert not model.connections().empty


@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER") is None, reason="Need graphdb server to run")
def test_regions(model: CimModel):
    assert model.regions.groupby("region").count().loc["NO", "name"] > 16


@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER") is None, reason="Need graphdb server to run")
def test_ac_lines(model: CimModel):
    lines = model.ac_lines()
    assert lines.shape[1] == 15
    assert all(lines[["x", "un"]].dtypes == float)


@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER") is None, reason="Need graphdb server to run")
def test_windings(model: CimModel):
    windings = model.transformers(region="NO")
    assert windings.shape[1] == 9


@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER") is None, reason="Need graphdb server to run")
def test_transformer_connected_to_converters(model: CimModel):
    transformers = model.transformers_connected_to_converter(region="NO")
    assert set(transformers.columns) == {"t_mrid", "name", "p_mrid"}
    assert not transformers.empty


@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER") is None, reason="Need graphdb server to run")
def test_borders_no(model: CimModel):
    borders = model.borders(region="NO")
    assert {"name", "t_mrid_1", "t_mrid_2", "area_1", "area_2", "market_code"}.issuperset(
        borders.columns
    )
    assert not borders.empty
    assert (borders[["area_1", "area_2"]] == "NO").any(axis=1).all()
    assert (borders["area_1"] != borders["area_2"]).all()


@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER") is None, reason="Need graphdb server to run")
def test_substation_voltage_level(model: CimModel):
    voltage_level = model.substation_voltage_level
    assert {"container", "v"}.difference(voltage_level.columns) == set()


@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER") is None, reason="Need graphdb server to run")
def test_station_group_codes_and_names(model: CimModel):
    st_group_names = model.station_group_codes_and_names
    assert not st_group_names.empty


@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER") is None, reason="Need graphdb server to run")
def test_dc_active_flow(model: CimModel):
    flow = model.dc_active_flow()
    assert not flow.empty


@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER") is None, reason="Need graphdb server to run")
def test_data_row():
    cols = ["a", "b", "c", "d", "e"]
    rows = [{"a": 1, "b": 2}, {"c": 3, "d": 4}, {"a": 5, "b": 6}, {"e": 7}]
    assert not set(data_row(cols, rows)).symmetric_difference(cols)


@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER") is None, reason="Need graphdb server to run")
def test_data_row_missing_column():
    cols = ["a", "b", "c", "d", "e"]
    rows = [{"a": 1, "b": 2}, {"c": 3}, {"a": 5, "b": 6}, {"e": 7}]
    assert set(data_row(cols, rows).keys()).symmetric_difference(cols) == {"d"}


@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER") is None, reason="Need graphdb server to run")
def test_dtypes(model: CimModel):
    mapper = TypeMapper(model.client)
    df = model.client.get_table(mapper.query)[0]
    assert df["sparql_type"].isna().sum() == 0


def test_prefix_resp_not_ok(monkeypatch):
    resp = requests.Response()
    resp.status_code = 401
    resp.reason = "Something went wrong"
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: resp)

    with pytest.raises(RuntimeError) as exc:
        GraphDBClient(ServiceConfig(server="some-serever")).get_prefixes()
    assert resp.reason in str(exc)
    assert str(resp.status_code) in str(exc)


def test_conf_bytes_from_template():
    template = confpath() / "native_store_config_template.ttl"

    conf_bytes = config_bytes_from_template(template, {"repo": "test_repo"})
    conf_str = conf_bytes.decode("utf8")
    assert 'rep:repositoryID "test_repo"' in conf_str


def test_create_delete_repo(rdf4j_url):
    if not rdf4j_url:
        pytest.skip("require rdf4j")
    repo = "test_repo"
    template = confpath() / "native_store_config_template.ttl"
    conf_bytes = config_bytes_from_template(template, {"repo": repo})
    service_cfg = ServiceConfig(repo, "http", rdf4j_url)
    current_repos = repos(service_cfg)

    assert "test_repo" not in [i.repo_id for i in current_repos]

    # protocol is added internally. Thus, skip from rdf4j_url
    client = new_repo(rdf4j_url, repo, conf_bytes, protocol="http")
    current_repos = repos(service_cfg)
    assert "test_repo" in [i.repo_id for i in current_repos]

    client.delete_repo()
    current_repos = repos(service_cfg)
    assert "test_repo" not in [i.repo_id for i in current_repos]


def test_update_prefixes(monkeypatch):
    monkeypatch.setattr(GraphDBClient, "get_prefixes", lambda *args: {})
    client = GraphDBClient(ServiceConfig(server="some-server"))
    assert client.prefixes == {}

    new_pref = {"eq": "http://eq"}
    client.update_prefixes(new_pref)
    assert client.prefixes == new_pref
