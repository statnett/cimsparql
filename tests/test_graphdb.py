import logging
import os
from datetime import datetime
from typing import List
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import requests

import cimsparql.query_support as sup
from cimsparql.cim import ID_OBJ
from cimsparql.constants import con_mrid_str
from cimsparql.enums import ConverterTypes
from cimsparql.graphdb import (
    GraphDBClient,
    config_bytes_from_template,
    confpath,
    data_row,
    new_repo,
    repos,
)
from cimsparql.model import CimModel
from cimsparql.type_mapper import TypeMapperQueries

logger = logging.getLogger(__name__)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
@patch.object(CimModel, "get_table_and_convert")
def test_date_version(get_table_mock, cim_model: CimModel):
    t_ref = datetime(2020, 1, 1)
    get_table_mock.return_value = pd.DataFrame(
        {"col1": [1], "activationDate": [np.datetime64(t_ref)]}
    )
    assert cim_model.date_version == t_ref


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_cimversion(cim_model: CimModel):
    assert cim_model.cim_version == 15


load_columns = [con_mrid_str, "t_mrid", "bidzone", "p", "q", "name"]


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_conform_load(cim_model: CimModel, n_samples: int):
    load = cim_model.loads(load_type=["ConformLoad"], limit=n_samples)
    assert len(load) == n_samples
    assert set(load.columns).issubset(load_columns)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_non_conform_load(cim_model: CimModel, n_samples: int):
    load = cim_model.loads(load_type=["NonConformLoad"], limit=n_samples)
    assert len(load) == n_samples
    assert set(load.columns).issubset(load_columns)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_series_compensator(cim_model: CimModel):
    compensators = cim_model.series_compensators(region="NO", rates=["Normal"], limit=3)
    assert compensators.shape == (3, 8)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_series_compensator_with_market(cim_model: CimModel):
    compensators = cim_model.series_compensators(region="NO", rates=None, with_market=True, limit=3)
    assert compensators.shape == (3, 9)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_phase_tap_changer(cim_model: CimModel):
    tap_changers = cim_model.phase_tap_changers(region=None, dry_run=False)
    assert tap_changers.shape == (1, 11)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_conform_and_non_conform_load(cim_model: CimModel, n_samples: int):
    load = cim_model.loads(load_type=["ConformLoad", "NonConformLoad"], limit=n_samples)
    assert len(load) == n_samples
    assert set(load.columns).issubset(load_columns)


@pytest.fixture()
def gen_columns() -> List[str]:
    return [
        "allocationmax",
        "allocationWeight",
        "market_code",
        "maxP",
        "minP",
        "name",
        "station_group",
    ]


@pytest.fixture()
def synchronous_machines_columns(gen_columns: List[str]) -> List[str]:
    return gen_columns + ["bidzone", "p", "q", "sn", "t_mrid"]


@pytest.fixture()
def wind_units_machines_columns(gen_columns: List[str]) -> List[str]:
    return gen_columns + ["plant_mrid"]


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_synchronous_machines(
    cim_model: CimModel, synchronous_machines_columns: List[str], n_samples: int
):
    synchronous_machines = cim_model.synchronous_machines(limit=n_samples)
    assert len(synchronous_machines) == n_samples
    assert set(synchronous_machines.columns).difference(synchronous_machines_columns) == set()


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_wind_generating_units(
    cim_model: CimModel, wind_units_machines_columns: List[str], n_samples: int
):
    wind_units_machines = cim_model.wind_generating_units(limit=n_samples)
    assert len(wind_units_machines) == n_samples
    assert set(wind_units_machines.columns).difference(wind_units_machines_columns) == set()


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_regions(cim_model: CimModel):
    assert cim_model.regions.groupby("region").count()["shortName"]["NO"] > 16


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_branch(cim_model: CimModel, n_samples: int):
    lines = cim_model.ac_lines(limit=n_samples, length=True)
    assert lines.shape == (n_samples, 9)
    assert all(lines[["x", "un"]].dtypes == float)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_branch_with_temperatures(cim_model: CimModel, n_samples: int):
    lines = cim_model.ac_lines(limit=n_samples, rates=None, temperatures=range(-30, 30, 10))
    assert lines.shape == (n_samples, 13)
    assert all(lines[["x", "un"]].dtypes == float)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_branch_with_two_temperatures(cim_model: CimModel, n_samples: int):
    lines = cim_model.ac_lines(limit=n_samples, rates=None, temperatures=range(-20, 0, 10))
    assert lines.shape == (n_samples, 9)
    assert all(lines[["x", "un"]].dtypes == float)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_ac_line_segment_with_market(cim_model: CimModel, n_samples: int):
    lines = cim_model.ac_lines(limit=n_samples, with_market=True, rates=None, temperatures=None)
    assert lines.shape == (n_samples, 9)
    assert all(lines[["x", "un"]].dtypes == float)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_branch_with_connectivity(cim_model: CimModel, n_samples: int):
    lines = cim_model.ac_lines(
        limit=n_samples, connectivity=con_mrid_str, temperatures=range(0, 10, 10)
    )
    assert lines.shape == (n_samples, 11)
    assert all(lines[["x", "un"]].dtypes == float)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_transformers_with_multiple_sub_regions(cim_model: CimModel):
    windings = cim_model.transformers(region=[f"NO0{no}" for no in [1, 2, 3]], sub_region=True)
    assert windings.shape[0] > 2
    assert windings.shape[1] == 9


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_transformers_with_faseshift(cim_model: CimModel):
    tap_changers = cim_model.phase_tap_changers(region="SE")
    assert "w_mrid_1" in tap_changers.columns


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_windings(cim_model: CimModel):
    windings = cim_model.transformers(region="NO01", sub_region=True)
    assert windings.shape[1] == 9


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_windings_with_market(cim_model: CimModel):
    windings = cim_model.transformers(region="NO01", sub_region=True, with_market=True)
    assert windings.shape[1] == 10


def transformers(gdb: GraphDBClient) -> pd.DataFrame:
    """Information used by ptc"""
    select = "select ?mrid ?endNumber ?w_mrid "
    where_list = [
        "?mrid rdf:type cim:PowerTransformer",
        "?w_mrid cim:PowerTransformerEnd.PowerTransformer ?mrid",
        "?w_mrid cim:TransformerEnd.endNumber ?endNumber",
    ]
    return gdb.get_table(sup.combine_statements(select, sup.group_query(where_list)))


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_breaker_length(breakers: pd.DataFrame, n_samples: int):
    assert len(breakers) == n_samples


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_connectors_length(disconnectors: pd.DataFrame, n_samples: int):
    assert len(disconnectors) == n_samples


@pytest.fixture()
def corridor_columns() -> List[str]:
    return ["name", "t_mrid_1", "t_mrid_2", "area_1", "area_2"]


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_transformer_connected_to_voltage_source_converters(cim_model: CimModel):
    transformers = cim_model.transformers_connected_to_converter(
        region="NO", converter_types=[ConverterTypes.VoltageSourceConverter]
    )
    assert set(transformers.columns) == {"t_mrid", "name", "mrid"}
    assert len(transformers) == 10


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_transformer_connected_to_dc_converters(cim_model: CimModel):
    transformers = cim_model.transformers_connected_to_converter(
        region="NO", converter_types=[ConverterTypes.DCConverter]
    )
    assert set(transformers.columns) == {"t_mrid", "name", "mrid"}
    assert len(transformers) == 16


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_transformer_connected_to_converters(cim_model: CimModel):
    transformers = cim_model.transformers_connected_to_converter(region="NO")
    assert set(transformers.columns) == {"t_mrid", "name", "mrid"}
    assert len(transformers) == 26


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_borders_no(cim_model: CimModel, corridor_columns: List[str]):
    borders = cim_model.borders(region="NO", limit=10)
    assert set(borders.columns).difference(corridor_columns) == set()
    assert len(borders) == 10
    assert (borders[["area_1", "area_2"]] == "NO").any(axis=1).all()
    assert (borders["area_1"] != borders["area_2"]).all()


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_borders_no_se(cim_model: CimModel, corridor_columns: List[str]):
    borders = cim_model.borders(region=["NO", "SE"])
    assert set(borders.columns).difference(corridor_columns) == set()
    assert (borders[["area_1", "area_2"]].isin(["NO", "SE"])).any(axis=1).all()
    assert (borders["area_1"] != borders["area_2"]).all()


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_data_row():
    cols = ["a", "b", "c", "d", "e"]
    rows = [{"a": 1, "b": 2}, {"c": 3, "d": 4}, {"a": 5, "b": 6}, {"e": 7}]
    assert not set(data_row(cols, rows)).symmetric_difference(cols)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_data_row_missing_column():
    cols = ["a", "b", "c", "d", "e"]
    rows = [{"a": 1, "b": 2}, {"c": 3}, {"a": 5, "b": 6}, {"e": 7}]
    assert set(data_row(cols, rows).keys()).symmetric_difference(cols) == {"d"}


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_dtypes(cim_model: CimModel):
    queries = TypeMapperQueries(cim_model.client.prefixes.prefixes)
    df = cim_model.client.get_table(queries.query)[0]
    assert df["sparql_type"].isna().sum() == 0


def test_prefix_resp_not_ok(monkeypatch):
    resp = requests.Response()
    resp.status_code = 401
    resp.reason = "Something went wrong"
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: resp)

    with pytest.raises(RuntimeError) as exc:
        GraphDBClient("http://some-url:87").get_prefixes()
    assert resp.reason in str(exc)
    assert str(resp.status_code) in str(exc)


def test_conf_bytes_from_template():
    template = confpath() / "native_store_config_template.ttl"

    conf_bytes = config_bytes_from_template(template, {"repo": "test_repo"})
    conf_str = conf_bytes.decode("utf8")
    assert 'rep:repositoryID "test_repo"' in conf_str


def test_create_delete_repo(rdf4j_url):
    repo = "test_repo"
    template = confpath() / "native_store_config_template.ttl"
    conf_bytes = config_bytes_from_template(template, {"repo": repo})
    url_with_protocol = "http://" + rdf4j_url
    current_repos = repos(url_with_protocol)

    assert "test_repo" not in [i.repo_id for i in current_repos]

    # protocol is added internally. Thus, skip from rdf4j_url
    client = new_repo(rdf4j_url, repo, conf_bytes, protocol="http")
    current_repos = repos(url_with_protocol)
    assert "test_repo" in [i.repo_id for i in current_repos]

    client.delete_repo()
    current_repos = repos(url_with_protocol)
    assert "test_repo" not in [i.repo_id for i in current_repos]


def test_add_mrid(micro_t1_nl):
    if not micro_t1_nl and not os.getenv("CI"):
        pytest.skip("Require RDF4J access")

    query = f"SELECT ?mrid WHERE {{?s {ID_OBJ}.mRID ?mrid}}"
    res = micro_t1_nl.client.exec_query(query)
    assert len(res) == 0

    micro_t1_nl.add_mrid("cim:TopologicalNode")
    res = micro_t1_nl.client.exec_query(query)
    assert len(res) == 5


def test_update_prefixes(monkeypatch):
    monkeypatch.setattr(GraphDBClient, "get_prefixes", lambda *args: {})
    client = GraphDBClient("http://some.url")
    assert client.prefixes.prefixes == {}

    new_pref = {"eq": "http://eq"}
    client.update_prefixes(new_pref)
    assert client.prefixes.prefixes == new_pref
