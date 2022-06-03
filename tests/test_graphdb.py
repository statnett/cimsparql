import os
from datetime import datetime
from typing import List
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import requests

from cimsparql.constants import con_mrid_str
from cimsparql.graphdb import GraphDBClient, data_row
from cimsparql.type_mapper import TypeMapperQueries


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
@patch.object(GraphDBClient, "_get_table_and_convert")
def test_date_version(get_table_mock, gdb_cli: GraphDBClient):
    t_ref = datetime(2020, 1, 1)
    get_table_mock.return_value = pd.DataFrame(
        {"col1": [1], "activationDate": [np.datetime64(t_ref)]}
    )
    assert gdb_cli.date_version == t_ref


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_cimversion(gdb_cli: GraphDBClient):
    assert gdb_cli.cim_version == 15


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_str_rep(gdb_cli: GraphDBClient, graphdb_service: str):
    target = f"<GraphDBClient object, service: {graphdb_service}>"
    assert str(gdb_cli) == target


load_columns = [con_mrid_str, "t_mrid", "bidzone", "p", "q"]


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_conform_load(gdb_cli: GraphDBClient, n_samples: int):
    load = gdb_cli.loads(load_type=["ConformLoad"], limit=n_samples)
    assert len(load) == n_samples
    assert set(load.columns).issubset(load_columns)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_non_conform_load(gdb_cli: GraphDBClient, n_samples: int):
    load = gdb_cli.loads(load_type=["NonConformLoad"], limit=n_samples)
    assert len(load) == n_samples
    assert set(load.columns).issubset(load_columns)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_series_compensator(gdb_cli: GraphDBClient):
    compensators = gdb_cli.series_compensators(region="NO", limit=3)
    assert compensators.shape == (3, 6)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_series_compensator_with_market(gdb_cli: GraphDBClient):
    compensators = gdb_cli.series_compensators(region="NO", limit=3, with_market=True)
    assert compensators.shape == (3, 8)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_phase_tap_changer(gdb_cli: GraphDBClient):
    tap_changers = gdb_cli.phase_tap_changers(region=None, dry_run=False)
    assert tap_changers.shape == (1, 11)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_conform_and_non_conform_load(gdb_cli: GraphDBClient, n_samples: int):
    load = gdb_cli.loads(load_type=["ConformLoad", "NonConformLoad"], limit=n_samples)
    assert len(load) == n_samples
    assert set(load.columns).issubset(load_columns)


@pytest.fixture()
def gen_columns() -> List[str]:
    return [
        "allocationMax",
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
    gdb_cli: GraphDBClient, synchronous_machines_columns: List[str], n_samples: int
):
    synchronous_machines = gdb_cli.synchronous_machines(limit=n_samples)
    assert len(synchronous_machines) == n_samples
    assert set(synchronous_machines.columns).difference(synchronous_machines_columns) == set()


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_wind_generating_units(
    gdb_cli: GraphDBClient, wind_units_machines_columns: List[str], n_samples: int
):
    wind_units_machines = gdb_cli.wind_generating_units(limit=n_samples)
    assert len(wind_units_machines) == n_samples
    assert set(wind_units_machines.columns).difference(wind_units_machines_columns) == set()


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_regions(gdb_cli: GraphDBClient):
    assert gdb_cli.regions.groupby("region").count()["shortName"]["NO"] > 16


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_branch(gdb_cli: GraphDBClient, n_samples: int):
    lines = gdb_cli.ac_lines(limit=n_samples)
    assert lines.shape == (n_samples, 11)
    assert all(lines[["x", "un"]].dtypes == float)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_branch_with_temperatures(gdb_cli: GraphDBClient, n_samples: int):
    lines = gdb_cli.ac_lines(limit=n_samples, rates=None, temperatures=range(-30, 30, 10))
    assert lines.shape == (n_samples, 14)
    assert all(lines[["x", "un"]].dtypes == float)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_branch_with_two_temperatures(gdb_cli: GraphDBClient, n_samples: int):
    lines = gdb_cli.ac_lines(limit=n_samples, rates=None, temperatures=range(-20, 0, 10))
    assert lines.shape == (n_samples, 10)
    assert all(lines[["x", "un"]].dtypes == float)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_ac_line_segment_with_market(gdb_cli: GraphDBClient, n_samples: int):
    lines = gdb_cli.ac_lines(limit=n_samples, with_market=True, rates=None, temperatures=None)
    assert lines.shape == (n_samples, 10)
    assert all(lines[["x", "un"]].dtypes == float)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_branch_with_connectivity(gdb_cli: GraphDBClient, n_samples: int):
    lines = gdb_cli.ac_lines(
        limit=n_samples, connectivity=con_mrid_str, temperatures=range(0, 10, 10)
    )
    assert lines.shape == (n_samples, 14)
    assert all(lines[["x", "un"]].dtypes == float)


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_transformers_with_multiple_sub_regions(gdb_cli: GraphDBClient):
    windings = gdb_cli.transformers(region=[f"NO0{no}" for no in [1, 2, 3]], sub_region=True)
    assert windings.shape[0] > 2
    assert windings.shape[1] == 11


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_transformers_with_faseshift(gdb_cli: GraphDBClient):
    tap_changers = gdb_cli.phase_tap_changers(region="SE")
    assert "w_mrid_1" in tap_changers.columns


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_windings(gdb_cli: GraphDBClient):
    windings = gdb_cli.transformers(region="NO01", sub_region=True)
    assert windings.shape[1] == 11


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_windings_with_market(gdb_cli: GraphDBClient):
    windings = gdb_cli.transformers(region="NO01", sub_region=True, with_market=True)
    assert windings.shape[1] == 12


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
def test_transformer_connected_to_voltage_source_converters(gdb_cli: GraphDBClient):
    transformers = gdb_cli.transformers_connected_to_converter(
        region="NO", converter_types=["VoltageSource"]
    )
    assert set(transformers.columns).difference(["t_mrid", "name"]) == set()
    assert len(transformers) == 10


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_transformer_connected_to_dc_converters(gdb_cli: GraphDBClient):
    transformers = gdb_cli.transformers_connected_to_converter(region="NO", converter_types=["DC"])
    assert set(transformers.columns).difference(["t_mrid", "name"]) == set()
    assert len(transformers) == 16


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_transformer_connected_to_converters(gdb_cli: GraphDBClient):
    transformers = gdb_cli.transformers_connected_to_converter(region="NO")
    assert set(transformers.columns).difference(["t_mrid", "name"]) == set()
    assert len(transformers) == 26


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_borders_no(gdb_cli: GraphDBClient, corridor_columns: List[str]):
    borders = gdb_cli.borders(region="NO", limit=10)
    assert set(borders.columns).difference(corridor_columns) == set()
    assert len(borders) == 10
    assert (borders[["area_1", "area_2"]] == "NO").any(axis=1).all()
    assert (borders["area_1"] != borders["area_2"]).all()


@pytest.mark.skipif(os.getenv("GRAPHDB_API", None) is None, reason="Need graphdb server to run")
def test_borders_no_se(gdb_cli: GraphDBClient, corridor_columns: List[str]):
    borders = gdb_cli.borders(region=["NO", "SE"])
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
def test_dtypes(gdb_cli: GraphDBClient):
    queries = TypeMapperQueries(gdb_cli.prefixes)
    df = gdb_cli.get_table(queries.query, map_data_types=False)
    assert df["sparql_type"].isna().sum() == 0


def test_prefix_resp_not_ok(monkeypatch):
    resp = requests.Response()
    resp.status_code = 401
    resp.reason = "Something went wrong"
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: resp)

    with pytest.raises(RuntimeError) as exc:
        GraphDBClient("http://some-url:87")

    assert resp.reason in str(exc)
    assert str(resp.status_code) in str(exc)
