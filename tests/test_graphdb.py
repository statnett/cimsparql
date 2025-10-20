import asyncio
import logging
import os
import re
from base64 import b64encode
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from functools import lru_cache
from http import HTTPStatus
from typing import Any, ParamSpec, TypeVar

import httpx
import pandas as pd
import pytest
from pytest_httpserver import HeaderValueMatcher, HTTPServer
from SPARQLWrapper import SPARQLWrapper

import cimsparql.data_models as cim_dm
import tests.t_utils.common as t_common
import tests.t_utils.custom_models as t_custom
from cimsparql.graphdb import (
    GraphDBClient,
    RepoInfo,
    RestApi,
    ServiceConfig,
    config_bytes_from_template,
    confpath,
    data_row,
    new_repo,
    repos,
)
from cimsparql.model import Model, SingleClientModel
from cimsparql.sparql_result_json import SparqlResultJsonFactory, SparqlResultValue
from cimsparql.type_mapper import TypeMapper

logger = logging.getLogger()

T = TypeVar("T")
P = ParamSpec("P")
R = TypeVar("R")


def exception_logging(func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
    logger.debug("Starting %s", func.__name__)
    try:
        return func(*args, **kwargs)
    except Exception:
        logger.exception("Exception for %s", func.__name__)
        return R() if callable(R) else R


@dataclass
class DataResult:
    ac_lines: cim_dm.AcLinesDataFrame
    associated_switches: cim_dm.AssociatedSwitchesDataFrame
    base_voltage: cim_dm.BaseVoltageDataFrame
    borders: cim_dm.BordersDataFrame
    bus_data: cim_dm.BusDataFrame
    connections: cim_dm.ConnectionsDataFrame
    connectivity_nodes: cim_dm.ConnectivityNodeDataFrame
    converters: cim_dm.ConvertersDataFrame
    coordinates: cim_dm.CoordinatesDataFrame
    dc_active_flow: cim_dm.DcActiveFlowDataFrame
    disconnected: cim_dm.DisconnectedDataFrame
    exchange: cim_dm.ExchangeDataFrame
    full_model: cim_dm.FullModelDataFrame
    gen_unit_and_sync_machine_mrid: cim_dm.GenUnitAndSyncMachineMridDataFrame
    hvdc_converter_bidzones: cim_dm.HVDCBidzonesDataFrame
    loads: cim_dm.LoadsDataFrame
    market_dates: cim_dm.MarketDatesDataFrame
    phase_tap_changer: cim_dm.PhaseTapChangerDataFrame
    powerflow: cim_dm.PowerFlowDataFrame
    regions: cim_dm.RegionsDataFrame
    series_compensators: cim_dm.BranchComponentDataFrame
    station_group_codes_and_names: cim_dm.StationGroupCodeNameDataFrame
    substation_voltage_level: cim_dm.SubstationVoltageDataFrame
    station_group_for_power_unit: cim_dm.StationGroupForPowerUnitDataFrame
    sv_power_deviation: cim_dm.SvPowerDeviationDataFrame
    switches: cim_dm.SwitchesDataFrame
    synchronous_machines: cim_dm.SynchronousMachinesDataFrame
    transformer_branches: cim_dm.TransformerWindingDataFrame
    transformer_windings: cim_dm.TransformerWindingsDataFrame
    transformers: cim_dm.TransformersDataFrame
    transformers_connected_to_converter: cim_dm.TransfConToConverterDataFrame
    transformer_center_nodes: cim_dm.BusDataFrame
    wind_generating_units: cim_dm.WindGeneratingUnitsDataFrame


@lru_cache
def all_data(model: Model) -> DataResult:
    return asyncio.run(collect_data(model))


async def collect_data(model: Model) -> DataResult:
    loop = asyncio.get_event_loop()

    with ThreadPoolExecutor() as executor:
        ac_line_future = loop.run_in_executor(executor, model.ac_lines)
        associated_switches_future = loop.run_in_executor(executor, model.associated_switches)
        base_voltage_future = loop.run_in_executor(executor, model.base_voltage)
        borders_future = loop.run_in_executor(executor, model.borders)
        bus_data_future = loop.run_in_executor(executor, model.bus_data)
        connections_future = loop.run_in_executor(executor, model.connections)
        connectivity_nodes_future = loop.run_in_executor(executor, model.connectivity_nodes)
        converters_future = loop.run_in_executor(executor, model.converters)
        coordinates_future = loop.run_in_executor(executor, model.coordinates)
        dc_active_flow_future = loop.run_in_executor(executor, model.dc_active_flow)
        disconnected_future = loop.run_in_executor(executor, model.disconnected)
        exchange_future = loop.run_in_executor(executor, model.exchange)
        full_model_future = loop.run_in_executor(executor, model.full_model)
        gen_unit_and_sync_machine_mrid_future = loop.run_in_executor(executor, model.gen_unit_and_sync_machine_mrid)
        hvdc_converter_bidzones_future = loop.run_in_executor(executor, model.hvdc_converter_bidzones)
        loads_future = loop.run_in_executor(executor, model.loads)
        market_dates_future = loop.run_in_executor(executor, model.market_dates)
        phase_tap_changer_future = loop.run_in_executor(executor, model.phase_tap_changer)
        powerflow_future = loop.run_in_executor(executor, model.powerflow)
        regions_future = loop.run_in_executor(executor, model.regions)
        series_compensators_future = loop.run_in_executor(executor, model.series_compensators)
        station_group_codes_and_names_future = loop.run_in_executor(executor, model.station_group_codes_and_names)
        substation_voltage_level_future = loop.run_in_executor(executor, model.substation_voltage_level)
        station_group_for_power_unit_future = loop.run_in_executor(executor, model.station_group_for_power_unit)
        sv_power_deviation_future = loop.run_in_executor(executor, model.sv_power_deviation)
        switches_future = loop.run_in_executor(executor, model.switches)
        synchronous_machines_future = loop.run_in_executor(executor, model.synchronous_machines)
        transformer_branches_future = loop.run_in_executor(executor, model.transformer_branches)
        transformer_windings_future = loop.run_in_executor(executor, model.transformer_windings)
        transformers_future = loop.run_in_executor(executor, model.transformers)
        transformers_connected_to_converter_future = loop.run_in_executor(
            executor, model.transformers_connected_to_converter
        )
        transformer_center_nodes_future = loop.run_in_executor(executor, model.transformer_center_nodes)
        wind_generating_units_future = loop.run_in_executor(executor, model.wind_generating_units)

    await asyncio.wait(
        [
            ac_line_future,
            associated_switches_future,
            base_voltage_future,
            borders_future,
            bus_data_future,
            connections_future,
            connectivity_nodes_future,
            converters_future,
            coordinates_future,
            dc_active_flow_future,
            disconnected_future,
            exchange_future,
            full_model_future,
            gen_unit_and_sync_machine_mrid_future,
            hvdc_converter_bidzones_future,
            loads_future,
            market_dates_future,
            phase_tap_changer_future,
            powerflow_future,
            regions_future,
            series_compensators_future,
            station_group_codes_and_names_future,
            substation_voltage_level_future,
            station_group_for_power_unit_future,
            sv_power_deviation_future,
            switches_future,
            synchronous_machines_future,
            transformer_branches_future,
            transformer_windings_future,
            transformers_future,
            transformers_connected_to_converter_future,
            transformer_center_nodes_future,
            wind_generating_units_future,
        ]
    )
    return DataResult(
        ac_lines=ac_line_future.result(),
        associated_switches=associated_switches_future.result(),
        base_voltage=base_voltage_future.result(),
        borders=borders_future.result(),
        bus_data=bus_data_future.result(),
        connections=connections_future.result(),
        connectivity_nodes=connectivity_nodes_future.result(),
        converters=converters_future.result(),
        coordinates=coordinates_future.result(),
        dc_active_flow=dc_active_flow_future.result(),
        disconnected=disconnected_future.result(),
        exchange=exchange_future.result(),
        full_model=full_model_future.result(),
        gen_unit_and_sync_machine_mrid=gen_unit_and_sync_machine_mrid_future.result(),
        hvdc_converter_bidzones=hvdc_converter_bidzones_future.result(),
        loads=loads_future.result(),
        market_dates=market_dates_future.result(),
        phase_tap_changer=phase_tap_changer_future.result(),
        powerflow=powerflow_future.result(),
        regions=regions_future.result(),
        series_compensators=series_compensators_future.result(),
        station_group_codes_and_names=station_group_codes_and_names_future.result(),
        substation_voltage_level=substation_voltage_level_future.result(),
        station_group_for_power_unit=station_group_for_power_unit_future.result(),
        sv_power_deviation=sv_power_deviation_future.result(),
        switches=switches_future.result(),
        synchronous_machines=synchronous_machines_future.result(),
        transformer_branches=transformer_branches_future.result(),
        transformer_windings=transformer_windings_future.result(),
        transformers=transformers_future.result(),
        transformers_connected_to_converter=transformers_connected_to_converter_future.result(),
        transformer_center_nodes=transformer_center_nodes_future.result(),
        wind_generating_units=wind_generating_units_future.result(),
    )


@pytest.mark.parametrize("test_model", t_custom.all_custom_models())
def test_not_empty(test_model: t_common.ModelTest):
    model = test_model.model
    if not model:
        pytest.skip("Require access to GraphDB")
    dfs = all_data(model)

    for name, df in asdict(dfs).items():
        assert not df.empty, f"Failed for dataframe {name}"


@pytest.fixture
def model() -> Model:
    test_model = t_custom.federated_model()
    if not test_model.model:
        pytest.skip("Require access to GraphDB")
    return test_model.model


def test_cimversion(model: SingleClientModel):
    assert model.cim_version == 16


def test_ltc_fixed_angle_equals_zero(model: SingleClientModel):
    branches = model.get_table_and_convert(model.transformer_branches_query(), index="mrid")
    angle = model.winding_angle()
    pytest.approx(branches.loc[angle.index, "angle"] == 0.0)


def test_regions(model: SingleClientModel):
    regions = model.regions()
    assert pd.to_numeric(regions.groupby("region").count().loc["NO", "name"]) > 16


def test_hvdc_converters_bidzones(model: SingleClientModel):
    hvdc_converter_bidzone = model.hvdc_converter_bidzones()

    corridors = set[tuple[str, str]](
        zip(hvdc_converter_bidzone["bidzone_1"], hvdc_converter_bidzone["bidzone_2"], strict=True)
    )

    # Check data quality in the models
    expect_corridors = {("SE4", "SE3"), ("NO2", "DE"), ("NO2", "DK1"), ("NO2", "GB"), ("NO2", "NL")}
    assert expect_corridors.issubset(corridors)


def test_windings(model: SingleClientModel):
    windings = model.transformers(region="NO")
    assert windings.shape[1] == 9


@pytest.mark.skipif(os.getenv("GRAPHDB_SERVER") is None, reason="Need graphdb server to run")
def test_borders_no(model: SingleClientModel):
    borders = model.borders(region="NO")
    assert (borders[["area_1", "area_2"]] == "NO").any(axis=1).all()
    assert (borders["area_1"] != borders["area_2"]).all()


def sparql_result_int(num: int) -> SparqlResultValue:
    return SparqlResultValue(
        type="literal",
        datatype="http://www.w3.org/2001/XMLSchema#integer",
        value=str(num),
    )


def test_data_row():
    cols = ["a", "b", "c", "d", "e"]
    rows = [
        {"a": sparql_result_int(1), "b": sparql_result_int(2)},
        {"c": sparql_result_int(3), "d": sparql_result_int(4)},
        {"a": sparql_result_int(5), "b": sparql_result_int(6)},
        {"e": sparql_result_int(7)},
    ]
    assert not set(data_row(cols, rows)).symmetric_difference(cols)


def test_data_row_missing_column():
    cols = ["a", "b", "c", "d", "e"]
    rows = [
        {"a": sparql_result_int(1), "b": sparql_result_int(2)},
        {"c": sparql_result_int(3)},
        {"a": sparql_result_int(5), "b": sparql_result_int(6)},
        {"e": sparql_result_int(7)},
    ]
    assert set(data_row(cols, rows).keys()).symmetric_difference(cols) == {"d"}


def test_dtypes(model: SingleClientModel):
    mapper = TypeMapper(model.client.service_cfg)
    data = model.client.get_table(mapper.query)[0]
    assert data["sparql_type"].isna().sum() == 0


def test_prefix_resp_not_ok():
    resp = httpx.Response(status_code=HTTPStatus.UNAUTHORIZED, text="Something went wrong")
    with pytest.raises(RuntimeError) as exc:
        GraphDBClient(ServiceConfig(server="some-serever")).get_prefixes(
            http_transport=httpx.MockTransport(lambda _: resp)
        )
    assert resp.reason_phrase in str(exc)
    assert f"{resp.status_code}" in str(exc)


def test_conf_bytes_from_template():
    template = confpath() / "native_store_config_template.ttl"

    conf_bytes = config_bytes_from_template(template, {"repo": "test_repo"})
    conf_str = conf_bytes.decode()
    assert 'rep:repositoryID "test_repo"' in conf_str


def test_create_delete_repo():
    url = t_common.rdf4j_url()

    # Check if it is possible to make contact
    try:
        resp = httpx.get("http://" + url, timeout=5.0)
        if resp.status_code not in [HTTPStatus.OK, HTTPStatus.FOUND] and not os.getenv("CI"):
            pytest.skip("Could not contact RDF4J server")
    except Exception as exc:
        if os.getenv("CI"):
            pytest.fail(f"{exc}")
        else:
            pytest.skip(f"{exc}")
    repo = "test_repo"
    template = confpath() / "native_store_config_template.ttl"
    conf_bytes = config_bytes_from_template(template, {"repo": repo})
    service_cfg = ServiceConfig(repo, "http", url)
    current_repos = repos(service_cfg)

    assert "test_repo" not in [i.repo_id for i in current_repos]

    # protocol is added internally. Thus, skip from t_common.rdf4j_url
    client = new_repo(service_cfg, conf_bytes)
    current_repos = repos(service_cfg)
    assert "test_repo" in [i.repo_id for i in current_repos]

    client.delete_repo()
    current_repos = repos(service_cfg)
    assert "test_repo" not in [i.repo_id for i in current_repos]


def test_repos_with_auth(httpserver: HTTPServer):
    response_json = {
        "results": {
            "bindings": [
                {
                    "uri": {"value": "uri"},
                    "id": {"value": "id"},
                    "title": {"value": "title"},
                    "readable": {"value": "true"},
                    "writable": {"value": "false"},
                }
            ]
        }
    }

    matcher = HeaderValueMatcher({"authorization": lambda value, expect: expect == value})
    user, password = "user", "password"
    encoded_user_passwd = b64encode(bytes(f"{user}:{password}", "utf8")).decode("utf8")
    httpserver.expect_request(
        "/repositories",
        headers={"authorization": f"Basic {encoded_user_passwd}"},
        header_value_matcher=matcher,
    ).respond_with_json(response_json)
    url = httpserver.url_for("/repositories")

    matches = re.match(r"^([a-z]+):\/\/([a-z:0-9]+)", url)
    assert matches
    protocol, server = matches.groups()

    cfg = ServiceConfig("repo", server=server, protocol=protocol, user=user, passwd=password)
    repo_info = repos(cfg)

    expect = RepoInfo("uri", "id", "title", readable=True, writable=False)
    assert repo_info == [expect]


def test_update_prefixes():
    client = GraphDBClient(ServiceConfig(server="some-server", rest_api=RestApi.DIRECT_SPARQL_ENDPOINT))

    new_pref = {"eq": "http://eq"}
    client.update_prefixes(new_pref)
    assert client.prefixes["eq"] == new_pref["eq"]


def test_custom_headers():
    custom_headers = {"my_header": "my_header_value"}
    client = GraphDBClient(ServiceConfig(server="some-server", token=None), custom_headers)
    assert client.sparql.customHttpHeaders == custom_headers


class FixedResultSparqlWrapper(SPARQLWrapper):
    def __init__(self) -> None:
        super().__init__("http://fixed-result-endpoint")
        self.result = SparqlResultJsonFactory.build()

    def queryAndConvert(self) -> dict[str, Any]:  # noqa: N802
        return self.result.model_dump(mode="json")


def test_inject_subclassed_sparql_wrapper():
    wrapper = FixedResultSparqlWrapper()
    client = GraphDBClient(sparql_wrapper=wrapper)

    # Confirm that we can successfully run a query
    data = client.get_table("select * where {?s ?p ?o}")[0]
    assert set(data.columns) == set(wrapper.result.head.variables)


def test_xnodes(model: Model):
    dfs = all_data(model)
    con_nodes = dfs.connectivity_nodes
    ac_lines = dfs.ac_lines.assign(
        bidzone_1=lambda df: df["connectivity_node_1"].map(con_nodes["bidzone"]),
        bidzone_2=lambda df: df["connectivity_node_2"].map(con_nodes["bidzone"]),
    )
    xnode_branches = ac_lines[ac_lines["name"].str.contains("Xnode")]
    non_xnode_branches = ac_lines[~ac_lines["name"].str.contains("Xnode")]
    assert not xnode_branches.empty
    assert not xnode_branches["bidzone_1"].isna().any()
    assert not xnode_branches["bidzone_2"].isna().any()

    non_xnode_bidzones = set(non_xnode_branches["bidzone_1"]) | set(non_xnode_branches["bidzone_2"])
    xnode_bidzones = set(xnode_branches["bidzone_1"]) | set(xnode_branches["bidzone_2"])

    # Verify that there are no distinct bidzones among the xnodes such as EU, EU-ELSP-1 etc.
    assert xnode_bidzones.issubset(non_xnode_bidzones)
