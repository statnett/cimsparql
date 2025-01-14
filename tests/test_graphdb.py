import asyncio
import logging
import os
import re
from base64 import b64encode
from collections import defaultdict
from collections.abc import Callable
from functools import lru_cache
from http import HTTPStatus
from typing import Any

import httpx
import pandas as pd
import pytest
from pytest_httpserver import HeaderValueMatcher, HTTPServer
from SPARQLWrapper import SPARQLWrapper

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
from cimsparql.sparql_result_json import SparqlResultJsonFactory
from cimsparql.type_mapper import TypeMapper

logger = logging.getLogger()


def exception_logging(func: Callable[[Any], pd.DataFrame], *args: Any):
    logger.debug("Starting %s", func.__name__)
    try:
        return func(*args)
    except Exception:
        logger.exception("Exception for %s", func.__name__)
        return pd.DataFrame()


@lru_cache
def all_data(model: Model) -> dict[str, pd.DataFrame]:
    return asyncio.run(collect_data(model))


async def collect_data(model: Model) -> dict[str, pd.DataFrame]:
    loop = asyncio.get_event_loop()
    queries = (
        model.ac_lines,
        model.associated_switches,
        model.base_voltage,
        model.borders,
        model.bus_data,
        model.connections,
        model.connectivity_nodes,
        model.converters,
        model.coordinates,
        model.dc_active_flow,
        model.disconnected,
        model.exchange,
        model.full_model,
        model.gen_unit_and_sync_machine_mrid,
        model.hvdc_converter_bidzones,
        model.loads,
        model.market_dates,
        model.phase_tap_changer,
        model.powerflow,
        model.regions,
        model.series_compensators,
        model.station_group_codes_and_names,
        model.substation_voltage_level,
        model.station_group_for_power_unit,
        model.sv_power_deviation,
        model.switches,
        model.synchronous_machines,
        model.transformer_branches,
        model.transformer_windings,
        model.transformers,
        model.transformers_connected_to_converter,
        model.transformer_center_nodes,
        model.wind_generating_units,
    )

    args = defaultdict(tuple, {"exchange": ("NO|SE",)})
    result = await asyncio.gather(
        *[loop.run_in_executor(None, exception_logging, query, *args[query.__name__]) for query in queries]
    )
    return {query.__name__: res for query, res in zip(queries, result, strict=False)}


@pytest.mark.parametrize("test_model", t_custom.all_custom_models())
def test_not_empty(test_model: t_common.ModelTest):
    model = test_model.model
    if not model:
        pytest.skip("Require access to GraphDB")
    dfs = all_data(model)

    for name, df in dfs.items():
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
    assert regions.groupby("region").count().loc["NO", "name"] > 16


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


def test_data_row():
    cols = ["a", "b", "c", "d", "e"]
    rows = [{"a": 1, "b": 2}, {"c": 3, "d": 4}, {"a": 5, "b": 6}, {"e": 7}]
    assert not set(data_row(cols, rows)).symmetric_difference(cols)


def test_data_row_missing_column():
    cols = ["a", "b", "c", "d", "e"]
    rows = [{"a": 1, "b": 2}, {"c": 3}, {"a": 5, "b": 6}, {"e": 7}]
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
    client = new_repo(url, repo, conf_bytes, protocol="http")
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

    # Confirm that we can sucessfully run a query
    data = client.get_table("select * where {?s ?p ?o}")[0]
    assert set(data.columns) == set(wrapper.result.head.variables)


def test_xnodes(model: Model):
    dfs = all_data(model)
    bus = dfs["bus_data"]
    ac_lines = dfs["ac_lines"].assign(
        bidzone_1=lambda df: df["node_1"].map(bus["bidzone"]),
        bidzone_2=lambda df: df["node_2"].map(bus["bidzone"]),
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


def test_bidzone_consistency(model: Model):
    dfs = all_data(model)

    # Verify that all bidzones are consistent regardless of whether they are collected via
    # topological nodes or connectivity nodes
    bus = dfs["bus_data"]
    con_nodes = dfs["connectivity_nodes"]
    ac_lines = dfs["ac_lines"]

    pd.testing.assert_series_equal(
        ac_lines["node_1"].map(bus["bidzone"]),
        ac_lines["connectivity_node_1"].map(con_nodes["bidzone"]),
        check_names=False,
    )
    pd.testing.assert_series_equal(
        ac_lines["node_2"].map(bus["bidzone"]),
        ac_lines["connectivity_node_2"].map(con_nodes["bidzone"]),
        check_names=False,
    )
