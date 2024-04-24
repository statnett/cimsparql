import asyncio
import logging
import os
import re
from base64 import b64encode
from collections.abc import Callable
from http import HTTPStatus
from typing import Any

import httpx
import pandas as pd
import pytest
import t_utils.common as t_common
import t_utils.custom_models as t_custom
from pytest_httpserver import HeaderValueMatcher, HTTPServer
from SPARQLWrapper import SPARQLWrapper

from cimsparql.graphdb import (
    GraphDBClient,
    RepoInfo,
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
    logger.debug(f"Starting {func.__name__}")
    try:
        return func(*args)
    except Exception:
        logger.exception("Exception for %s", func.__name__)
        return pd.DataFrame()


async def collect_data(model: Model) -> list[pd.DataFrame]:
    loop = asyncio.get_event_loop()
    result = await asyncio.gather(
        loop.run_in_executor(None, exception_logging, model.ac_lines),
        loop.run_in_executor(None, exception_logging, model.borders),
        loop.run_in_executor(None, exception_logging, model.branch_node_withdraw),
        loop.run_in_executor(None, exception_logging, model.bus_data),
        loop.run_in_executor(None, exception_logging, model.connections),
        loop.run_in_executor(None, exception_logging, model.connectivity_nodes),
        loop.run_in_executor(None, exception_logging, model.converters),
        loop.run_in_executor(None, exception_logging, model.coordinates),
        loop.run_in_executor(None, exception_logging, model.disconnected),
        loop.run_in_executor(None, exception_logging, model.dc_active_flow),
        loop.run_in_executor(None, exception_logging, model.exchange, "NO|SE"),
        loop.run_in_executor(None, exception_logging, model.full_model),
        loop.run_in_executor(None, exception_logging, model.hvdc_converter_bidzones),
        loop.run_in_executor(None, exception_logging, model.loads),
        loop.run_in_executor(None, exception_logging, model.market_dates),
        loop.run_in_executor(None, exception_logging, model.powerflow),
        loop.run_in_executor(None, exception_logging, model.regions),
        loop.run_in_executor(None, exception_logging, model.series_compensators),
        loop.run_in_executor(None, exception_logging, model.station_group_codes_and_names),
        loop.run_in_executor(None, exception_logging, model.substation_voltage_level),
        loop.run_in_executor(None, exception_logging, model.synchronous_machines),
        loop.run_in_executor(None, exception_logging, model.switches),
        loop.run_in_executor(None, exception_logging, model.three_winding_transformers),
        loop.run_in_executor(None, exception_logging, model.transformer_windings),
        loop.run_in_executor(None, exception_logging, model.transformers_connected_to_converter),
        loop.run_in_executor(None, exception_logging, model.transformers),
        loop.run_in_executor(None, exception_logging, model.two_winding_transformers),
        loop.run_in_executor(None, exception_logging, model.wind_generating_units),
    )
    return result


@pytest.mark.asyncio
@pytest.mark.parametrize("test_model", t_custom.all_custom_models())
async def test_not_empty(test_model: t_common.ModelTest):
    model = test_model.model
    if not model:
        pytest.skip("Require access to GraphDB")
    dfs = await collect_data(model)

    for i, df in enumerate(dfs):
        assert not df.empty, f"Failed for dataframe {i}"


@pytest.fixture
def model() -> SingleClientModel:
    test_model = t_custom.combined_model()
    if not test_model.model:
        pytest.skip("Require access to GraphDB")
    return test_model.model


def test_cimversion(model: SingleClientModel):
    assert model.cim_version == 16


def test_regions(model: SingleClientModel):
    regions = model.regions()
    assert regions.groupby("region").count().loc["NO", "name"] > 16


def test_hvdc_converters_bidzones(model: SingleClientModel):
    df = model.hvdc_converter_bidzones()

    corridors = set(zip(df["bidzone_1"], df["bidzone_2"], strict=True))

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
    df = model.client.get_table(mapper.query)[0]
    assert df["sparql_type"].isna().sum() == 0


def test_prefix_resp_not_ok(monkeypatch: pytest.MonkeyPatch):
    resp = httpx.Response(status_code=HTTPStatus.UNAUTHORIZED, text="Something went wrong")
    monkeypatch.setattr(httpx, "get", lambda *_, **__: resp)
    with pytest.raises(RuntimeError) as exc:
        GraphDBClient(ServiceConfig(server="some-serever")).get_prefixes()
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
        resp = httpx.get("http://" + url)
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
    protocol, server = matches.groups()

    cfg = ServiceConfig("repo", server=server, protocol=protocol, user=user, passwd=password)
    repo_info = repos(cfg)

    expect = RepoInfo("uri", "id", "title", True, False)
    assert repo_info == [expect]


def test_update_prefixes(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(GraphDBClient, "get_prefixes", lambda *_: {})
    client = GraphDBClient(ServiceConfig(server="some-server"))
    assert client.prefixes == {}

    new_pref = {"eq": "http://eq"}
    client.update_prefixes(new_pref)
    assert client.prefixes == new_pref


def test_custom_headers():
    custom_headers = {"my_header": "my_header_value"}
    client = GraphDBClient(ServiceConfig(server="some-server", token=None), custom_headers)
    assert client.sparql.customHttpHeaders == custom_headers


class FixedResultSparqlWrapper(SPARQLWrapper):
    def __init__(self) -> None:
        super().__init__("http://fixed-result-endpoint")
        self.result = SparqlResultJsonFactory.build()

    def queryAndConvert(self) -> dict:  # noqa: N802
        return self.result.model_dump(mode="json")


def test_inject_subclassed_sparql_wrapper():
    wrapper = FixedResultSparqlWrapper()
    client = GraphDBClient(sparql_wrapper=wrapper)

    # Confirm that we can sucessfully run a query
    df = client.get_table("select * where {?s ?p ?o}")[0]
    assert set(df.columns) == set(wrapper.result.head.variables)
