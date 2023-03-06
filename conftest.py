import json
import logging
import os
import pathlib
import re
from copy import deepcopy
from typing import Dict, Generator, Optional

import pandas as pd
import pytest

from cimsparql.constants import CIM_TYPES_WITH_MRID
from cimsparql.graphdb import (
    GraphDBClient,
    RestApi,
    ServiceConfig,
    config_bytes_from_template,
    confpath,
    new_repo,
    new_repo_blazegraph,
)
from cimsparql.model import (
    CimModel,
    ModelConfig,
    MultiClientCimModel,
    get_cim_model,
    get_federated_cim_model,
)

this_dir = pathlib.Path(__file__).parent

logger = logging.getLogger(__name__)
ssh_graph = "<http://entsoe.eu/CIM/SteadyStateHypothesis/1/1>"


@pytest.fixture(scope="session")
def graphdb_service() -> ServiceConfig:
    return ServiceConfig(repo="abot_combined")


@pytest.fixture(scope="session")
def model(graphdb_service: ServiceConfig) -> CimModel:
    if os.getenv("GRAPHDB_SERVER") is None:
        pytest.skip("Require access to GraphDB")
    system_state_repo = f"repository:{graphdb_service.repo},infer=false"
    eq_repo = f"repository:{graphdb_service.repo},infer=false"
    return get_cim_model(graphdb_service, ModelConfig(system_state_repo, ssh_graph, eq_repo))


@pytest.fixture(scope="session")
def micro_t1_nl_graphdb() -> Optional[CimModel]:
    repo = os.getenv("GRAPHDB_MICRO_NL_REPO", "abot-micro-nl")
    model = None
    try:
        s_cfg = ServiceConfig(repo=repo)
        m_cfg = ModelConfig(system_state_repo=f"repository:{repo}", eq_repo=f"repository:{repo}")
        if os.getenv("GRAPHDB_SERVER"):
            model = get_cim_model(s_cfg, m_cfg)
    except Exception as exc:
        logger.error(f"{exc}")
        model = None
    return model


@pytest.fixture(scope="session")
def model_sep() -> Optional[MultiClientCimModel]:
    eq_repo = os.getenv("GRAPHDB_EQ", "abot_222-2-1_2")
    system_state_repo = os.getenv("GRAPHDB_STATE", "abot_20220825T1621Z")
    eq_client_cfg = ServiceConfig(eq_repo)
    tpsvssh_client_cfg = ServiceConfig(system_state_repo)
    eq_client = GraphDBClient(eq_client_cfg)
    tpsvssh_client = GraphDBClient(tpsvssh_client_cfg)

    m_cfg = ModelConfig(
        f"repository:{system_state_repo},infer=false",
        ssh_graph,
        f"repository:{eq_repo},infer=false",
    )
    return get_federated_cim_model(eq_client, tpsvssh_client, m_cfg)


@pytest.fixture(scope="session")
def graphdb_real_data_models(
    model: CimModel, model_sep: MultiClientCimModel
) -> Dict[str, MultiClientCimModel]:
    return {"model": model, "model_sep": model_sep}


@pytest.fixture(scope="session")
def connections(model: CimModel) -> pd.DataFrame:
    return model.connections()


@pytest.fixture(scope="session")
def rdf4j_url() -> str:
    if os.getenv("CI"):
        # Running on GitHub
        return "localhost:8080/rdf4j-server"

    return os.getenv("RDF4J_URL", "")


@pytest.fixture(scope="session")
def blazegraph_url() -> str:
    if os.getenv("CI"):
        # Running on GitHub
        return "localhost:9999/blazegraph/namespace"
    return os.getenv("BLAZEGRAPH_URL", "")


def initialized_rdf4j_repo(service_url: str) -> GraphDBClient:
    data_path = this_dir / "tests/data"

    template = confpath() / "native_store_config_template.ttl"
    config = config_bytes_from_template(template, {"repo": "picasso"})

    client = new_repo(service_url, "picasso", config, allow_exist=True, protocol="http")

    data_file = data_path / "artist.ttl"
    client.upload_rdf(data_file, "turtle")
    return client


@pytest.fixture
def rdf4j_gdb(rdf4j_url: str) -> Generator[Optional[GraphDBClient], None, None]:
    client = None
    try:
        client = initialized_rdf4j_repo(rdf4j_url)
        yield client
    except Exception as exc:
        logger.error(f"{exc}")
        yield client
    finally:
        # Tear down delete content
        if client:
            client.delete_repo()


def init_test_cim_model(rdf4j_url: str, name: str, repo_name_suffix: str = ""):
    nq_file = pathlib.Path(__file__).parent / f"tests/data/{name}.nq"
    template = confpath() / "native_store_config_template.ttl"
    repo_name = name + repo_name_suffix
    config = config_bytes_from_template(template, {"repo": repo_name})
    client = new_repo(rdf4j_url, repo_name, config, allow_exist=False, protocol="http")
    graph = "<http://mygraph.com/demo/1/1>"
    client.upload_rdf(nq_file, "n-quads", {"context": graph})
    ns_file = confpath() / "namespaces.json"
    with open(ns_file, "r") as infile:
        ns = json.load(infile)

    for k, v in ns.items():
        client.set_namespace(k, v)
    return client


def init_test_cim_blazegraph(url: str, name: str, suffix: str = ""):
    repo = f"{name}{suffix}"
    client = new_repo_blazegraph(url, repo, "http")
    nq_file = pathlib.Path(__file__).parent / f"tests/data/{name}.nq"
    graph = "http://mygraph.com/demo/1/1"
    client.upload_rdf(nq_file, "n-quads", {"context-uri": graph})
    return client


def split_tpsvssh(fname: pathlib.Path) -> tuple[list[str], list[str]]:
    tpsvssh_lines = []
    remaining_lines = []

    tpsvssh_contexts = {
        "http://entsoe.eu/CIM/StateVariables/4/1",
        "http://entsoe.eu/CIM/SteadyStateHypothesis/1/1",
        "http://entsoe.eu/CIM/Topology/4/1",
    }

    # Regex extracts the content between < > of the last occurence on each line
    prog = re.compile(r"<([^>]+)>[^>]+$")
    with open(fname, "r") as infile:
        for line in infile:
            m = prog.search(line)
            if m and m.group(1) in tpsvssh_contexts:
                tpsvssh_lines.append(line)
            else:
                remaining_lines.append(line)
    return "".join(tpsvssh_lines), "".join(remaining_lines)


def init_federated_micro_cim_blazegraph(url: str):
    tpsvssh_client = new_repo_blazegraph(url, "federated_micro_t1_nl_tpsvssh", "http")
    eq_client = new_repo_blazegraph(url, "federated_micro_t1_nl_eq", "http")

    # Split NQ file content in SV/TP/SSH profile
    fname = pathlib.Path(__file__).parent / "tests/data/micro_t1_nl.nq"
    tpsvssh, remaining = split_tpsvssh(fname)
    tpsvssh_client.upload_rdf(tpsvssh.encode("utf8"), "n-quads")
    eq_client.upload_rdf(remaining.encode("utf8"), "n-quads")

    m_cfg = ModelConfig(
        system_state_repo=tpsvssh_client.service_cfg.url + ",infer=false",
        eq_repo=eq_client.service_cfg.url + ",infer=false",
    )
    return get_federated_cim_model(eq_client, tpsvssh_client, m_cfg)


def cim_client(url: str, repo_name: str, repo_name_suffix: str, rest_api: RestApi) -> GraphDBClient:
    if rest_api == RestApi.BLAZEGRAPH:
        return init_test_cim_blazegraph(url, repo_name, repo_name_suffix)
    return init_test_cim_model(url, repo_name, repo_name_suffix)


def init_cim_model(
    url: str,
    repo_name: str,
    repo_name_suffix: str = "",
    rest_api: RestApi = RestApi.RDF4J,
    config: Optional[ModelConfig] = None,
) -> Optional[CimModel]:
    try:
        client = cim_client(url, repo_name, repo_name_suffix, rest_api)
        return CimModel(client, config)
    except Exception as exc:
        logger.error(f"{exc}")
        return None


@pytest.fixture(scope="session")
def micro_t1_nl(rdf4j_url: str) -> Generator[Optional[CimModel], None, None]:
    model = init_cim_model(rdf4j_url, "micro_t1_nl")
    try:
        yield model
    finally:
        if model:
            model.client.delete_repo()


@pytest.fixture(scope="session")
def micro_t1_nl_bg(blazegraph_url: str) -> Generator[Optional[CimModel], None, None]:
    model = init_cim_model(blazegraph_url, "micro_t1_nl", rest_api=RestApi.BLAZEGRAPH)
    try:
        yield model
    finally:
        if model:
            model.client.delete_repo()


def apply_custom_modifications(model: Optional[CimModel]) -> None:
    if model:
        for rdf_type in CIM_TYPES_WITH_MRID:
            model.add_mrid(f"cim:{rdf_type}")


@pytest.fixture(scope="session")
def micro_t1_nl_bg_federated(
    blazegraph_url: str,
) -> Generator[Optional[MultiClientCimModel], None, None]:
    model = init_federated_micro_cim_blazegraph(blazegraph_url)
    try:
        yield model
    finally:
        if model:
            for client in model.distinct_clients:
                try:
                    client.delete_repo()
                except Exception:
                    pass


@pytest.fixture(scope="session")
def micro_t1_nl_models(
    micro_t1_nl, micro_t1_nl_bg, micro_t1_nl_graphdb, micro_t1_nl_bg_federated
) -> Dict[str, Optional[CimModel]]:
    return {
        "rdf4j": micro_t1_nl,
        "blazegraph": micro_t1_nl_bg,
        "blazegraph_fed": micro_t1_nl_bg_federated,
        "graphdb": micro_t1_nl_graphdb,
    }


def small_grid_model(url: str, api: RestApi):
    tpsvssh_mod = init_cim_model(url, "smallgrid_tpsvssh", "", api)
    config = ModelConfig(tpsvssh_mod.client.service_cfg.url) if tpsvssh_mod else ModelConfig()
    eq_mod = init_cim_model(url, "smallgrid_eq", "", api, config)
    try:
        apply_custom_modifications(tpsvssh_mod)
        apply_custom_modifications(eq_mod)
        yield eq_mod
    except Exception as exc:
        logger.error(f"{exc}")
        yield None
    finally:
        if eq_mod:
            eq_mod.client.delete_repo()
        if tpsvssh_mod:
            tpsvssh_mod.client.delete_repo()


@pytest.fixture(scope="session")
def small_grid_model_rdf4j(rdf4j_url) -> Generator[Optional[CimModel], None, None]:
    yield from small_grid_model(rdf4j_url, RestApi.RDF4J)


@pytest.fixture(scope="session")
def small_grid_model_bg(blazegraph_url) -> Generator[Optional[CimModel], None, None]:
    yield from small_grid_model(blazegraph_url, RestApi.BLAZEGRAPH)


@pytest.fixture(scope="session")
def smallgrid_models(small_grid_model_rdf4j, small_grid_model_bg):
    small_grid = os.getenv("SMALL_GRID", "abot-smallgrid")
    if graphdb_server := os.getenv("GRAPHDB_SERVER"):
        s_cfg = ServiceConfig(f"{small_grid}_eq", server=graphdb_server)
        m_cfg = ModelConfig(
            system_state_repo=f"repository:{small_grid}_tpsvssh", ssh_graph=ssh_graph
        )
        try:
            model = get_cim_model(s_cfg, m_cfg)
        except Exception as exc:
            logger.error(f"{exc}")
            model = None

        # Add MultiClient varianet for graphdb
        tpsv_cfg = ServiceConfig(repo=f"{small_grid}_tpsvssh")
        eq_client = GraphDBClient(s_cfg)
        tpsvssh_client = GraphDBClient(tpsv_cfg)
        m_cfg = deepcopy(m_cfg)
        m_cfg.eq_repo = f"repository:{small_grid}_eq"
        try:
            sep_model = get_federated_cim_model(eq_client, tpsvssh_client, m_cfg)
        except Exception as exc:
            logger.error(f"{exc}")
            sep_model = None
    else:
        model = None
        sep_model = None
    return {
        "rdf4j": small_grid_model_rdf4j,
        "blazegraph": small_grid_model_bg,
        "graphdb": model,
        "graphdb_sep": sep_model,
    }
