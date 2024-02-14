import functools
import logging
import os
import re
from pathlib import Path

import t_utils.common as t_common
from cimsparql.adaptions import XmlModelAdaptor
from cimsparql.graphdb import GraphDBClient, RestApi, ServiceConfig, new_repo_blazegraph
from cimsparql.model import (
    ModelConfig,
    SingleClientModel,
    get_federated_cim_model,
    get_single_client_model,
)

this_dir = Path(__file__).parent

logger = logging.getLogger()


def micro_t1_nl_graphdb() -> t_common.ModelTest:
    repo = os.getenv("GRAPHDB_MICRO_NL_REPO", "abot-micro-nl")
    model = None
    try:
        s_cfg = ServiceConfig(repo=repo, max_delay_seconds=1)
        m_cfg = ModelConfig(system_state_repo=f"repository:{repo}", eq_repo=f"repository:{repo}")
        if os.getenv("GRAPHDB_SERVER"):
            model = get_single_client_model(s_cfg, m_cfg)
    except Exception:
        logger.exception("Failed to get single model")
    return t_common.ModelTest(model, False, False)


def split_tpsvssh(fname: Path) -> tuple[list[str], list[str]]:
    tpsvssh_lines = []
    remaining_lines = []

    tpsvssh_contexts = {
        "http://entsoe.eu/CIM/StateVariables/4/1",
        "http://entsoe.eu/CIM/SteadyStateHypothesis/1/1",
        "http://entsoe.eu/CIM/Topology/4/1",
    }

    # Regex extracts the content between < > of the last occurence on each line
    prog = re.compile(r"<([^>]+)>[^>]+$")
    with open(fname) as infile:
        for line in infile:
            m = prog.search(line)
            if m and m.group(1) in tpsvssh_contexts:
                tpsvssh_lines.append(line)
            else:
                remaining_lines.append(line)
    return "".join(tpsvssh_lines), "".join(remaining_lines)


@functools.lru_cache
def federated_micro_t1_nl_bg() -> t_common.ModelTest:
    model = None
    url = t_common.blazegraph_url()
    try:
        tpsvssh_client = new_repo_blazegraph(url, "federated_micro_t1_nl_tpsvssh", "http")
        eq_client = new_repo_blazegraph(url, "federated_micro_t1_nl_eq", "http")

        adaptor = XmlModelAdaptor.from_folder(this_dir.parent / "data/micro")
        adaptor.adapt(eq_client.service_cfg.url)
        tpsvssh_ctx = adaptor.tpsvssh_contexts()
        tpsvssh = adaptor.nq_bytes(tpsvssh_ctx)
        remaining = adaptor.nq_bytes(
            [ctx for ctx in adaptor.graph.contexts() if ctx not in tpsvssh_ctx]
        )
        tpsvssh_client.upload_rdf(tpsvssh, "n-quads")
        eq_client.upload_rdf(remaining, "n-quads")

        m_cfg = ModelConfig(
            system_state_repo=tpsvssh_client.service_cfg.url + ",infer=false",
            eq_repo=eq_client.service_cfg.url + ",infer=false",
        )
        model = get_federated_cim_model(eq_client, tpsvssh_client, m_cfg)
    except Exception:
        logger.exception("Failed to get federated cim model")
    return t_common.ModelTest(model)


def upload_micro_model(client: GraphDBClient) -> None:
    adaptor = XmlModelAdaptor.from_folder(this_dir.parent / "data/micro")
    adaptor.adapt(client.service_cfg.url)
    graph = "<http://mygraph.com/demo/1/1>"
    client.upload_rdf(adaptor.nq_bytes(), "n-quads", {"context": graph})


@functools.lru_cache
def micro_t1_nl() -> t_common.ModelTest:
    """
    Micro model in RDF4J. It is cached, so multiple calls with the same url
    returns the same model
    """
    model = None
    try:
        url = t_common.rdf4j_url()
        client = t_common.init_repo_rdf4j(url, "micro_t1_nl")
        upload_micro_model(client)
        model = SingleClientModel(client)
    except Exception:
        logger.exception("Failed to get single client model")
    return t_common.ModelTest(model)


@functools.lru_cache
def micro_t1_nl_bg() -> t_common.ModelTest:
    model = None
    try:
        url = t_common.blazegraph_url()
        client = new_repo_blazegraph(url, "micro_t1_nl", "http")
        upload_micro_model(client)
        model = SingleClientModel(client)
    except Exception:
        logger.exception("Failed to get single client model")
    return t_common.ModelTest(model)


@functools.lru_cache
def small_grid_model(url: str, api: RestApi) -> t_common.ModelTest:
    def bg_http(url: str, name: str) -> GraphDBClient:
        return new_repo_blazegraph(url, name, "http")

    model = None
    test_folder = Path(__file__).parent.parent / "data/small"
    try:
        init_func = t_common.init_repo_rdf4j if api == RestApi.RDF4J else bg_http
        eq_client = init_func(url, "smallgrid_eq")
        adaptor = XmlModelAdaptor.from_folder(test_folder)
        adaptor.adapt(eq_client.service_cfg.url)
        tpsvssh_client = init_func(url, "smallgrid_tpsvssh")
        tpsvssh_ctx = adaptor.tpsvssh_contexts()
        tpsvssh_client.upload_rdf(adaptor.nq_bytes(tpsvssh_ctx), "n-quads")

        remaining = adaptor.nq_bytes(
            [ctx for ctx in adaptor.graph.contexts() if ctx not in tpsvssh_ctx]
        )

        eq_client.upload_rdf(remaining, "n-quads")

        m_cfg = ModelConfig(
            system_state_repo=tpsvssh_client.service_cfg.url,
            eq_repo=eq_client.service_cfg.url,
        )
        model = get_federated_cim_model(eq_client, tpsvssh_client, m_cfg)
    except Exception:
        logger.exception("Failed to get federated model")
    return t_common.ModelTest(model)


def micro_models() -> list[t_common.ModelTest]:
    return [micro_t1_nl(), micro_t1_nl_bg(), federated_micro_t1_nl_bg()]


def smallgrid_models() -> list[t_common.ModelTest]:
    bg_model = small_grid_model(t_common.blazegraph_url(), RestApi.BLAZEGRAPH)
    rdfj4_model = small_grid_model(t_common.rdf4j_url(), RestApi.RDF4J)
    return [rdfj4_model, bg_model]
