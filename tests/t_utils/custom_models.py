import functools
import logging
import os

import tests.t_utils.common as t_common
from cimsparql.graphdb import GraphDBClient, ServiceConfig
from cimsparql.model import ModelConfig, get_federated_cim_model

logger = logging.getLogger()


def combined_graphdb_service() -> ServiceConfig:
    repo = os.getenv("GRAPHDB_COMBINED_REPO", "abot_combined")
    return ServiceConfig(repo=repo, max_delay_seconds=1, token=os.getenv("GRAPHDB_TOKEN"))


@functools.lru_cache
def federated_model() -> t_common.ModelTest:
    if os.getenv("CI") or os.getenv("GRAPHDB_TOKEN") is None:
        return t_common.ModelTest(None, must_run_in_ci=False, cleanup=False)
    eq_repo = os.getenv("GRAPHDB_EQ", "abot-eq-1")
    system_state_repo = os.getenv("GRAPHDB_STATE", "abot-situations-1")
    server = os.getenv("GRAPHDB_SERVER", "modelstore.dev.form.statnett.no:8443")
    eq_client_cfg = ServiceConfig(eq_repo, server=server, max_delay_seconds=1)
    tpsvssh_client_cfg = ServiceConfig(system_state_repo, server=server, max_delay_seconds=1)
    eq_client = GraphDBClient(eq_client_cfg)
    tpsvssh_client = GraphDBClient(tpsvssh_client_cfg)

    m_cfg = ModelConfig(f"repository:{system_state_repo},infer=false", f"repository:{eq_repo},infer=false")
    model = None
    try:
        model = get_federated_cim_model(eq_client, tpsvssh_client, m_cfg)
    except Exception:
        logger.exception("Failed to get federated model")
    return t_common.ModelTest(model, must_run_in_ci=False, cleanup=False)


def all_custom_models() -> list[t_common.ModelTest]:
    return [federated_model()]
