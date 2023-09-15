import functools
import logging
import os
from typing import List

import t_utils.common as t_common

from cimsparql.graphdb import GraphDBClient, ServiceConfig
from cimsparql.model import ModelConfig, get_federated_cim_model, get_single_client_model

logger = logging.getLogger()


def combined_graphdb_service() -> ServiceConfig:
    repo = os.getenv("GRAPHDB_COMBINED-repo", "abot_combined")
    return ServiceConfig(repo=repo)


@functools.lru_cache
def combined_model() -> t_common.ModelTest:
    if os.getenv("CI"):
        return t_common.ModelTest(None, False, False)
    service = combined_graphdb_service()
    system_state_repo = f"repository:{service.repo},infer=false"
    eq_repo = f"repository:{service.repo},infer=false"
    model = None
    try:
        model = get_single_client_model(service, ModelConfig(system_state_repo, eq_repo))
    except Exception as exc:
        logger.error(f"{exc}")
    return t_common.ModelTest(model, False, False)


@functools.lru_cache
def federated_model() -> t_common.ModelTest:
    if os.getenv("CI"):
        return t_common.ModelTest()
    eq_repo = os.getenv("GRAPHDB_EQ", "abot_222-2-1_2")
    system_state_repo = os.getenv("GRAPHDB_STATE", "abot_20220825T1621Z")
    eq_client_cfg = ServiceConfig(eq_repo)
    tpsvssh_client_cfg = ServiceConfig(system_state_repo)
    eq_client = GraphDBClient(eq_client_cfg)
    tpsvssh_client = GraphDBClient(tpsvssh_client_cfg)

    m_cfg = ModelConfig(
        f"repository:{system_state_repo},infer=false", f"repository:{eq_repo},infer=false"
    )
    model = None
    try:
        model = get_federated_cim_model(eq_client, tpsvssh_client, m_cfg)
    except Exception as exc:
        logger.error(f"{exc}")
    return t_common.ModelTest(model, False, False)


def all_custom_models() -> List[t_common.ModelTest]:
    return [combined_model(), federated_model()]
