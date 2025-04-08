from __future__ import annotations

import functools
import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from cimsparql.graphdb import GraphDBClient, ServiceConfig, config_bytes_from_template, confpath, new_repo

if TYPE_CHECKING:
    from cimsparql.model import Model

this_dir = Path(__file__).parent


@dataclass
class ModelTest:
    model: Model | None = None
    must_run_in_ci: bool = True
    cleanup: bool = True
    name: str = ""


@functools.lru_cache
def init_repo_rdf4j(url: str, name: str) -> GraphDBClient:
    template = confpath() / "native_store_config_template.ttl"
    config = config_bytes_from_template(template, {"repo": name})
    service_config = ServiceConfig(repo=name, server=url, protocol="http", timeout=30)
    return new_repo(service_config, config, allow_exist=False)


@functools.lru_cache
def initialized_rdf4j_repo() -> GraphDBClient:
    data_path = this_dir.parent / "data"
    client = init_repo_rdf4j(rdf4j_url(), "picasso")

    data_file = data_path / "artist.ttl"
    client.upload_rdf(data_file, "turtle")
    return client


def rdf4j_url() -> str:
    if os.getenv("CI"):
        # Running on GitHub
        return "localhost:8080/rdf4j-server"

    return os.getenv("RDF4J_URL", "")


def blazegraph_url() -> str:
    if os.getenv("CI"):
        # Running on GitHub
        return "localhost:9999/blazegraph/namespace"
    return os.getenv("BLAZEGRAPH_URL", "")


def check_model(test_model: ModelTest) -> None:
    if not test_model.model:
        if test_model.must_run_in_ci and os.getenv("CI"):
            pytest.fail("Micro model test can not be skipped in CI pipelines")
        else:
            pytest.skip("Can not access micro model")
