import functools
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from cimsparql.graphdb import GraphDBClient, config_bytes_from_template, confpath, new_repo
from cimsparql.model import Model

this_dir = Path(__file__).parent


@dataclass
class ModelTest:
    model: Optional[Model] = None
    must_run_in_ci: bool = True
    cleanup: bool = True


@functools.lru_cache
def init_repo_rdf4j(url: str, name: str) -> GraphDBClient:
    template = confpath() / "native_store_config_template.ttl"
    config = config_bytes_from_template(template, {"repo": name})
    return new_repo(url, name, config, allow_exist=False, protocol="http")


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
