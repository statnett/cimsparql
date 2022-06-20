import logging
import os
import pathlib
from typing import Optional, Set

import pandas as pd
import pytest
import requests

from cimsparql.constants import con_mrid_str
from cimsparql.graphdb import GraphDBClient
from cimsparql.url import GraphDbConfig, service

this_dir = pathlib.Path(__file__).parent

ssh_repo = "current"
eq_repo = "20190521T0030Z"

cim_date = "20190522_070618"

logger = logging.getLogger(__name__)


def local_server() -> str:
    return os.getenv("GRAPHDB_LOCAL_TEST_SERVER", "127.0.0.2:7200")


local_graphdb = GraphDbConfig(local_server(), protocol="http")


@pytest.fixture(scope="session")
def n_samples() -> int:
    return 40


@pytest.fixture(scope="session")
def local_graphdb_config():
    return local_graphdb


@pytest.fixture(scope="session")
def gcli_eq():
    return GraphDBClient(service=service(server=local_server(), repo=eq_repo, protocol="http"))


@pytest.fixture(scope="session")
def gcli_ssh():
    return GraphDBClient(service=service(server=local_server(), repo=ssh_repo, protocol="http"))


@pytest.fixture(scope="session")
def gcli_cim():
    return GraphDBClient(service=service(server=local_server(), repo=cim_date, protocol="http"))


@pytest.fixture(scope="session")
def root_dir():
    return this_dir


@pytest.fixture(scope="session")
def server():
    return os.getenv("GRAPHDB_API", None)


@pytest.fixture(scope="session")
def graphdb_repo() -> str:
    return os.getenv("GRAPHDB_REPO", "LATEST")


@pytest.fixture(scope="session")
def graphdb_path(graphdb_repo: str) -> str:
    return "services/pgm/equipment/" if graphdb_repo == "LATEST" else ""


@pytest.fixture(scope="session")
def graphdb_service(server, graphdb_repo, graphdb_path) -> str:
    return service(graphdb_repo, server, "https", graphdb_path)


@pytest.fixture(scope="session")
def gdb_cli(graphdb_service: str) -> GraphDBClient:
    return GraphDBClient(graphdb_service)


@pytest.fixture
def type_dataframe():
    return pd.DataFrame(
        {
            "str_col": ["a", "b", "c"],
            "int_col": [1.0, 2.0, 3.0],
            "float_col": ["2.2", "3.3", "4.4"],
            "prefixed_col": ["prefix_a", "prefix_b", "prefix_c"],
            "boolean_col": ["True", "True", "False"],
        }
    )


@pytest.fixture
def type_dataframe_ref():
    return pd.DataFrame(
        {
            "str_col": ["a", "b", "c"],
            "int_col": [1, 2, 3],
            "float_col": [2.2, 3.3, 4.4],
            "prefixed_col": ["prefix_a", "prefix_b", "prefix_c"],
            "boolean_col": [True, True, False],
        }
    ).astype({"int_col": int, "str_col": "string", "prefixed_col": "string"})


def datatypes_url(datatype: str) -> str:
    return {
        "Stage.priority": "http://www.alstom.com/grid/CIM-schema-cim15-extension",
        "PerCent": "http://iec.ch/TC57/2010/CIM-schema-cim15",
        "AsynchronousMachine.converterFedDrive": "http://entsoe.eu/Secretariat/ProfileExtension/1",
    }[datatype] + f"#{datatype}"


@pytest.fixture
def sparql_data_types() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "sparql_type": [f"{datatypes_url(key)}" for key in ["Stage.priority", "PerCent"]]
            + ["http://flag"],
            "range": ["cim#Integer", "xsd#float", "xsd#boolean"],
        }
    )


@pytest.fixture
def prefixes():
    return {"cim": "cim#", "xsd": "xsd#"}


@pytest.fixture
def data_row():
    return {
        "str_col": {"type": "literal", "value": "a"},
        "int_col": {"datatype": datatypes_url("Stage.priority"), "type": "literal", "value": "1"},
        "float_col": {"datatype": datatypes_url("PerCent"), "type": "literal", "value": "2.2"},
        "prefixed_col": {"type": "uri", "value": "prefixed_a"},
        "boolean_col": {"datatype": "http://flag"},
    }


@pytest.fixture(scope="session")
def disconnectors(gdb_cli: GraphDBClient, n_samples: int) -> pd.DataFrame:
    return gdb_cli.connections(
        rdf_types="cim:Disconnector", limit=n_samples, connectivity=con_mrid_str
    )


@pytest.fixture(scope="module")
def breakers(gdb_cli: GraphDBClient, n_samples: int) -> pd.DataFrame:
    return gdb_cli.connections(rdf_types="cim:Breaker", limit=n_samples, connectivity=con_mrid_str)


@pytest.fixture(scope="session")
def rdf4j_url() -> str:
    if os.getenv("CI"):
        # Running on GitHub
        return "http://localhost:8080/rdf4j-server"

    return os.getenv("RDF4J_URL")


def upload_ttl_to_repo(
    url: str, fname: pathlib.Path, ignored_error_codes: Optional[Set[int]] = None
):
    ignored_error_codes = ignored_error_codes or set()
    with open(fname, "rb") as infile:
        data_bytes = infile.read()

    response = requests.put(url, data=data_bytes, headers={"Content-Type": "text/turtle"})
    if response.status_code not in ignored_error_codes:
        response.raise_for_status()


def initialized_rdf4j_repo_url(service_url) -> str:
    data_path = this_dir / "tests/data"

    config = data_path / "native_store_config.ttl"
    name = "picasso"
    data_file = data_path / "artist.ttl"

    # Initialize repo
    url = f"{service_url}/repositories/{name}"

    # 409 happens if repo exist before
    upload_ttl_to_repo(url, config, ignored_error_codes={409})
    upload_ttl_to_repo(url + "/statements", data_file)
    return url


@pytest.fixture(scope="session")
def rdf4j_gdb(rdf4j_url) -> Optional[GraphDBClient]:
    try:
        url = initialized_rdf4j_repo_url(rdf4j_url)
    except Exception as exc:
        logger.error(f"{exc}")
        return None
    return GraphDBClient(url)
