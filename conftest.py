import os
import pytest
import pathlib

from cimsparql.graphdb import GraphDBClient
from cimsparql.redland import Model
from cimsparql.url import service, GraphDbConfig

this_dir = pathlib.Path(__file__).parent

ssh_repo = "20190522T0730Z"
eq_repo = "20190521T0030Z"

cim_date = "20190522_070618"

sv = f"cim_{cim_date}_val_bymin_rtnet_ems_sv"
tp = f"cim_{cim_date}_val_bymin_rtnet_ems_tp"


def local_server():
    try:
        return os.environ["GRAPHDB_LOCAL_TEST_SERVER"]
    except KeyError:
        return "127.0.0.1:7200"


local_graphdb = GraphDbConfig(local_server(), protocol="http")


need_local_graphdb_ssh = pytest.mark.skipif(
    ssh_repo not in local_graphdb.repos(), reason=f"Need {ssh_repo} in local repository"
)

need_local_graphdb_eq = pytest.mark.skipif(
    eq_repo not in local_graphdb.repos(), reason=f"Need {eq_repo} in local repository"
)

need_local_graphdb_cim = pytest.mark.skipif(
    cim_date not in local_graphdb.repos(), reason=f"Need {cim_date} in local repository"
)


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
def ieee118():
    return Model(pathlib.Path(this_dir / "data" / f"IEEE118.xml"), base_uri="ieee118_case")


@pytest.fixture(scope="session")
def root_dir():
    return this_dir


@pytest.fixture(scope="session")
def gdb_cli():
    return GraphDBClient(service())
