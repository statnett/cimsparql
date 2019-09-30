import os
import pytest
import pathlib

from cimsparql.graphdb import GraphDBClient
from cimsparql.redland import Model
from cimsparql.url import service, GraphDbConfig

this_dir = pathlib.Path(__file__).parent

ssh = "20190522T0730Z_1D_NO_SSH_1"

cim_date = "20190522_070618"

sv = f"cim_{cim_date}_val_bymin_rtnet_ems_sv"
tp = f"cim_{cim_date}_val_bymin_rtnet_ems_tp"


need_cim_ssh = pytest.mark.skipif(
    not pathlib.Path(this_dir / "data" / f"{ssh}.xml").exists(), reason=f"Require {ssh} to run"
)

need_cim_sv = pytest.mark.skipif(
    not pathlib.Path(this_dir / "data" / f"{sv}.xml").exists(), reason=f"Require {sv} to run"
)

need_cim_tp = pytest.mark.skipif(
    not pathlib.Path(this_dir / "data" / f"{tp}.xml").exists(), reason=f"Require {tp} to run"
)


def local_server():
    try:
        return os.environ["GRAPHDB_LOCAL_TEST_SERVER"]
    except KeyError:
        return "127.0.0.1:7200"


need_local_graphdb = pytest.mark.skipif(
    GraphDbConfig(local_server(), protocol="http").repos() == [],
    reason="Need repos in local repository",
)


@pytest.fixture(scope="session")
def local_graphdb_config():
    return GraphDbConfig(server=local_server(), protocol="http")


@pytest.fixture(scope="session")
def ieee118():
    return Model(pathlib.Path(this_dir / "data" / f"IEEE118.xml"), base_uri="ieee118_case")


@pytest.fixture(scope="session")
def sv_profile():
    return sv


@pytest.fixture(scope="session")
def ssh_profile():
    return ssh


@pytest.fixture(scope="session")
def tp_profile():
    return tp


@pytest.fixture(scope="session")
def root_dir():
    return this_dir


@pytest.fixture(scope="session")
def gdb_cli():
    return GraphDBClient(service())


@pytest.fixture(scope="session")
def gdb_cli_local():
    return GraphDBClient(service())
