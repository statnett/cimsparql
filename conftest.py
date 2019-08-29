import pytest
import pathlib

from cimsparql.graphdb import GraphDBClient
from cimsparql.redland import Model
from cimsparql.url import service, Prefix

this_dir = pathlib.Path(__file__).parent

ssh = "20190522T0730Z_1D_NO_SSH_1"

cim_date = "20190522_072846"

sv = f"cim_{cim_date}_mis_bymin_rtnet_ems_sv"
tp = f"cim_{cim_date}_mis_bymin_rtnet_ems_tp"

need_cim_ssh = pytest.mark.skipif(
    not pathlib.Path(this_dir / "data" / f"{ssh}.xml").exists(), reason=f"Require {ssh} to run"
)

need_cim_sv = pytest.mark.skipif(
    not pathlib.Path(this_dir / "data" / f"{sv}.xml").exists(), reason=f"Require {sv} to run"
)

need_cim_tp = pytest.mark.skipif(
    not pathlib.Path(this_dir / "data" / f"{tp}.xml").exists(), reason=f"Require {tp} to run"
)


@pytest.fixture(scope="session")
def ieee118():
    return Model(16, pathlib.Path(this_dir / "data" / f"IEEE118.xml"), base_uri="ieee118_case")


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
def cim15():
    return Prefix(15)


@pytest.fixture(scope="session")
def gdb_cli():
    return GraphDBClient(15, service())
