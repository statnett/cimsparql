import pytest
import pathlib

from cimsparql.graphdb import GraphDBClient
from cimsparql.url import service, Prefix

this_dir = pathlib.Path(__file__).parent

ssh = "20190522T0730Z_1D_NO_SSH_1"

need_cim_ssh = pytest.mark.skipif(
    not pathlib.Path(this_dir / "data" / f"{ssh}.xml").exists(), reason=f"Require {ssh} to run"
)


@pytest.fixture(scope="session")
def identifier():
    return ssh


@pytest.fixture(scope="session")
def root_dir():
    return this_dir


@pytest.fixture(scope="session")
def cim15():
    return Prefix(15)


@pytest.fixture(scope="session")
def cim16():
    return Prefix(16)


@pytest.fixture(scope="session")
def gdb_cli():
    return GraphDBClient(service())
