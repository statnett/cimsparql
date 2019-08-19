import pytest
import pathlib

from cimsparql.graphdb import GraphDBClient
from cimsparql.url import service, Prefix

this_dir = pathlib.Path(__file__).parent


@pytest.fixture(scope="session")
def cim15():
    return Prefix(15)


@pytest.fixture(scope="session")
def cim16():
    return Prefix(16)


@pytest.fixture(scope="session")
def gdb_cli():
    return GraphDBClient(service())
