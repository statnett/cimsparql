import os
import pytest

from cimsparql.url import GraphDbConfig, service
from conftest import cim_date, local_server


@pytest.fixture(scope="module")
def config():
    return GraphDbConfig()


def test_default_graphdb_config_service(config):
    assert config._service == service(repo=None)


def test_default_graphdb_repos(config):
    assert "SNMST-MasterCim15-VERSION-LATEST" in config.repos()


def test_local_graphdb_config_service(local_graphdb_config):
    assert local_graphdb_config._service == service(
        server=local_server(), repo=None, protocol="http"
    )


def test_local_graphdb_repos(local_graphdb_config):
    try:
        os.environ["GRAPHDB_LOCAL_TEST_SERVER"]
        assert cim_date in local_graphdb_config.repos()
    except KeyError:
        pass


def test_local_graphdb_config_raise_connection_exception():
    graphdb_config = GraphDbConfig(server="127.0.0.1:7200")
    assert graphdb_config.repos() == []
