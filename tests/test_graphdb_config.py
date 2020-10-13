import pytest
from mock import MagicMock, patch

from cimsparql.url import GraphDbConfig, service
from conftest import local_server


@pytest.fixture(scope="module")
def config():
    return GraphDbConfig()


def test_default_graphdb_config_service(config):
    assert config._service == service(repo=None)


@patch("cimsparql.url.GraphDbConfig.__init__", new=MagicMock(return_value=None))
def test_set_repos_response_is_none():
    config = GraphDbConfig()
    config._set_repos(None)
    assert config.repos == []


@patch("cimsparql.url.GraphDbConfig.__init__", new=MagicMock(return_value=None))
def test_set_repos_no_response():
    config = GraphDbConfig()
    config._set_repos(MagicMock(ok=False))
    assert config.repos == []


def test_default_graphdb_repos(config):
    assert any([repo.startswith("SNMST-MasterCim15-VERSION") for repo in config.repos])


def test_local_graphdb_config_service(local_graphdb_config):
    assert local_graphdb_config._service == service(
        server=local_server(), repo=None, protocol="http"
    )


def test_local_graphdb_config_raise_connection_exception():
    graphdb_config = GraphDbConfig(server="127.0.0.1:7200")
    assert graphdb_config.repos == []
