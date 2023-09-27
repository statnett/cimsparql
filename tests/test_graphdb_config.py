import os
from unittest.mock import MagicMock, Mock, patch

from httpx import RequestError

from cimsparql.url import GraphDbConfig, service


@patch("cimsparql.url.httpx.get")
def test_set_repos_with_response_but_key_error(get_mock: Mock):
    get_mock.return_value = MagicMock(json=MagicMock(side_effect=KeyError()))
    config = GraphDbConfig()
    assert config.repos == []


@patch("cimsparql.url.httpx.get")
def test_set_repos_no_response(get_mock: Mock):
    get_mock.side_effect = RequestError("")
    config = GraphDbConfig()
    assert config.repos == []


def test_local_graphdb_config_service():
    server = os.getenv("GRAPHDB_SERVER", "127.0.0.1:7200")
    config = GraphDbConfig(server)
    assert config._service == service(server=server)


def test_local_graphdb_config_raise_connection_exception():
    graphdb_config = GraphDbConfig(server="127.0.0.1:7200")
    assert graphdb_config.repos == []
