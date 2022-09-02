import os

import requests
from mock import MagicMock, patch

from cimsparql.url import GraphDbConfig, service


@patch("cimsparql.url.requests.get")
def test_set_repos_with_response_but_key_error(get_mock):
    get_mock.return_value = MagicMock(json=MagicMock(side_effect=KeyError()))
    config = GraphDbConfig()
    assert config.repos == []


@patch("cimsparql.url.requests.get")
def test_set_repos_no_response(get_mock):
    get_mock.side_effect = requests.exceptions.RequestException()
    config = GraphDbConfig()
    assert config.repos == []


def test_local_graphdb_config_service():
    server = os.getenv("GRAPHDB_SERVER")
    config = GraphDbConfig(server)
    assert config._service == service(server=server)


def test_local_graphdb_config_raise_connection_exception():
    graphdb_config = GraphDbConfig(server="127.0.0.1:7200")
    assert graphdb_config.repos == []
