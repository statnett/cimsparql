"""Functions used to configure GraphDB client

Will handle authenticated instances of GraphDB where user and password is given in environment
variables ("GRAPHDB_USER" & "GRAPHDB_USER_PASSWD").

"""

from __future__ import annotations

import os

import httpx


def service(
    repo: str | None = None,
    server: str = "127.0.0.1:7200",
    protocol: str = "https",
    path: str = "",
) -> str:
    """Returns service url for GraphdDBClient

    Args:
       repo: Repo on server
       server: server ip/name
       protocol: http or https
    """
    url = f"{protocol}://{server}/{path}repositories"
    if repo:
        url += f"/{repo}"
    return url


def service_blazegraph(server: str, repo: str, protocol: str = "https") -> str:
    return f"{protocol}://{server}/{repo}/sparql"


class GraphDbConfig:
    def __init__(
        self,
        server: str = "127.0.0.1:7200",
        protocol: str = "https",
        auth: httpx.BasicAuth | None = None,
    ) -> None:
        """Get repo configuration from GraphDB

        Args:
           server: GraphDB server
           protocol: http or https

        """
        self._service = service(None, server, protocol)
        if auth is None:
            auth = httpx.BasicAuth(
                os.getenv("GRAPHDB_USER", ""), os.getenv("GRAPHDB_USER_PASSWD", "")
            )
        try:
            response = httpx.get(self._service, headers={"Accept": "application/json"}, auth=auth)
            response.raise_for_status()
            self._repos = response.json()["results"]["bindings"]
        except (httpx.RequestError, KeyError):
            self._repos: list[dict[str, dict[str, str]]] = []

    @property
    def repos(self) -> list[str]:
        """List of available repos on GraphDB server."""
        if self._repos:
            return [repo["id"]["value"] for repo in self._repos]
        return []
