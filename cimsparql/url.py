"""Functions used to configure GraphDB client

Will handle authenticated instances of GraphDB where user and password is given in environment
variables ("GRAPHDB_USER" & "GRAPHDB_USER_PASSWD").

"""
import os
from typing import Dict, ItemsView, List, Optional

import requests


def _get_server(server: str, production: bool, repo: str) -> str:
    if repo == "LATEST":
        return ("." if production else "-test.").join(["api", "statnett.no"])
    return server


def service(
    repo: Optional[str] = None,
    server: str = "graphdb.statnett.no",
    protocol: str = "https",
    production: bool = False,
) -> str:
    """Returns service url for GraphdDBClient

    Args:
       repo: Repo on server
       server: server ip/name
       protocol: http or https
    """
    path = "services/pgm/equipment/" if repo == "LATEST" else ""
    url = f"{protocol}://{_get_server(server, production, repo)}/{path}repositories"
    if repo is not None:
        url += f"/{repo}"
    return url


class GraphDbConfig:
    def __init__(
        self,
        server: str = "graphdb.statnett.no",
        protocol: str = "https",
        auth: requests.auth.AuthBase = None,
    ) -> None:
        """Get repo configuration from GraphDB

        Args:
           server: GraphDB server
           protocol: http or https

        """
        self._service = service(None, server, protocol)
        if auth is None:
            auth = requests.auth.HTTPBasicAuth(
                os.getenv("GRAPHDB_USER"), os.getenv("GRAPHDB_USER_PASSWD")
            )
        try:
            response = requests.get(
                self._service, headers={"Accept": "application/json"}, auth=auth
            )
            response.raise_for_status()
            self._repos = response.json()["results"]["bindings"]
        except (requests.exceptions.RequestException, KeyError):
            self._repos: List[Dict[str, Dict[str, str]]] = []

    @property
    def repos(self) -> List[str]:
        """List of available repos on GraphDB server"""
        if self._repos:
            return [repo["id"]["value"] for repo in self._repos]
        return []


class Prefix:
    def header_str(self) -> str:
        """Build header string, for sparql queries, with list of prefixes

        The list of available prefixes should be provided by the source (sourch as GraphDB).

        """
        try:
            return "\n".join([f"PREFIX {name}:<{url}#>" for name, url in self.prefixes.items()])
        except AttributeError:
            return ""

    def items(self) -> ItemsView[str, str]:
        """Get an itemsview of prefixes in graphdb instance"""
        return self.prefixes.items()

    @property
    def cim_version(self) -> int:
        """CIM version on server/repo"""
        return int(self.prefixes["cim"].split("CIM-schema-cim")[1])

    @property
    def prefixes(self) -> Dict[str, str]:
        """Source defined prefixes"""
        return self._prefixes

    @property
    def ns(self) -> Dict[str, str]:
        """Return namespace as dict"""
        return {name: f"{url}#" for name, url in self.items()}

    @property
    def inverse_ns(self) -> Dict[str, str]:
        """Return inverse namespace as dict"""
        return {f"{url}#": name for name, url in self.items()}
