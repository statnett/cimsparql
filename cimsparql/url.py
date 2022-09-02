"""Functions used to configure GraphDB client

Will handle authenticated instances of GraphDB where user and password is given in environment
variables ("GRAPHDB_USER" & "GRAPHDB_USER_PASSWD").

"""
import os
import re
from typing import Dict, ItemsView, Iterable, List, Optional

import requests


def service(
    repo: Optional[str] = None,
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


def service_blazegraph(server: str, repo: str, protocol: str = "https"):
    return f"{protocol}://{server}/{repo}/sparql"


class GraphDbConfig:
    def __init__(
        self,
        server: str = "127.0.0.1:7200",
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
        """List of available repos on GraphDB server."""
        if self._repos:
            return [repo["id"]["value"] for repo in self._repos]
        return []


class Prefix:
    def __init__(self, prefixes: Dict[str, str]) -> None:
        self.prefixes = prefixes

    def update(self, pref: Dict[str, str]):
        self.prefixes.update(pref)

    def in_prefixes(self, variables: Iterable) -> Iterable:
        return {variable for variable in variables if variable.split(":")[0] in self.prefixes}

    def header_str(self, query: str) -> str:
        """Build header string, for sparql queries, with list of prefixes.

        The list of available prefixes should be provided by the source (such as GraphDB).

        """
        names_in_query = set(re.findall(r"(\w+):\w+", query))
        return "\n".join(
            f"PREFIX {name}:<{self.prefixes[name]}>"
            for name in names_in_query
            if name in self.prefixes
        )

    def items(self) -> ItemsView[str, str]:
        """Get an itemsview of prefixes in graphdb instance."""
        return self.prefixes.items()

    @property
    def cim_version(self) -> int:
        """CIM version on server/repo"""
        m = re.search("cim(\\d+)", self.prefixes["cim"])
        return int(m.group(1))

    @property
    def ns(self) -> Dict[str, str]:
        """Return namespace as dict"""
        return dict(self.items())

    @property
    def inverse_ns(self) -> Dict[str, str]:
        """Return inverse namespace as dict"""
        return {url: name for name, url in self.items()}
