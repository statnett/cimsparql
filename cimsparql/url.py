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
        """List of available repos on GraphDB server"""
        if self._repos:
            return [repo["id"]["value"] for repo in self._repos]
        return []


class Prefix:
    def __init__(self) -> None:
        self._prefixes = None

    @property
    def prefixes(self) -> Dict[str, str]:
        if self._prefixes is None:
            self._prefixes = self._get_prefixes()
        return self._prefixes

    def _get_prefixes(self) -> Dict[str, str]:
        prefixes = {}
        auth = requests.auth.HTTPBasicAuth(self.user, self.passwd)
        response = requests.get(self.service + "/namespaces", auth=auth)
        if response.ok:
            for line in response.text.split():
                prefix, uri = line.split(",")
                if prefix != "prefix":
                    prefixes[prefix] = uri.rstrip("#")
        else:
            msg = (
                "Could not fetch namespaces and prefixes from graphdb "
                "Verify that user and password are correctly set in the "
                "GRAPHDB_USER and GRAPHDB_USER_PASSWD environment variable"
            )
            raise RuntimeError(
                f"{msg}\nStatus code: {response.status_code}\nReason: {response.reason}"
            )
        return prefixes

    def in_prefixes(self, variables: Iterable) -> Iterable:
        return {variable for variable in variables if variable.split(":")[0] in self.prefixes}

    def header_str(self, query: str) -> str:
        """Build header string, for sparql queries, with list of prefixes

        The list of available prefixes should be provided by the source (such as GraphDB).

        """
        try:
            return "\n".join(
                [
                    f"PREFIX {name}:<{self.prefixes[name]}#>"
                    for name in set(re.findall(r"(\w+):\w+", query))
                    if name in self.prefixes
                ]
            )
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
    def ns(self) -> Dict[str, str]:
        """Return namespace as dict"""
        return {name: f"{url}#" for name, url in self.items()}

    @property
    def inverse_ns(self) -> Dict[str, str]:
        """Return inverse namespace as dict"""
        return {f"{url}#": name for name, url in self.items()}
