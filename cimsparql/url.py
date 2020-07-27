from typing import Dict, ItemsView, List, Optional

import requests


def service(
    repo: Optional[str], server: str = "graphdb.statnett.no", protocol: str = "https"
) -> str:
    """Returns service url for GraphdDBClient

    Args:
       repo: Repo on server
       server: server ip/name
       protocol: http or https
    """
    if repo == "LATEST":
        url = "https://api.statnett.no/services/pgm/equipment/repositories/LATEST"
    else:
        url = f"{protocol}://{server}/repositories"
        if repo is not None:
            url += f"/{repo}"
    return url


class GraphDbConfig:
    def __init__(self, server: str = "graphdb.statnett.no", protocol: str = "https") -> None:
        """Get repo configuration from GraphDB

        Args:
           server: GraphDB server
           protocol: http or https

        """
        self._service = service(None, server, protocol)
        try:
            response = requests.get(self._service, headers={"Accept": "application/json"})
            self._set_repos(response)
        except requests.exceptions.RequestException:
            self._repos: List[str] = []

    def _set_repos(self, response: requests.Response) -> None:
        if response is not None and response.ok:
            self._repos = response.json()["results"]["bindings"]
        else:
            self._repos = []

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
        return {name: f"{url}#" for name, url in self.items()}

    @property
    def inverse_ns(self) -> Dict[str, str]:
        return {f"{url}#": name for name, url in self.items()}
