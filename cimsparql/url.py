import requests
from typing import Dict, List


def service(repo: str, server: str = "graphdb.statnett.no", protocol: str = "https") -> str:
    """Returns service url for GraphdDBClient

    :param repo: Repo to use
    :param server: server name
    :param protocol:
    """
    url = f"{protocol}://{server}/repositories"
    if repo is not None:
        url += f"/{repo}"
    return url


class GraphDbConfig(object):
    def __init__(self, server: str = "graphdb.statnett.no", protocol: str = "https"):
        """Get repo configuration from GraphDB

        :param server: GraphDB server
        :param protocol:
        """
        self._service = service(None, server, protocol)
        try:
            repos = requests.get(self._service, headers={"Accept": "application/json"})
            if repos.ok:
                self._repos = repos.json()["results"]["bindings"]
            else:
                self._repos = {}
        except requests.exceptions.ConnectionError:
            self._repos = {}

    def repos(self) -> List[str]:
        """List available repos on GraphDB server"""
        return [repo["id"]["value"] for repo in self._repos]


class Prefix(object):
    def header_str(self) -> str:
        try:
            return "\n".join([f"PREFIX {name}:<{url}#>" for name, url in self.prefixes.items()])
        except AttributeError:
            return ""

    def items(self):
        return self.prefixes.items()

    @property
    def cim_version(self) -> int:
        return int(self.prefixes["cim"].split("CIM-schema-cim")[1])

    @property
    def prefixes(self) -> Dict[str, str]:
        return self._prefixes

    @property
    def ns(self) -> Dict[str, str]:
        return {name: f"{url}#" for name, url in self.items()}

    @property
    def inverse_ns(self) -> Dict[str, str]:
        return {f"{url}#": name for name, url in self.items()}
