import requests
from typing import Dict, List


def service(
    server: str = "graphdb.statnett.no",
    repo: str = "SNMST-MasterCim15-VERSION-LATEST",
    protocol: str = "https",
) -> str:
    url = f"{protocol}://{server}/repositories"
    if repo is not None:
        url += f"/{repo}"
    return url


class GraphDbConfig(object):
    def __init__(self, server: str = "graphdb.statnett.no", protocol: str = "https"):
        self._service = service(server, None, protocol)
        try:
            repos = requests.get(self._service, headers={"Accept": "application/json"})
            if repos.ok:
                self._repos = repos.json()["results"]["bindings"]
            else:
                self._repos = {}
        except requests.exceptions.ConnectionError:
            self._repos = {}

    def repos(self) -> List[str]:
        return [repo["id"]["value"] for repo in self._repos]


class Prefix(object):
    def set_cim_version(self):
        self._cim_version = int(self.prefix_dict["cim"].split("CIM-schema-cim")[1])

    def header_str(self) -> str:
        try:
            return "\n".join([f"PREFIX {name}:<{url}#>" for name, url in self.prefix_dict.items()])
        except AttributeError:
            return ""

    def ns(self) -> Dict[str, str]:
        return {name: f"{url}#" for name, url in self.prefix_dict.items()}

    def items(self):
        return self.prefix_dict.items()

    def inverse(self) -> Dict[str, str]:
        return {f"{url}#": name for name, url in self.prefix_dict.items()}
