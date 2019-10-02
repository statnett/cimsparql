import requests
from typing import Dict


def service(
    server: str = "graphdb.statnett.no",
    repo: str = "SNMST-Master1Repo-VERSION-LATEST",
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

    def repos(self):
        return [repo["id"]["value"] for repo in self._repos]


class Prefix(object):
    def set_cim_version(self):
        self._cim_version = int(self.prefix_dict["cim"].split("CIM-schema-cim")[1])

    def header_str(self) -> str:
        try:
            return "\n".join([f"PREFIX {name}:<{url}#>" for name, url in self.prefix_dict.items()])
        except AttributeError:
            return ""

    def get_prefix_dict(self, service: str):
        self.prefix_dict = {}
        response = requests.get(service + f"/namespaces")
        if response.ok:
            for line in response.text.split():
                prefix, uri = line.split(",")
                if prefix != "prefix":
                    self.prefix_dict[prefix] = uri.rstrip("#")

    def ns(self) -> Dict:
        return {name: f"{url}#" for name, url in self.prefix_dict.items()}

    def items(self):
        return self.prefix_dict.items()

    def inverse(self) -> Dict:
        return {f"{url}#": name for name, url in self.prefix_dict.items()}
