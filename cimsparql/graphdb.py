"""Graphdb CIM sparql client"""
import json
import os
from dataclasses import dataclass, field
from enum import auto
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from deprecated import deprecated
from SPARQLWrapper import JSON, SPARQLWrapper
from strenum import StrEnum

from cimsparql.url import Prefix, service, service_blazegraph


def data_row(cols: List[str], rows: List[Dict[str, str]]) -> Dict[str, str]:
    """Get a sample row for extraction of data types

    Args:
       cols: queried columns (optional might return None)
       rows: 'results'→'bindings' from SPARQLWrapper

    Returns:
       samples result of all columns in query
    """
    full_row = {}
    for row in rows:
        if set(cols).difference(full_row):
            full_row.update(row)
        else:
            break
    return full_row


class RestApi(StrEnum):
    RDF4J = auto()
    BLAZEGRAPH = auto()


def parse_namespaces_rdf4j(response: requests.Response) -> Dict[str, str]:
    prefixes = {}
    for line in response.text.split()[1:]:
        prefix, uri = line.split(",")
        prefixes[prefix] = uri
    return prefixes


@dataclass
class ServiceConfig:
    repo: str = field(default=os.getenv("GRAPHDB_REPO", "LATEST"))
    protocol: str = "https"
    server: str = field(default=os.getenv("GRAPHDB_SERVER", "127.0.0.1:7200"))
    path: str = ""
    user: str = field(default=os.getenv("GRAPHDB_USER"))
    passwd: str = field(default=os.getenv("GRAPHDB_USER_PASSWD"))
    rest_api: RestApi = field(default=os.getenv("SPARQL_REST_API", RestApi.RDF4J))

    def __post_init__(self):
        if self.rest_api not in RestApi:
            raise ValueError(f"rest_api must be one of {RestApi}")

    @property
    def url(self) -> str:
        if self.rest_api == RestApi.BLAZEGRAPH:
            return service_blazegraph()
        return service(self.repo, self.server, self.protocol, self.path)


# Available formats from RDF4J API
# https://rdf4j.org/documentation/reference/rest-api/
MIME_TYPE_RDF_FORMATS = {
    "rdf/xml": "application/rdf+xml",
    "n-triples": "text/plain",
    "turtle": "text/turtle",
    "n3": "text/rdf+n3",
    "n-quads": "text/x-nquads",
    "json-ld": "application/ld+json",
    "rdf/json": "application/rdf+json",
    "trix": "application/trix",
    "trig": "application/x-trig",
    "rdf4J binary rdf": "application/x-binary-rdf",
}

UPLOAD_END_POINT = {RestApi.RDF4J: "/statements", RestApi.BLAZEGRAPH: ""}


def require_rdf4j(f):
    def wrapper(*args):
        self = args[0]
        if self.service_cfg.rest_api != RestApi.RDF4J:
            raise NotImplementedError("Function only implemented for RDF4J")
        return f(*args)

    return wrapper


class GraphDBClient:
    def __init__(
        self, service_cfg: Optional[ServiceConfig] = None, infer: bool = False, sameas: bool = True
    ) -> None:
        """GraphDB client

        Args:
           service: string with url to graphdb repository. See cimsparql.url.service
           mapper: GraphDBClient with the mapper (Default to self).
           infer: deduce further knowledge based on existing RDF data and a formal set of
           sameas: map same concepts from two or more datasets
        """
        self.service_cfg = service_cfg or ServiceConfig()
        self.sparql = SPARQLWrapper(self.service_cfg.url)
        self.sparql.setReturnFormat(JSON)
        self.sparql.setCredentials(self.service_cfg.user, self.service_cfg.passwd)
        self.set_parameter("infer", str(infer))
        self.set_parameter("sameAs", str(sameas))
        self._prefixes = None

    def set_parameter(self, key: str, value: str) -> None:
        self.sparql.clearParameter(key)
        self.sparql.addParameter(key, value)

    def set_repo(self, repo: str) -> None:
        self.service_cfg.repo = repo
        self.sparql.endpoint = self.service_cfg.url

    @property
    def prefixes(self) -> Prefix:
        if self._prefixes is None:
            pref = self.get_prefixes()
            self._prefixes = Prefix(pref)
        return self._prefixes

    def update_prefixes(self, pref: Dict[str, str]):
        """
        Update prefixes from a dict
        """
        self.prefixes.update(pref)

    def __str__(self) -> str:
        return f"<GraphDBClient object, service: {self.service_cfg.url}>"

    def query_with_header(self, query: str, add_prefixes: bool, limit: Optional[int] = None) -> str:
        if add_prefixes:
            query = "\n".join([self.prefixes.header_str(query), query])
        if limit is not None:
            query += f" limit {limit}"
        return query

    def _exec_query(self, query: str, limit: Optional[int], add_prefixes: bool):
        self.sparql.setQuery(self.query_with_header(query, add_prefixes, limit))

        processed_results = self.sparql.queryAndConvert()

        cols = processed_results["head"]["vars"]
        data = processed_results["results"]["bindings"]
        out = [{c: row.get(c, {}).get("value") for c in cols} for row in data]
        return out, data_row(cols, data)

    def exec_query(self, query: str, limit: Optional[int] = None) -> List[Dict[str, str]]:
        out, _ = self._exec_query(query, limit)
        return out

    def get_table(
        self, query: str, limit: Optional[int] = None, add_prefixes: bool = True
    ) -> Tuple[pd.DataFrame, Dict[str, str]]:
        """
        Args:
           query: to sparql server
           limit: limit number of resulting rows
        Example:
           >>> from cimsparql.graphdb import GraphDBClient
           >>> from cimsparql.url import service
           >>> gdbc = GraphDBClient(service('LATEST'))
           >>> query = 'select * where { ?subject ?predicate ?object }'
           >>> gdbc.get_table(query, limit=10)
        """
        out, data_row = self._exec_query(query, limit, add_prefixes)
        return pd.DataFrame(out), data_row

    @property
    def empty(self) -> bool:
        """Identify empty GraphDB repo"""
        try:
            self.get_table("select * where {?s ?p ?o}", limit=1)
            return False
        except IndexError:
            return True

    def get_prefixes(self) -> Dict[str, str]:
        if self.service_cfg.rest_api == RestApi.BLAZEGRAPH:
            # Blazegraph does not expose prefixes over API
            # When using Blazegraph custom prefixes must be added via `update_prefixes`
            # By default we load a pre-defined set of prefixes
            return default_namespaces()

        prefixes = {}
        auth = requests.auth.HTTPBasicAuth(self.service_cfg.user, self.service_cfg.passwd)
        response = requests.get(self.service_cfg.url + "/namespaces", auth=auth)
        if response.ok:
            prefixes = parse_namespaces_rdf4j(response)
        else:
            msg = (
                "Could not fetch namespaces and prefixes from graphdb "
                "Verify that user and password are correctly set in the "
                "GRAPHDB_USER and GRAPHDB_USER_PASSWD environment variable"
            )
            raise RuntimeError(
                f"{msg} Status code: {response.status_code} Reason: {response.reason}"
            )
        return prefixes

    def delete_repo(self):
        endpoint = delete_repo_endpoint(self.service_cfg)
        response = requests.delete(endpoint)
        response.raise_for_status()

    def upload_rdf(self, fname: Path, rdf_format: str):
        with open(fname, "rb") as infile:
            xml_content = infile.read()

        response = requests.post(
            self.service_cfg.url + UPLOAD_END_POINT[self.service_cfg.rest_api],
            data=xml_content,
            headers={"Content-Type": MIME_TYPE_RDF_FORMATS[rdf_format]},
        )
        response.raise_for_status()

    def update_query(self, query: str):
        """
        Function that passes a query via a post API call
        """
        response = requests.post(
            self.service_cfg.url + UPLOAD_END_POINT[self.service_cfg.rest_api],
            data={"update": query},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response.raise_for_status()

    @require_rdf4j
    def set_namespace(self, prefix: str, value: str):
        auth = requests.auth.HTTPBasicAuth(self.service_cfg.user, self.service_cfg.passwd)
        response = requests.put(
            self.service_cfg.url + f"/namespaces/{prefix}",
            data=value,
            headers={"Content-Type": "text/plain"},
            auth=auth,
        )
        response.raise_for_status()

    @require_rdf4j
    def get_namespace(self, prefix: str) -> str:
        auth = requests.auth.HTTPBasicAuth(self.service_cfg.user, self.service_cfg.passwd)
        response = requests.get(self.service_cfg.url + f"/namespaces/{prefix}", auth=auth)
        response.raise_for_status()
        return response.text


@deprecated(version="1.11", reason="Use cimsparqel.model.get_cim_model instead")
def get_graphdb_client(
    server: str,
    graphdb_repo: str,
    graphdb_path: str = "services/pgm/equipment/",
    protocol: str = "https",
):
    from cimsparql.model import get_cim_model

    return get_cim_model(server, graphdb_repo, graphdb_path, protocol)


@dataclass
class RepoInfo:
    uri: str
    repo_id: str
    title: str
    readable: bool
    writable: bool


def repos(server: str) -> List[RepoInfo]:
    """
    List available repositories
    """
    response = requests.get(server + "/repositories")
    response.raise_for_status()

    infos = []
    for line in response.text.split("\n")[1:]:
        if not line:
            continue
        uri, repo_id, title, readable, writable = line.strip().split(",")
        readable = readable == "true"
        writable = writable == "true"
        info = RepoInfo(uri=uri, repo_id=repo_id, title=title, readable=readable, writable=writable)
        infos.append(info)
    return infos


def new_repo(
    server: str, repo: str, config: bytes, allow_exist: bool = True, protocol: str = "http"
) -> GraphDBClient:
    """
    Initiialzie a new repository

    Args:
        server: URL to service
        repo: Name of repo
        config: Bytes representaiton of a Turtle config file
        allow_exist: If True, a client pointing to the existing repo is returned.
            Otherwise an error is raised if a repository with the same name already exists
        protocol: Default https
    """
    ignored_errors = {409} if allow_exist else set()
    url = service(repo, server, protocol)
    response = requests.put(url, data=config, headers={"Content-Type": "text/turtle"})
    if response.status_code not in ignored_errors:
        response.raise_for_status()

    return GraphDBClient(url)


def new_repo_blazegraph(url: str, repo: str, protocol: str = "https") -> GraphDBClient:
    template = confpath() / "blazegraph_repo_config.xml"
    config = config_bytes_from_template(template, {"repo": repo})

    response = requests.post(
        f"http://{url}", data=config, headers={"Content-type": "application/xml"}
    )
    response.raise_for_status()
    client = GraphDBClient(service_blazegraph(url, repo, protocol))
    client.service_cfg.rest_api = RestApi.BLAZEGRAPH
    return client


def config_bytes_from_template(
    template: Path, params: Dict[str, str], encoding: str = "utf8"
) -> bytes:
    """
    Replace value in template file with items in params

    Args:
        template: Path to template file
        params: Dict with key-value pairs where key must be enclode by double curly braces
            in the template file ({{}}). {{key}} will be replaced by value
    """
    with open(template, "rb") as infile:
        data = infile.read()

    for param, value in params.items():
        enc_templ = f"{{{{{param}}}}}".encode(encoding)
        enc_value = value.encode(encoding)
        data = data.replace(enc_templ, enc_value)
    return data


def confpath() -> Path:
    return Path(__file__).parent.parent / "pkg_data"


def default_namespaces() -> Dict[str, str]:
    with open(confpath() / "namespaces.json") as infile:
        return json.load(infile)


def delete_repo_endpoint(config: ServiceConfig) -> str:
    if config.rest_api == RestApi.BLAZEGRAPH:
        # Remove /sparql at the end
        return config.url.rpartition("/")[0]
    return config.url
