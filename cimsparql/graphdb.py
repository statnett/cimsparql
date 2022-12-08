"""Graphdb CIM sparql client"""
import json
import os
from dataclasses import dataclass, field
from enum import auto
from pathlib import Path
from typing import Dict, List, Optional, Tuple, TypedDict, Union

import httpx
import pandas as pd
import requests
from SPARQLWrapper import JSON, POST, SPARQLWrapper
from strenum import StrEnum

from cimsparql.async_sparql_wrapper import AsyncSparqlWrapper
from cimsparql.url import service, service_blazegraph


class SparqlResult(TypedDict):
    cols: List[str]
    data: List[Dict[str, Dict[str, str]]]
    out: List[Dict[str, str]]


def data_row(cols: List[str], rows: List[Dict[str, Dict[str, str]]]) -> Dict[str, str]:
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
    DIRECT_SPARQL_ENDPOINT = auto()


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

    # Parameters for rest api
    # https://rdf4j.org/documentation/reference/rest-api/
    distinct: bool = False
    infer: bool = False
    limit: Optional[int] = None
    offset: Optional[int] = None
    timeout: Optional[int] = None

    def __post_init__(self):
        if self.rest_api not in RestApi:
            raise ValueError(f"rest_api must be one of {RestApi}")

    @property
    def url(self) -> str:
        if self.rest_api == RestApi.BLAZEGRAPH:
            return service_blazegraph(self.server, self.repo, self.protocol)
        elif self.rest_api == RestApi.DIRECT_SPARQL_ENDPOINT:
            return self.server
        return service(self.repo, self.server, self.protocol, self.path)

    @property
    def parameters(self) -> Dict[str, Union[bool, int]]:
        return {
            "distinct": self.distinct,
            "infer": self.infer,
            "limit": self.limit,
            "offset": self.offset,
        }


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
    def __init__(self, service_cfg: Optional[ServiceConfig] = None) -> None:
        """GraphDB client

        Args:
           service: string with url to graphdb repository. See cimsparql.url.service
           mapper: GraphDBClient with the mapper (Default to self).
           infer: deduce further knowledge based on existing RDF data and a formal set of
           sameas: map same concepts from two or more datasets
        """
        self.service_cfg = service_cfg or ServiceConfig()
        self.sparql = SPARQLWrapper(self.service_cfg.url)
        self._init_sparql_wrapper()
        self._prefixes = None

    def _init_sparql_wrapper(self):
        self.sparql.setReturnFormat(JSON)
        self.sparql.setMethod(POST)
        self.sparql.setCredentials(self.service_cfg.user, self.service_cfg.passwd)
        if self.service_cfg.timeout:
            self.sparql.setTimeout(self.service_cfg.timeout)
        self._update_sparql_parameters()

    def _update_sparql_parameters(self):
        for key, value in self.service_cfg.parameters.items():
            if value is not None:
                self.set_parameter(key, str(value))

    def set_parameter(self, key: str, value: str) -> None:
        self.sparql.clearParameter(key)
        self.sparql.addParameter(key, value)

    def set_repo(self, repo: str) -> None:
        self.service_cfg.repo = repo
        self.sparql.endpoint = self.service_cfg.url

    @property
    def prefixes(self) -> Dict[str, str]:
        if self._prefixes is None:
            self._prefixes = self.get_prefixes()
        return self._prefixes

    def update_prefixes(self, pref: Dict[str, str]):
        """
        Update prefixes from a dict
        """
        self.prefixes.update(pref)

    def __str__(self) -> str:
        return f"<GraphDBClient object, service: {self.service_cfg.url}>"

    def _prep_query(self, query: str):
        self.sparql.setQuery(query)
        self._update_sparql_parameters()

    def _process_result(self, results: dict) -> dict:
        cols = results["head"]["vars"]
        data = results["results"]["bindings"]
        out = [{c: row.get(c, {}).get("value") for c in cols} for row in data]
        return {"out": out, "cols": cols, "data": data}

    def _exec_query(self, query: str) -> SparqlResult:
        self._prep_query(query)
        results = self.sparql.queryAndConvert()
        return self._process_result(results)

    def exec_query(self, query: str) -> List[Dict[str, str]]:
        out = self._exec_query(query)
        return out["out"]

    def _convert_query_result_to_df(self, res: SparqlResult) -> Tuple[pd.DataFrame, Dict[str, str]]:
        df = pd.DataFrame(res["out"]) if len(res["out"]) else pd.DataFrame(columns=res["cols"])
        return df, data_row(res["cols"], res["data"])

    def get_table(self, query: str) -> Tuple[pd.DataFrame, Dict[str, str]]:
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
        res = self._exec_query(query)
        return self._convert_query_result_to_df(res)

    @property
    def empty(self) -> bool:
        """Identify empty GraphDB repo"""
        return self.get_table("select * where {?s ?p ?o} limit 1")[0].empty

    def get_prefixes(self) -> Dict[str, str]:
        prefixes = default_namespaces()

        if self.service_cfg.rest_api in (RestApi.BLAZEGRAPH, RestApi.DIRECT_SPARQL_ENDPOINT):
            # These APis does not expose prefixes. Custom prefixes must be added
            # via `update_prefixes`. By default we load a pre-defined set of prefixes
            return prefixes

        auth = requests.auth.HTTPBasicAuth(self.service_cfg.user, self.service_cfg.passwd)
        response = requests.get(self.service_cfg.url + "/namespaces", auth=auth)
        if response.ok:
            prefixes.update(parse_namespaces_rdf4j(response))
            return prefixes
        msg = (
            "Could not fetch namespaces and prefixes from graphdb "
            "Verify that user and password are correctly set in the "
            "GRAPHDB_USER and GRAPHDB_USER_PASSWD environment variable"
        )
        raise RuntimeError(f"{msg} Status code: {response.status_code} Reason: {response.reason}")

    def delete_repo(self) -> None:
        endpoint = delete_repo_endpoint(self.service_cfg)
        response = requests.delete(endpoint)
        response.raise_for_status()

    def upload_rdf(
        self, fname: Path, rdf_format: str, params: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Upload data in RDF format to a srevice

        Args
            fname: Filename to the RDF file to upload
            rdf_format: RDF file type (e.g. rdf/xml, rdf/json etc. Consult the RDF4J rest api
                doc for a complete list of available options)
            params: Additional parameters passed to the post request
        """
        with open(fname, "rb") as infile:
            xml_content = infile.read()

        response = requests.post(
            self.service_cfg.url + UPLOAD_END_POINT[self.service_cfg.rest_api],
            data=xml_content,
            params=params,
            headers={"Content-Type": MIME_TYPE_RDF_FORMATS[rdf_format]},
        )
        response.raise_for_status()

    def update_query(self, query: str) -> None:
        """
        Function that passes a query via a post API call
        """
        auth = requests.auth.HTTPBasicAuth(self.service_cfg.user, self.service_cfg.passwd)
        response = requests.post(
            self.service_cfg.url + UPLOAD_END_POINT[self.service_cfg.rest_api],
            data={"update": query},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            auth=auth,
        )
        response.raise_for_status()

    @require_rdf4j
    def set_namespace(self, prefix: str, value: str) -> None:
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


@dataclass
class RepoInfo:
    uri: str
    repo_id: str
    title: str
    readable: bool
    writable: bool


async def repos(service_cfg: Optional[ServiceConfig] = None) -> List[RepoInfo]:
    """
    List available repositories
    """

    service_cfg = service_cfg or ServiceConfig()

    auth = None
    if service_cfg.user and service_cfg.passwd:
        auth = httpx.BasicAuth(service_cfg.user, service_cfg.passwd)

    url = f"{service_cfg.protocol}://{service_cfg.server}/repositories"

    async with httpx.AsyncClient() as client:
        response = await client.get(url, auth=auth, headers={"Accept": "application/json"})
    response.raise_for_status()

    def _repo_info(repo):
        uri = repo["uri"]["value"]
        repo_id = repo["id"]["value"]
        title = repo["title"]["value"]
        readable = repo["readable"]["value"] == "true"
        writable = repo["writable"]["value"] == "true"
        return RepoInfo(uri, repo_id, title, readable, writable)

    return [_repo_info(repo) for repo in response.json()["results"]["bindings"]]


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

    return GraphDBClient(ServiceConfig(repo, protocol, server))


def new_repo_blazegraph(url: str, repo: str, protocol: str = "https") -> GraphDBClient:
    template = confpath() / "blazegraph_repo_config.xml"
    config = config_bytes_from_template(template, {"repo": repo})

    response = requests.post(
        f"{protocol}://{url}", data=config, headers={"Content-type": "application/xml"}
    )
    response.raise_for_status()
    client = GraphDBClient(ServiceConfig(repo, protocol, url, rest_api=RestApi.BLAZEGRAPH))
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


class AsyncGraphDBClient(GraphDBClient):
    def __init__(self, service_cfg: Optional[ServiceConfig] = None) -> None:
        super().__init__(service_cfg)
        self.sparql = AsyncSparqlWrapper(self.service_cfg.url)
        self._init_sparql_wrapper()

    async def _exec_query(self, query: str) -> SparqlResult:
        self._prep_query(query)
        results = await self.sparql.queryAndConvert()
        return self._process_result(results)

    async def exec_query(self, query: str) -> List[Dict[str, str]]:
        out = await self._exec_query(query)
        return out["out"]

    async def get_table(self, query: str) -> Tuple[pd.DataFrame, Dict[str, str]]:
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
        res = await self._exec_query(query)
        return self._convert_query_result_to_df(res)


def make_async(client: GraphDBClient) -> AsyncGraphDBClient:
    """
    Convenience function that creates a new async graph db client from an existing client
    """
    return AsyncGraphDBClient(client.service_cfg)
