"""Graphdb CIM sparql client."""

from __future__ import annotations

import json
import os
from copy import deepcopy
from dataclasses import dataclass, field
from enum import StrEnum
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict, TypeVar

import httpx
import pandas as pd
import tenacity
from SPARQLWrapper import JSON, POST, SPARQLWrapper

from cimsparql.retry_cb import RetryCallback, RetryCallbackFactory
from cimsparql.sparql_result_json import SparqlData, SparqlResultHead, SparqlResultJson, SparqlResultValue
from cimsparql.url import service, service_blazegraph

if TYPE_CHECKING:
    from collections.abc import Callable

    from tenacity.stop import stop_base

    from cimsparql.sparql_result_json import SparqlResultValue


class SparqlResult(TypedDict):
    cols: list[str]
    data: list[dict[str, SparqlResultValue]]


def data_row(cols: list[str], rows: list[dict[str, SparqlResultValue]]) -> dict[str, SparqlResultValue]:
    """Get a sample row for extraction of data types.

    Args:
       cols: queried columns (optional might return None)
       rows: 'results'→'bindings' from SPARQLWrapper

    Returns:
       samples result of all columns in query
    """
    full_row: dict[str, SparqlResultValue] = {}
    for row in rows:
        if set(cols).difference(full_row.keys()):
            full_row.update(row)
        else:
            break
    return full_row


class RestApi(StrEnum):
    RDF4J = "RDF4J"
    BLAZEGRAPH = "BLAZEGRAPH"
    DIRECT_SPARQL_ENDPOINT = "DIRECT_SPARQL_ENDPOINT"


def parse_namespaces_rdf4j(response: httpx.Response) -> dict[str, str]:
    return dict(line.split(",") for line in response.text.split()[1:])


@dataclass(frozen=True)
class ServiceConfig:
    repo: str = field(default=os.getenv("GRAPHDB_REPO", "LATEST"))
    protocol: str = "https"
    server: str = field(default=os.getenv("GRAPHDB_SERVER", "127.0.0.1:7200"))
    path: str = ""
    user: str | None = field(default=os.getenv("GRAPHDB_USER"))
    passwd: str | None = field(default=os.getenv("GRAPHDB_USER_PASSWD"))
    token: str | None = field(default=os.getenv("GRAPHDB_TOKEN"))
    rest_api: RestApi = field(default=RestApi(os.getenv("SPARQL_REST_API", "RDF4J")))
    ca_bundle: str | None = field(default=None)
    retry_callback_factory: RetryCallbackFactory = field(default=RetryCallback)
    retry_stop_criteria: stop_base = field(default=tenacity.stop_after_attempt(1))

    # Parameters for rest api
    # https://rdf4j.org/documentation/reference/rest-api/
    distinct: bool = False
    infer: bool = False
    limit: int | None = None
    offset: int | None = None
    timeout: int | None = None
    max_delay_seconds: int = 60
    validate: bool = False

    def __post_init__(self) -> None:
        if self.rest_api not in RestApi:
            raise ValueError(f"rest_api must be one of {RestApi}")

    @property
    def url(self) -> str:
        if self.rest_api == RestApi.BLAZEGRAPH:
            return service_blazegraph(self.server, self.repo, self.protocol)
        if self.rest_api == RestApi.DIRECT_SPARQL_ENDPOINT:
            return self.server
        return service(self.repo, self.server, self.protocol, self.path)

    @property
    def parameters(self) -> dict[str, bool | int | None]:
        return {
            "distinct": self.distinct,
            "infer": self.infer,
            "limit": self.limit,
            "offset": self.offset,
        }

    @property
    def auth(self) -> httpx.BasicAuth | None:
        return httpx.BasicAuth(self.user, self.passwd) if self.user and self.passwd and not self.token else None


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

if TYPE_CHECKING:
    from typing import Concatenate, ParamSpec

    P = ParamSpec("P")
    T = TypeVar("T")


def require_rdf4j(
    f: Callable[Concatenate[GraphDBClient, P], T],
) -> Callable[Concatenate[GraphDBClient, P], T]:
    def wrapper(cli: GraphDBClient, *args: P.args, **kwargs: P.kwargs) -> T:
        if cli.service_cfg.rest_api != RestApi.RDF4J:
            raise NotImplementedError("Function only implemented for RDF4J")
        return f(cli, *args, **kwargs)

    return wrapper


class GraphDBClient:
    """GraphDB client for sending sparql queries to GraphDB server.

    Args:
        service_cfg: Service configuration (see ServiceConfig)
        custom_headers: Added to SPARQLWrapper using addCustomHttpHeader
        sparql_wrapper: SPARQLWrapper instance used to post queries. If not given,
        a SPARQLWrapper create posting queries to the URL given in service_cfg

    Example:
    >>> from cimsparql.graphdb import GraphDBClient
    >>> gdbc = GraphDBClient()
    >>> query = 'select * where { ?subject ?predicate ?object } limit 10'
    >>> df, row = gdbc.get_table(query)xs

    Where row is the output of graphdb.data_row
    """

    x_correlation_id = "x-correlation-id"

    def __init__(
        self,
        service_cfg: ServiceConfig | None = None,
        custom_headers: dict[str, str] | None = None,
        sparql_wrapper: SPARQLWrapper | None = None,
    ) -> None:
        self.service_cfg = service_cfg or ServiceConfig()
        self.sparql = sparql_wrapper or SPARQLWrapper(self.service_cfg.url)
        self.sparql.setReturnFormat(JSON)
        self.sparql.setMethod(POST)
        if self.service_cfg.token:
            self.sparql.addCustomHttpHeader("Authorization", self.service_cfg.token)
        else:
            self.sparql.setCredentials(self.service_cfg.user, self.service_cfg.passwd)
        if self.service_cfg.timeout:
            self.sparql.setTimeout(self.service_cfg.timeout)
        self.update_sparql_parameters()
        if custom_headers:
            for name, value in custom_headers.items():
                self.sparql.addCustomHttpHeader(name, value)
        self._prefixes = None

    def add_correlation_id_to_header(self, correlation_id: str) -> None:
        self.sparql.addCustomHttpHeader(self.x_correlation_id, correlation_id)

    def clear_correlation_id_from_header(self) -> None:
        self.sparql.clearCustomHttpHeader(self.x_correlation_id)

    def create_sparql_wrapper(self) -> SPARQLWrapper:
        return SPARQLWrapper(self.service_cfg.url)

    def update_sparql_parameters(self) -> None:
        for key, value in self.service_cfg.parameters.items():
            if value is not None:
                self.set_parameter(key, str(value))

    def set_parameter(self, key: str, value: str) -> None:
        self.sparql.clearParameter(key)
        self.sparql.addParameter(key, value)

    @property
    def prefixes(self) -> dict[str, str]:
        if self._prefixes is None:
            self._prefixes = self.get_prefixes()
        return self._prefixes

    def update_prefixes(self, pref: dict[str, str]) -> None:
        """Update prefixes from a dict."""
        self.prefixes.update(pref)

    def __str__(self) -> str:
        return f"<GraphDBClient object, service: {self.service_cfg.url}>"

    def exec_query(self, query: str) -> SparqlResultJson:
        # To allow exec query to be run in threads, we use a deepcopy of the underlying
        # sparql wrapper .This is needed since setQuery changes the state of the SPARQLWrapper
        sparql_wrapper = deepcopy(self.sparql)
        sparql_wrapper.setQuery(query)

        retry_cb = self.service_cfg.retry_callback_factory()
        retry_cb.pre_call(query)

        sparql_result = None
        for attempt in tenacity.Retrying(
            stop=self.service_cfg.retry_stop_criteria,
            wait=tenacity.wait_exponential(max=self.service_cfg.max_delay_seconds),
            before=retry_cb.before,
            after=retry_cb.after,
        ):
            with attempt:
                results = sparql_wrapper.queryAndConvert()

                sparql_result = SparqlResultJson.model_validate(results)
                if self.service_cfg.validate:
                    sparql_result.validate_column_consistency()
        return sparql_result or SparqlResultJson(head=SparqlResultHead(), results=SparqlData(bindings=[]))

    def _convert_query_result_to_df(
        self, sparql_result: SparqlResultJson
    ) -> tuple[pd.DataFrame, dict[str, SparqlResultValue]]:
        df = (
            pd.DataFrame(sparql_result.results.values_as_dict(), columns=sparql_result.head.variables)
            if len(sparql_result.results.bindings)
            else pd.DataFrame(columns=sparql_result.head.variables)
        )
        return df, data_row(sparql_result.head.variables, sparql_result.results.bindings)

    def get_table(self, query: str) -> tuple[pd.DataFrame, dict[str, SparqlResultValue]]:
        """Get result from sparql query as a pandas dataframe.

        Args:
           query: to sparql server
           limit: limit number of resulting rows

        """
        return self._convert_query_result_to_df(self.exec_query(query))

    @property
    def empty(self) -> bool:
        """Identify empty GraphDB repo."""
        return self.get_table("select * where {?s ?p ?o} limit 1")[0].empty

    def get_prefixes(self, http_transport: httpx.BaseTransport | None = None) -> dict[str, str]:
        prefixes = default_namespaces()

        if self.service_cfg.rest_api in (RestApi.BLAZEGRAPH, RestApi.DIRECT_SPARQL_ENDPOINT):
            # These APis does not expose prefixes. Custom prefixes must be added
            # via `update_prefixes`. By default we load a pre-defined set of prefixes
            return prefixes

        with httpx.Client(transport=http_transport, timeout=5.0) as client:
            response = client.get(
                self.service_cfg.url + "/namespaces",
                auth=self.service_cfg.auth or httpx.USE_CLIENT_DEFAULT,
                headers=self.sparql.customHttpHeaders,
            )
        if response.status_code == HTTPStatus.OK:
            prefixes.update(parse_namespaces_rdf4j(response))
            return prefixes
        msg = (
            "Could not fetch namespaces and prefixes from graphdb "
            "Verify that user and password are correctly set in the "
            "GRAPHDB_USER and GRAPHDB_USER_PASSWD environment variable"
        )
        raise RuntimeError(f"{msg} Status code: {response.status_code} Reason: {response.reason_phrase}")

    def delete_repo(self) -> None:
        endpoint = delete_repo_endpoint(self.service_cfg)
        response = httpx.delete(endpoint, timeout=5.0)
        response.raise_for_status()

    def upload_rdf(self, content: Path | bytes, rdf_format: str, params: dict[str, str] | None = None) -> None:
        """Upload data in RDF format to a service.

        Args:
            content: Filename to the RDF file to upload
            rdf_format: RDF file type (e.g. rdf/xml, rdf/json etc. Consult the RDF4J rest api
                doc for a complete list of available options)
            params: Additional parameters passed to the post request
        """

        def read_xml_content(file: Path) -> bytes:
            with file.open("rb") as infile:
                return infile.read()

        xml_content = read_xml_content(content) if isinstance(content, Path) else content

        response = httpx.post(
            self.service_cfg.url + UPLOAD_END_POINT[self.service_cfg.rest_api],
            content=xml_content,
            params=params,
            headers={"Content-Type": MIME_TYPE_RDF_FORMATS[rdf_format]},
            timeout=5.0,
        )
        response.raise_for_status()

    def update_query(self, query: str) -> None:
        """Pass a query via a post API call."""
        response = httpx.post(
            self.service_cfg.url + UPLOAD_END_POINT[self.service_cfg.rest_api],
            data={"update": query},
            headers=self.sparql.customHttpHeaders | {"Content-Type": "application/x-www-form-urlencoded"},
            auth=self.service_cfg.auth,
            timeout=5.0,
        )
        response.raise_for_status()

    @require_rdf4j
    def set_namespace(self, prefix: str, value: str) -> None:
        response = httpx.put(
            self.service_cfg.url + f"/namespaces/{prefix}",
            content=value,
            headers={"Content-Type": "text/plain"},
            auth=self.service_cfg.auth,
            timeout=5.0,
        )
        response.raise_for_status()

    @require_rdf4j
    def get_namespace(self, prefix: str) -> str:
        response = httpx.get(self.service_cfg.url + f"/namespaces/{prefix}", auth=self.service_cfg.auth, timeout=5.0)

        response.raise_for_status()
        return response.text


@dataclass
class RepoInfo:
    uri: str
    repo_id: str
    title: str
    readable: bool
    writable: bool


def repos(service_cfg: ServiceConfig | None = None) -> list[RepoInfo]:
    """List available repositories."""
    service_cfg = service_cfg or ServiceConfig()

    auth = httpx.USE_CLIENT_DEFAULT
    if service_cfg.user and service_cfg.passwd:
        auth = httpx.BasicAuth(service_cfg.user, service_cfg.passwd)

    url = f"{service_cfg.protocol}://{service_cfg.server}/repositories"

    with httpx.Client(timeout=5.0) as client:
        response = client.get(url, auth=auth, headers={"Accept": "application/json"})
    response.raise_for_status()

    def _repo_info(repo: dict[str, dict[str, str]]) -> RepoInfo:
        def get(key: str, default: str = "") -> str:
            return repo.get(key, {}).get("value", default)

        uri = get("uri")
        repo_id = get("id")
        title = get("title")
        readable = get("readable", "false") == "true"
        writable = get("writable", "false") == "true"
        return RepoInfo(uri, repo_id, title, readable, writable)

    return [_repo_info(repo) for repo in response.json()["results"]["bindings"]]


def new_repo(service_config: ServiceConfig, config: bytes, allow_exist: bool = True) -> GraphDBClient:
    ignored_errors = {HTTPStatus.CONFLICT} if allow_exist else set[HTTPStatus]()
    url = service_config.url
    response = httpx.put(
        url, content=config, headers={"Content-Type": "text/turtle"}, timeout=service_config.timeout or 5.0
    )
    if response.status_code not in ignored_errors:
        response.raise_for_status()

    return GraphDBClient(service_cfg=service_config)


def new_repo_blazegraph(url: str, repo: str, protocol: str = "https", token: str | None = None) -> GraphDBClient:
    template = confpath() / "blazegraph_repo_config.xml"
    config = config_bytes_from_template(template, {"repo": repo})

    response = httpx.post(
        f"{protocol}://{url}",
        content=config,
        headers={"Content-type": "application/xml"},
        timeout=5.0,
    )
    response.raise_for_status()
    return GraphDBClient(ServiceConfig(repo, protocol, url, rest_api=RestApi.BLAZEGRAPH, token=token))


def config_bytes_from_template(template: Path, params: dict[str, str], encoding: str = "utf-8") -> bytes:
    """Replace value in template file with items in params.

    Args:
        template: Path to template file
        params: Dict with key-value pairs where key must be enclode by double curly braces
            in the template file ({{}}). {{key}} will be replaced by value
        encoding: use this encoding when replacing items
    """
    with template.open("rb") as infile:
        data = infile.read()

    for param, value in params.items():
        enc_templ = f"{{{{{param}}}}}".encode(encoding)
        enc_value = value.encode(encoding)
        data = data.replace(enc_templ, enc_value)
    return data


def confpath() -> Path:
    return Path(__file__).parent.parent / "pkg_data"


def default_namespaces() -> dict[str, str]:
    with (confpath() / "namespaces.json").open() as infile:
        return json.load(infile)


def delete_repo_endpoint(config: ServiceConfig) -> str:
    if config.rest_api == RestApi.BLAZEGRAPH:
        # Remove /sparql at the end
        return config.url.rpartition("/")[0]
    return config.url
