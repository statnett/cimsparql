"""Graphdb CIM sparql client"""
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from deprecated import deprecated
from SPARQLWrapper import JSON, SPARQLWrapper

from cimsparql import url
from cimsparql.url import Prefix, service


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


@dataclass
class ServiceConfig:
    url: str
    user: str = field(default=os.getenv("GRAPHDB_USER"))
    passwd: str = field(default=os.getenv("GRAPHDB_USER_PASSWD"))


class GraphDBClient:
    def __init__(
        self,
        service: str,
        infer: bool = False,
        sameas: bool = True,
    ) -> None:
        """GraphDB client

        Args:
           service: string with url to graphdb repository. See cimsparql.url.service
           mapper: GraphDBClient with the mapper (Default to self).
           infer: deduce further knowledge based on existing RDF data and a formal set of
           sameas: map same concepts from two or more datasets
        """
        service = service or url.service(repo=os.getenv("GRAPHDB_REPO", "LATEST"))
        self.service_cfg = ServiceConfig(url=service)
        self.sparql = SPARQLWrapper(self.service_cfg.url)
        self.sparql.setReturnFormat(JSON)
        self.sparql.setCredentials(self.service_cfg.user, self.service_cfg.passwd)
        self.sparql.addParameter("infer", str(infer))
        self.sparql.addParameter("sameAs", str(sameas))

        self._prefixes = None

    @property
    def prefixes(self) -> Prefix:
        if self._prefixes is None:
            pref = self.get_prefixes()
            self._prefixes = Prefix(pref)
        return self._prefixes

    def __str__(self) -> str:
        return f"<GraphDBClient object, service: {self.service_cfg.url}>"

    def query_with_header(self, query: str, limit: Optional[int] = None) -> str:
        query = "\n".join([self.prefixes.header_str(query), query])
        if limit is not None:
            query += f" limit {limit}"
        return query

    @staticmethod
    def value_getter(d: Dict[str, str]) -> str:
        """Get item of 'value' key if present else None"""
        return d.get("value")

    def _exec_query(self, query: str, limit: Optional[int]):
        self.sparql.setQuery(self.query_with_header(query, limit))

        processed_results = self.sparql.queryAndConvert()

        cols = processed_results["head"]["vars"]
        data = processed_results["results"]["bindings"]
        out = [{c: self.value_getter(row.get(c, {})) for c in cols} for row in data]
        return out, data_row(cols, data)

    def exec_query(self, query: str, limit: Optional[int] = None) -> List[Dict[str, str]]:
        out, _ = self._exec_query(query, limit)
        return out

    def get_table(
        self, query: str, limit: Optional[int] = None
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
        out, data_row = self._exec_query(query, limit)
        return pd.DataFrame(out), data_row

    @property
    def empty(self) -> bool:
        """Identify empty GraphDB repo"""
        try:
            self.get_table("SELECT * \n WHERE { ?s ?p ?o }", limit=1)
            return False
        except IndexError:
            return True

    def get_prefixes(self) -> Dict[str, str]:
        prefixes = {}
        auth = requests.auth.HTTPBasicAuth(self.service_cfg.user, self.service_cfg.passwd)
        response = requests.get(self.service_cfg.url + "/namespaces", auth=auth)
        if response.ok:
            # Extract prefixes (skip line 1 which is a header)
            for line in response.text.split()[1:]:
                prefix, uri = line.split(",")
                prefixes[prefix] = uri
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

    def delete_repo(self):
        response = requests.delete(self.service_cfg.url)
        response.raise_for_status()

    def upload_rdf_xml(self, fname: Path):
        with open(fname, "rb") as infile:
            xml_content = infile.read()

        response = requests.post(
            self.service_cfg.url + "/statements",
            data=xml_content,
            headers={"Content-Type": "application/rdf+xml"},
        )
        response.raise_for_status()

    def upload_ttl(self, fname: Path):
        with open(fname, "rb") as infile:
            turtle_content = infile.read()

        response = requests.post(
            self.service_cfg.url + "/statements",
            data=turtle_content,
            headers={"Content-Type": "text/turtle"},
        )
        response.raise_for_status()


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
