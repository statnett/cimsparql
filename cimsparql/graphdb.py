"""Graphdb CIM sparql client"""
import contextlib
import os
from typing import Dict, List, Tuple

import pandas as pd
import requests
from SPARQLWrapper import JSON, SPARQLWrapper

from cimsparql import url
from cimsparql.model import CimModel


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


class GraphDBClient(CimModel):
    def __init__(
        self, service: str, mapper: CimModel = None, infer: bool = False, sameas: bool = True
    ) -> None:
        """GraphDB client

        Args:
           service: string with url to graphdb repository. See cimsparql.url.service
           mapper: GraphDBClient with the mapper (Default to self).
           infer: deduce further knowledge based on existing RDF data and a formal set of
           sameas: map same concepts from two or more datasets
        """
        super().__init__(mapper, service, infer, sameas)

    def __str__(self) -> str:
        return f"<GraphDBClient object, service: {self.service}>"

    def _setup_client(self, service: str, infer: bool, sameas: bool, **kwargs) -> None:
        """Setup client for querying

        Args:
           service: string with url to graphdb repository. See cimsparql.url.service
           infer: deduce further knowledge based on existing RDF data and a formal set of
           sameas: map same concepts from two or more datasets
        """
        self.user = os.getenv("GRAPHDB_USER")
        self.passwd = os.getenv("GRAPHDB_USER_PASSWD")
        self.service = service
        self.sparql = SPARQLWrapper(self.service)
        self.sparql.setReturnFormat(JSON)
        self.sparql.setCredentials(self.user, self.passwd)
        self.sparql.addParameter("infer", str(infer))
        self.sparql.addParameter("sameAs", str(sameas))
        self._prefixes = None

    @property
    def service(self):
        return self._service

    @service.setter
    def service(self, service: str):
        if service is None:
            self._service = url.service(repo=os.getenv("GRAPHDB_REPO", "LATEST"))
        else:
            self._service = service

    @CimModel.prefixes.getter
    def prefixes(self):
        if self._prefixes is None:
            self.prefixes = self._service
        return self._prefixes

    @CimModel.prefixes.setter
    def prefixes(self, service: str):
        self._prefixes = {}
        auth = requests.auth.HTTPBasicAuth(self.user, self.passwd)
        response = requests.get(service + "/namespaces", auth=auth)
        if response.ok:
            for line in response.text.split():
                prefix, uri = line.split(",")
                if prefix != "prefix":
                    self._prefixes[prefix] = uri.rstrip("#")

    @staticmethod
    def value_getter(d: Dict[str, str]) -> str:
        """Get item of 'value' key if present else None"""
        with contextlib.suppress(KeyError):
            return d["value"]

    @staticmethod
    def _col_map(data_row) -> Dict[str, str]:
        return {
            column: data.get("datatype", data.get("type", None))
            for column, data in data_row.items()
            if data.get("datatype", data.get("type", None)) != "literal"
        }

    def _get_table(self, query: str, limit: int) -> Tuple[pd.DataFrame, Dict]:
        self.sparql.setQuery(self._query_with_header(query, limit))

        processed_results = self.sparql.queryAndConvert()

        cols = processed_results["head"]["vars"]
        data = processed_results["results"]["bindings"]
        out = [{c: self.value_getter(row.get(c, {})) for c in cols} for row in data]

        return pd.DataFrame(out), data_row(cols, data)


def get_graphdb_client(
    server: str,
    graphdb_repo: str,
    graphdb_path: str = "services/pgm/equipment/",
    protocol: str = "https",
) -> GraphDBClient:
    """Get GraphDB client

    Args:
        server: graphdb server
        graphdb_repo: query this repo
        graphdb_path: Prepend with this path when graphdb_repo is LATEST
        protocol: https or http
    """
    graphdb_path = graphdb_path if graphdb_repo == "LATEST" else ""
    return GraphDBClient(url.service(graphdb_repo, server, protocol, graphdb_path))
