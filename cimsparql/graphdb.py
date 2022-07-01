"""Graphdb CIM sparql client"""
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from deprecated import deprecated
from SPARQLWrapper import JSON, SPARQLWrapper

from cimsparql import url
from cimsparql.url import Prefix


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

    def _get_table(self, query: str, limit: Optional[int]) -> Tuple[pd.DataFrame, Dict[str, str]]:
        out, data_row = self._exec_query(query, limit)
        return pd.DataFrame(out), data_row

    @property
    def empty(self) -> bool:
        """Identify empty GraphDB repo"""
        try:
            self._get_table("SELECT * \n WHERE { ?s ?p ?o }", limit=1)
            return False
        except IndexError:
            return True

    def get_table(
        self,
        query: str,
        index: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Gets given table from the configured database.

        Args:
           query: to sparql server
           index: column name to use as index
           limit: limit number of resulting rows
           map_data_types: gets datatypes from the configured graphdb & maps the types in the result
                  to correct python types
           custom_maps: dictionary of 'sparql_datatype': function to apply on columns with that
                  type. Overwrites sparql map for the types specified.
           columns: dictionary of 'column_name': function,
                  uses pandas astype on the column, or applies function.
                  Sparql map overwrites columns when available

        Example:
           >>> from cimsparql.graphdb import GraphDBClient
           >>> from cimsparql.url import service
           >>> gdbc = GraphDBClient(service('LATEST'))
           >>> query = 'select * where { ?subject ?predicate ?object }'
           >>> gdbc.get_table(query, limit=10)
        """
        try:
            result, data_row = self._get_table(query, limit)
        except IndexError:
            return pd.DataFrame([]), {}
        return result, data_row

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


@deprecated(version="1.11", reason="Use cimsparqel.model.get_cim_model instead")
def get_graphdb_client(
    server: str,
    graphdb_repo: str,
    graphdb_path: str = "services/pgm/equipment/",
    protocol: str = "https",
):
    from cimsparql.model import get_cim_model

    return get_cim_model(server, graphdb_repo, graphdb_path, protocol)
