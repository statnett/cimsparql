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
        super().__init__(service=service, mapper=mapper, infer=infer, sameas=sameas)

    def __str__(self) -> str:
        return f"<GraphDBClient object, service: {self._service}>"

    def _setup_client(self, service: str, infer: bool, sameas: bool, **kwargs) -> None:
        """Setup client for querying

        Args:
           service: string with url to graphdb repository. See cimsparql.url.service
           infer: deduce further knowledge based on existing RDF data and a formal set of
           sameas: map same concepts from two or more datasets
        """
        if service is None:
            self._service = url.service(repo=os.getenv("GRAPHDB_REPO", "LATEST"))
        else:
            self._service = service

        self.sparql = SPARQLWrapper(self._service)
        self.sparql.setReturnFormat(JSON)
        self.sparql.addParameter("infer", str(infer))
        self.sparql.addParameter("sameAs", str(sameas))
        self.prefixes = self._service

    @CimModel.prefixes.setter
    def prefixes(self, service: str):
        self._prefixes = {}
        response = requests.get(service + "/namespaces")
        if response.ok:
            for line in response.text.split():
                prefix, uri = line.split(",")
                if prefix != "prefix":
                    self._prefixes[prefix] = uri.rstrip("#")

    @staticmethod
    def value_getter(d: Dict[str, str]) -> str:
        """Get item of 'value' key if present else None"""
        try:
            return d["value"]
        except KeyError:
            pass

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
