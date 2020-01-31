import os
from typing import Dict, Tuple

import pandas as pd
import requests
from SPARQLWrapper import JSON, SPARQLWrapper

from cimsparql import url
from cimsparql.model import CimModel


class GraphDBClient(CimModel):
    def __init__(
        self, service: str, mapper: CimModel = None, infer: bool = False, sameas: bool = True,
    ):
        """GraphDB client

        Args:
           service: string with url to graphdb repository. See cimsparql.url.service
           mapper: GraphDBClient with the mapper (Default to self).
           infer: deduce further knowledge based on existing RDF data and a formal set of
           sameas: map same concepts from two or more datasets
        """
        super().__init__(
            service=service, mapper=mapper, infer=infer, sameas=sameas,
        )

    def _load_from_source(self, service: str, infer: bool, sameas: bool, **kwargs):
        if service is None:
            self._service = url.service(
                repo=os.getenv("GRAPHDB_REPO", "SNMST-MasterCim15-VERSION-LATEST")
            )
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
        response = requests.get(service + f"/namespaces")
        if response.ok:
            for line in response.text.split():
                prefix, uri = line.split(",")
                if prefix != "prefix":
                    self._prefixes[prefix] = uri.rstrip("#")

    @staticmethod
    def value_getter(d) -> str:
        try:
            return d["value"]
        except KeyError:
            pass

    @staticmethod
    def _col_map(data_row) -> Dict[str, str]:
        return {
            column: data.get("datatype", data.get("type", None))
            for column, data in data_row.items()
        }

    def _get_table(self, query: str, limit: int, **kwargs) -> Tuple[pd.DataFrame, Dict]:
        self.sparql.setQuery(self._query_str(query, limit))

        processed_results = self.sparql.queryAndConvert()

        cols = processed_results["head"]["vars"]
        data = processed_results["results"]["bindings"]
        out = [{c: self.value_getter(row.get(c, {})) for c in cols} for row in data]

        return pd.DataFrame(out), data[0]
