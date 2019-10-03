import pandas as pd

from SPARQLWrapper import SPARQLWrapper, JSON

from cimsparql import url
from cimsparql.model import CimModel
from typing import List, Dict


class GraphDBClient(CimModel):
    def __init__(self, service: str = None, infer: bool = False, sameas: bool = True):
        """
        :param service:
        """
        super().__init__()

        if service is None:
            service = url.service()

        self.get_prefix_dict(service)
        self.set_cim_version()

        self.sparql = SPARQLWrapper(service)
        self.sparql.setReturnFormat(JSON)
        self.sparql.addParameter("infer", str(infer))
        self.sparql.addParameter("sameAs", str(sameas))

    @staticmethod
    def value_getter(d):
        try:
            if d["type"] == "uri":
                return d["value"].split("_")[-1]
            else:
                return d["value"]
        except KeyError:
            pass

    def get_table(self, query: str, index: str = None, limit: int = None) -> pd.DataFrame:
        """
        Gets given table from the configured database.

        :param query: to sparql server
        :param index: column name to use as index
        :param limit: limit number of resulting rows
        :return: table as DataFrame
        """
        self.sparql.setQuery(self._query_str(query, limit))

        processed_results = self.sparql.queryAndConvert()

        cols = processed_results["head"]["vars"]
        data = processed_results["results"]["bindings"]

        out = [{c: self.value_getter(row.get(c, {})) for c in cols} for row in data]
        result = pd.DataFrame(out)

        if len(result) > 0 and index:
            result.set_index(index, inplace=True)

        return result
