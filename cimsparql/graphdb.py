import requests
import pandas as pd

from SPARQLWrapper import SPARQLWrapper, JSON
from typing import Tuple, Dict

from cimsparql import url
from cimsparql.model import CimModel


class GraphDBClient(CimModel):
    def __init__(
        self, service: str = None, mapper: CimModel = None, infer: bool = False, sameas: bool = True
    ):
        """
        :param service:
        """
        super().__init__(service=service, mapper=mapper, infer=infer, sameas=sameas)

    def _load_from_source(self, service: str, infer: bool, sameas: bool, **kwargs):
        if service is None:
            self._service = url.service()
        else:
            self._service = service

        self.sparql = SPARQLWrapper(self._service)
        self.sparql.setReturnFormat(JSON)
        self.sparql.addParameter("infer", str(infer))
        self.sparql.addParameter("sameAs", str(sameas))

    def get_prefix_dict(self, *args, **kwargs):
        self.prefix_dict = {}
        response = requests.get(self._service + f"/namespaces")
        if response.ok:
            for line in response.text.split():
                prefix, uri = line.split(",")
                if prefix != "prefix":
                    self.prefix_dict[prefix] = uri.rstrip("#")

    @staticmethod
    def value_getter(d):
        try:
            return d["value"]
        except KeyError:
            pass

    @staticmethod
    def _col_map(data_row) -> Dict:
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
