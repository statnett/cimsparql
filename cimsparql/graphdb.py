import pandas as pd

from SPARQLWrapper import SPARQLWrapper, JSON

from cimsparql import url
from cimsparql.model import CimModel

pd.set_option("display.max_columns", None)


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

    def get_table(self, query: str, index: str = None, limit: int = None) -> pd.DataFrame:
        """
        Gets given table from the configured database.

        :param query: to sparql server
        :param infer: include inferred data
        :param sameas: expand results over owl:sameas
        :return: table as DataFrame
        """
        self.sparql.setQuery(self._query_str(query, limit))

        processed_results = self.sparql.queryAndConvert()

        cols = processed_results["head"]["vars"]

        out = []
        for row in processed_results["results"]["bindings"]:
            item = []
            for c in cols:
                item.append(row.get(c, {}).get("value"))
            out.append(item)

        result = pd.DataFrame(out, columns=cols)

        if len(result) > 0 and index:
            result.set_index(index, inplace=True)

        return result
