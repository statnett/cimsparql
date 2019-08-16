import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON

pd.set_option("display.max_columns", None)


class GraphDBClient(object):
    def __init__(self, service: str):
        """
        :param service:
        """
        self.sparql = SPARQLWrapper(service)

    def get_table(self, query: str, infer: bool = False, sameAs: bool = True) -> pd.DataFrame:
        """
        Gets given table from the configured database.

        :param query: to sparql server
        :param infer: include inferred data
        :param sameAs: expand results over owl:sameAs
        :return: table as DataFrame
        """
        self.sparql.setQuery(query)
        self.sparql.setReturnFormat(JSON)
        self.sparql.addParameter("infer", str(infer))
        self.sparql.addParameter("sameAs", str(sameAs))

        processed_results = self.sparql.queryAndConvert()

        cols = processed_results["head"]["vars"]

        out = []
        for row in processed_results["results"]["bindings"]:
            item = []
            for c in cols:
                item.append(row.get(c, {}).get("value"))
            out.append(item)

        return pd.DataFrame(out, columns=cols)
