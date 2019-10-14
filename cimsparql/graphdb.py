import pandas as pd
from typing import Dict
from SPARQLWrapper import SPARQLWrapper, JSON

from cimsparql import url
from cimsparql.model import CimModel
from cimsparql.type_mapper import TypeMapper


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

        self.mapper = TypeMapper(self)

    @staticmethod
    def value_getter(d):
        try:
            return d["value"]
        except KeyError:
            pass

    def get_table(
        self,
        query: str,
        index: str = None,
        limit: int = None,
        map_data_types: bool = True,
        custom_maps: dict = None,
        columns: dict = None,
    ) -> pd.DataFrame:
        """
        Gets given table from the configured database.

        :param query: to sparql server
        :param index: column name to use as index
        :param limit: limit number of resulting rows
        :param map_data_types: gets datatypes from the configured graphdb & maps the
                               types in the result to correct python types
        :param custom_maps: dictionary of 'sparql_datatype': function
                            to apply on columns with that type.
                            Overwrites sparql map for the types specified.
        :param columns: dictionary of 'column_name': function,
                        uses pandas astype on the column, or applies function.
                        Overwrites sparql map for the columns specified
        :return: table as DataFrame
        """
        self.sparql.setQuery(self._query_str(query, limit))

        processed_results = self.sparql.queryAndConvert()

        cols = processed_results["head"]["vars"]
        data = processed_results["results"]["bindings"]

        out = [{c: self.value_getter(row.get(c, {})) for c in cols} for row in data]
        result = pd.DataFrame(out)
        if map_data_types and len(result) > 0:
            result = self.mapper.map_data_types(result, data[0], custom_maps, columns)

        if len(result) > 0 and index:
            result.set_index(index, inplace=True)
        return result

    def get_table_and_convert(
        self, query: str, index: str = None, limit: int = None, columns: Dict = None
    ) -> pd.DataFrame:
        result = self.get_table(query, index=index, limit=limit, columns=columns)
        return result
