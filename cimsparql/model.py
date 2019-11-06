import pandas as pd

from cimsparql import queries, ssh_queries, tp_queries, sv_queries
from cimsparql.url import Prefix
from cimsparql.type_mapper import TypeMapper


from typing import Dict, List, TypeVar, Tuple

CimModelType = TypeVar("CimModelType", bound="CimModel")


class CimModel(Prefix):
    def __init__(self, mapper: TypeMapper, network_analysis: bool, *args, **kwargs):
        self._network_analysis = network_analysis
        self._load_from_source(*args, **kwargs)
        self.get_prefix_dict(*args, **kwargs)
        self._set_mapper(mapper)
        self.set_cim_version()

    def _set_mapper(self, mapper: TypeMapper = None):
        if mapper is None and "rdfs" in self.prefix_dict:
            self.mapper = TypeMapper(self)
        else:
            self.mapper = mapper

    def _query_str(self, query: str, limit: int = None) -> str:
        q = f"{self.header_str()}\n\n{query}"
        if limit is not None:
            q += f" limit {limit}"
        return q

    def bus_data(self, region: str = "NO", limit: int = None) -> pd.DataFrame:
        query = queries.bus_data(region)
        return self.get_table_and_convert(query, index="mrid", limit=limit)

    def loads(
        self,
        load_type: List[str],
        load_vars: Tuple[str] = ("p", "q"),
        region: str = "NO",
        limit: int = None,
        connectivity: str = None,
    ) -> pd.DataFrame:
        query = queries.load_query(
            load_type, load_vars, region, connectivity, self._network_analysis
        )
        columns = {var: float for var in load_vars}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def wind_generating_units(self, limit: int = None) -> pd.DataFrame:
        query = queries.wind_generating_unit_query(self._network_analysis)
        float_list = ["maxP", "allocationMax", "allocationWeight", "minP"]
        columns = {var: float for var in float_list}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def synchronous_machines(
        self,
        synchronous_vars: Tuple[str] = ("sn", "p", "q"),
        region: str = "NO",
        limit: int = None,
        connectivity: str = None,
    ) -> pd.DataFrame:
        query = queries.synchronous_machines_query(synchronous_vars, region, connectivity)
        float_list = ["maxP", "minP", "allocationMax", "allocationWeight"]
        float_list += synchronous_vars
        columns = {var: float for var in float_list}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def connections(
        self,
        rdf_types: Tuple[str] = ("cim:Breaker", "cim:Disconnector"),
        region: str = "NO",
        limit: int = None,
        connectivity: bool = True,
    ) -> pd.DataFrame:
        query = queries.connection_query(self._cim_version, rdf_types, region, connectivity)
        return self.get_table_and_convert(query, index="mrid", limit=limit)

    def ac_lines(
        self,
        region: str = "NO",
        limit: int = None,
        connectivity: str = None,
        rates: Tuple[str] = queries.ratings,
    ) -> pd.DataFrame:
        query = queries.ac_line_query(self._cim_version, region, connectivity, rates)
        float_list = ["x", "r", "un", "bch", "length"] + [f"rate{rate}" for rate in rates]
        columns = {var: float for var in float_list}
        return self.get_table_and_convert(query, limit=limit, columns=columns)

    def series_compensators(self, region: str = "NO", limit: int = None, connectivity: str = None):
        query = queries.series_compensator_query(self._cim_version, region, connectivity)
        result, data_row = self._get_table(query=query, limit=limit)
        return self.get_table_and_convert(query, limit=limit)

    def transformers(
        self,
        region: str = "NO",
        limit: int = None,
        connectivity: str = None,
        rates: Tuple[str] = queries.ratings,
    ) -> pd.DataFrame:
        query = queries.transformer_query(region, connectivity, rates)
        columns = {var: float for var in ["x", "un"] + [f"rate{rate}" for rate in rates]}
        columns["endNumber"] = int
        return self.get_table_and_convert(query, limit=limit, columns=columns)

    def disconnected(self, index: str = None, limit: int = None) -> pd.DataFrame:
        query = ssh_queries.disconnected(self._cim_version)
        return self.get_table(query, index=index, limit=limit)

    def ssh_synchronous_machines(self, limit: int = None) -> pd.DataFrame:
        query = ssh_queries.synchronous_machines()
        columns = {"p": float, "q": float, "controlEnabled": bool}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def ssh_load(self, rdf_types: List[str] = None, limit: int = None) -> pd.DataFrame:
        if rdf_types is None:
            rdf_types = ["cim:ConformLoad", "cim:NonConformLoad"]
        query = ssh_queries.load(rdf_types)
        columns = {"p": float, "q": float}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def ssh_generating_unit(self, rdf_types: List[str] = None, limit: int = None) -> pd.DataFrame:
        if rdf_types is None:
            rdf_types = [f"cim:{unit}GeneratingUnit" for unit in ["Hydro", "Thermal", "Wind"]]
        query = ssh_queries.generating_unit(rdf_types)
        columns = {"normalPF": float}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def terminal(self, limit: int = None) -> pd.DataFrame:
        query = tp_queries.terminal(self._cim_version)
        columns = {"connected": bool}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def topological_node(self, limit: int = None) -> pd.DataFrame:
        query = tp_queries.topological_node()
        columns = {"BaseVoltage": float}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def powerflow(self, power: Tuple[str] = ("p", "q"), limit: int = None) -> pd.DataFrame:
        query = sv_queries.powerflow(power)
        columns = {x: float for x in power}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def voltage(self, voltage_vars: Tuple[str] = ("v", "angle"), limit: int = None) -> pd.DataFrame:
        query = sv_queries.voltage(voltage_vars)
        columns = {x: float for x in voltage_vars}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def tapstep(self, limit: int = None) -> pd.DataFrame:
        query = sv_queries.tapstep()
        columns = {"position": float}
        return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    @property
    def empty(self) -> bool:
        try:
            table, _ = self._get_table("SELECT * \n WHERE { ?s ?p ?o } limit 1")
        except IndexError:
            return True
        return False

    @staticmethod
    def _assign_column_types(result, columns):
        for column, column_type in columns.items():
            if column_type is str:
                continue
            elif column_type is bool:
                result.loc[:, column] = result.loc[:, column].str.contains("True|true")
            else:
                result.loc[result[column] == "None", column] = ""
                try:
                    result.loc[:, column] = pd.to_numeric(result[column]).astype(column_type)
                except ValueError:
                    raise

    @classmethod
    def col_map(cls, data_row, columns) -> Tuple[Dict[str, str]]:
        columns = {} if columns is None else columns
        col_map = cls._col_map(data_row)
        return col_map, {col: columns[col] for col in set(columns).difference(col_map)}

    def get_table(
        self,
        query: str,
        index: str = None,
        limit: int = None,
        map_data_types: bool = True,
        custom_maps: Dict = None,
        columns: Dict = None,
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
                        Sparql map overwrites columns when available
        :return: table as DataFrame
        """
        try:
            result, data_row = self._get_table(query=query, limit=limit)
        except IndexError:
            return pd.DataFrame([])

        if map_data_types and self.mapper is not None:
            col_map, columns = self.col_map(data_row, columns)
            result = self.mapper.map_data_types(result, col_map, custom_maps, columns)

        if index:
            result.set_index(index, inplace=True)
        return result

    @property
    def map_data_types(self) -> bool:
        try:
            return self.mapper.have_cim_version(self.prefix_dict["cim"])
        except AttributeError:
            return False

    @classmethod
    def manual_convert_types(
        cls: CimModelType, df: pd.DataFrame, columns: Dict, index: str
    ) -> pd.DataFrame:
        if columns is None:
            columns = {}
        reset_index = index is not None
        if reset_index:
            df.reset_index(inplace=True)
        df = df.astype(str)
        cls._assign_column_types(df, columns)
        if reset_index:
            df.set_index(index, inplace=True)
        return df

    def get_table_and_convert(
        self,
        query: str,
        index: str = None,
        limit: int = None,
        custom_maps: Dict = None,
        columns: Dict = None,
    ) -> pd.DataFrame:

        result = self.get_table(
            query, index, limit, map_data_types=True, custom_maps=custom_maps, columns=columns
        )

        if not self.map_data_types and len(result) > 0:
            result = self.manual_convert_types(result, columns, index)

        return result
