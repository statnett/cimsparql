from typing import Dict, List, Tuple, TypeVar, Union

import pandas as pd

from cimsparql import queries, ssh_queries, sv_queries, tp_queries
from cimsparql.type_mapper import TypeMapper
from cimsparql.url import Prefix

CimModelType = TypeVar("CimModelType", bound="CimModel")


class CimModel(Prefix):
    """Used to query with sparql queries (typically CIM)
    """

    def __init__(self, mapper: TypeMapper, *args, **kwargs):
        self._load_from_source(*args, **kwargs)
        self.mapper = mapper

    @property
    def mapper(self) -> TypeMapper:
        """Mapper used to convert str → type described by ontology in Graphdb

        Getter:
           Returns a mapper that can be used by self or other instances
        Setter:
           Sets mapper for self. Query GraphDB if not provided (None)
        """
        return self._mapper

    @mapper.setter
    def mapper(self, mapper: TypeMapper = None):
        if mapper is None and "rdfs" in self.prefixes:
            self._mapper = TypeMapper(self)
        else:
            self._mapper = mapper

    def _query_str(self, query: str, limit: int = None) -> str:
        q = f"{self.header_str()}\n\n{query}"
        if limit is not None:
            q += f" limit {limit}"
        return q

    def bus_data(
        self, region: str = "NO", sub_region: bool = False, limit: int = None, dry_run: bool = False
    ) -> pd.DataFrame:
        """Query name of topological nodes (TP query).

        Args:
           region: Limit to region (use None to get all)
           sub_region: True - assume sub regions, False - assume region
           limit: return first 'limit' number of rows
           dry_run: return string with sql query
        """
        query = queries.bus_data(region, sub_region)
        if dry_run:
            return self._query_str(query, limit=limit)
        else:
            return self.get_table_and_convert(query, index="mrid", limit=limit)

    def loads(
        self,
        load_type: List[str],
        load_vars: Tuple[str] = ("p", "q"),
        region: str = "NO",
        sub_region: bool = False,
        connectivity: str = None,
        station_group: bool = False,
        with_sequence_number: bool = False,
        network_analysis: bool = True,
        limit: int = None,
        dry_run: bool = False,
    ) -> pd.DataFrame:
        """Query load data

        Args:
           load_type: List of load types. Allowed: "ConformLoad", "NonConformLoad", "EnergyConsumer"
           load_vars: List of additional load vars to return. Possible are 'p' and/or 'q'.
           region: Limit to region
           sub_region: Assume region is a sub_region
           connectivity: Include connectivity mrids
           station_group: return station group mrid (if any)
           with_sequence_number: Include the sequence numbers in output
           network_analysis: Include only network analysis enabled components
           loads: return first 'limit' number of rows
           dry_run: return string with sql query

        Returns:
           DataFrame: with mrid as index and columns ['terminal_mrid', 'bid_marked_code', 'p', 'q',
           'station_group']

        Example:
           >>> from cimsparql.graphdb import GraphDBClient
           >>> from cimsparql.url import service
           >>> gdbc = GraphDBClient(service('SNMST-MasterCim15-VERSION-LATEST'))
           >>> gdbc.loads(load_type=['ConformLoad', 'NonConformLoad'])
        """
        query = queries.load_query(
            load_type,
            load_vars,
            region,
            sub_region,
            connectivity,
            with_sequence_number,
            network_analysis,
            station_group,
            self.cim_version,
        )
        if dry_run:
            return self._query_str(query, limit=limit)
        else:
            columns = {var: float for var in load_vars}
            return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def wind_generating_units(
        self, limit: int = None, network_analysis: bool = True, dry_run: bool = False
    ) -> pd.DataFrame:
        """Query wind generating units

        Args:
           network_analysis: Include only network analysis enabled components
           limit: return first 'limit' number of rows
           dry_run: return string with sql query

        Returns:.
           wind_generating_units: with mrid as index and columns ['terminal_mrid', 'bid_marked_code'
           , 'p', 'q']

        Example:
           >>> from cimsparql.graphdb import GraphDBClient
           >>> from cimsparql.url import service
           >>> gdbc = GraphDBClient(service('SNMST-MasterCim15-VERSION-LATEST'))
           >>> gdbc.wind_generating_units(limit=10)

        """
        query = queries.wind_generating_unit_query(network_analysis)
        if dry_run:
            return self._query_str(query, limit=limit)
        else:
            float_list = ["maxP", "allocationMax", "allocationWeight", "minP"]
            columns = {var: float for var in float_list}
            return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def synchronous_machines(
        self,
        synchronous_vars: Tuple[str] = ("sn", "p", "q"),
        region: str = "NO",
        sub_region: bool = False,
        limit: int = None,
        connectivity: str = None,
        dry_run: bool = False,
    ) -> pd.DataFrame:
        """Query synchronous machines

        Args:
           synchronous_vars: additional vars to include in output
           region: Limit to region
           sub_region: Assume region is a sub_region
           limit: return first 'limit' number of rows
           connectivity: Include connectivity mrids
           dry_run: return string with sql query

        Example:
           >>> from cimsparql.graphdb import GraphDBClient
           >>> from cimsparql.url import service
           >>> gdbc = GraphDBClient(service('SNMST-MasterCim15-VERSION-LATEST'))
           >>> gdbc.synchronous_machines(limit=10)
        """
        query = queries.synchronous_machines_query(
            synchronous_vars, region, sub_region, connectivity
        )
        if dry_run:
            return self._query_str(query, limit=limit)
        else:
            float_list = ["maxP", "minP", "allocationMax", "allocationWeight"]
            float_list += synchronous_vars
            columns = {var: float for var in float_list}
            return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def connections(
        self,
        rdf_types: Union[str, Tuple[str]] = ("cim:Breaker", "cim:Disconnector"),
        region: str = "NO",
        sub_region: bool = False,
        limit: int = None,
        connectivity: bool = True,
        dry_run: bool = False,
    ) -> pd.DataFrame:
        """Query connectors

        Args:
           rdf_types: Only cim:breaker and cim:Disconnector allowed
        Returns:

        Example:
           region: Limit to region
           sub_region: Assume region is a sub_region
           limit: return first 'limit' number of rows
           connectivity: Include connectivity mrids
           dry_run: return string with sql query

        Example:
           >>> from cimsparql.graphdb import GraphDBClient
           >>> from cimsparql.url import service
           >>> gdbc = GraphDBClient(service('SNMST-MasterCim15-VERSION-LATEST'))
           >>> gdbc.connections(limit=10)
        """
        query = queries.connection_query(
            self.cim_version, rdf_types, region, sub_region, connectivity
        )
        if dry_run:
            return self._query_str(query, limit=limit)
        else:
            return self.get_table_and_convert(query, index="mrid", limit=limit)

    def ac_lines(
        self,
        region: str = "NO",
        sub_region: bool = False,
        limit: int = None,
        connectivity: str = None,
        rates: Tuple[str] = queries.ratings,
        with_market: bool = False,
        temperatures: List = None,
        dry_run: bool = False,
    ) -> pd.DataFrame:
        """Query ac line segments

        Args:
           region: Limit to region
           sub_region: Assume region is a sub_region
           limit: return first 'limit' number of rows
           connectivity: Include connectivity mrids
           rates: include rates in output (available: "Normal", "Warning", "Overload")
           with_market: include marked connections in output
           temperatures: include temperature correction factors in output
           dry_run: return string with sql query

        Example:
           >>> from cimsparql.graphdb import GraphDBClient
           >>> from cimsparql.url import service
           >>> gdbc = GraphDBClient(service('SNMST-MasterCim15-VERSION-LATEST'))
           >>> gdbc.ac_lines(limit=10)
        """
        query = queries.ac_line_query(
            self.cim_version,
            self.ns["cim"],
            region,
            sub_region,
            connectivity,
            rates,
            with_market=with_market,
            temperatures=temperatures,
        )
        if dry_run:
            return self._query_str(query, limit=limit)
        else:
            ac_lines = self.get_table_and_convert(query, limit=limit)
            if temperatures is not None:
                for temperature in temperatures:
                    column = f"{queries.negpos(temperature)}_{abs(temperature)}"
                    ac_lines.loc[ac_lines[column].isna(), column] = 1.0
            return ac_lines

    def series_compensators(
        self,
        region: str = "NO",
        sub_region: bool = False,
        limit: int = None,
        connectivity: str = None,
        with_market: bool = False,
        dry_run: bool = False,
    ) -> pd.DataFrame:
        """Query series compensators

        Args:
           region: Limit to region
           sub_region: Assume region is a sub_region
           limit: return first 'limit' number of rows
           connectivity: Include connectivity mrids
           with_market: include marked connections in output
           dry_run: return string with sql query

        Example:
           >>> from cimsparql.graphdb import GraphDBClient
           >>> from cimsparql.url import service
           >>> gdbc = GraphDBClient(service('SNMST-MasterCim15-VERSION-LATEST'))
           >>> gdbc.series_compensators(limit=10)
        """
        query = queries.series_compensator_query(
            self.cim_version, region, sub_region, connectivity, with_market=with_market
        )
        if dry_run:
            return self._query_str(query, limit=limit)
        else:
            return self.get_table_and_convert(query, limit=limit)

    def transformers(
        self,
        region: str = "NO",
        sub_region: bool = False,
        limit: int = None,
        connectivity: str = None,
        rates: Tuple[str] = queries.ratings,
        with_market: bool = False,
        dry_run: bool = False,
    ) -> pd.DataFrame:
        """Query transformers

        Args:
           region: Limit to region
           sub_region: Assume region is a sub_region
           limit: return first 'limit' number of rows
           connectivity: Include connectivity mrids
           rates: include rates in output (available: "Normal", "Warning", "Overload")
           with_market: include marked connections in output
           dry_run: return string with sql query

        Example:
           >>> from cimsparql.graphdb import GraphDBClient
           >>> from cimsparql.url import service
           >>> gdbc = GraphDBClient(service('SNMST-MasterCim15-VERSION-LATEST'))
           >>> gdbc.transformers(limit=10)
        """
        query = queries.transformer_query(
            region, sub_region, connectivity, rates, with_market=with_market
        )
        if dry_run:
            return self._query_str(query, limit=limit)
        else:
            return self.get_table_and_convert(query, limit=limit)

    def disconnected(
        self, index: str = None, limit: int = None, dry_run: bool = False
    ) -> pd.DataFrame:
        query = ssh_queries.disconnected(self.cim_version)
        if dry_run:
            return self._query_str(query, limit=limit)
        else:
            return self.get_table(query, index=index, limit=limit)

    def ssh_synchronous_machines(self, limit: int = None, dry_run: bool = False) -> pd.DataFrame:
        query = ssh_queries.synchronous_machines()
        if dry_run:
            return self._query_str(query, limit=limit)
        else:
            columns = {"p": float, "q": float, "controlEnabled": bool}
            return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def ssh_load(
        self, rdf_types: List[str] = None, limit: int = None, dry_run: bool = False
    ) -> pd.DataFrame:
        if rdf_types is None:
            rdf_types = ["cim:ConformLoad", "cim:NonConformLoad"]
        query = ssh_queries.load(rdf_types)
        if dry_run:
            return self._query_str(query, limit=limit)
        else:
            columns = {"p": float, "q": float}
            return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def ssh_generating_unit(
        self, rdf_types: List[str] = None, limit: int = None, dry_run: bool = False
    ) -> pd.DataFrame:
        if rdf_types is None:
            rdf_types = [f"cim:{unit}GeneratingUnit" for unit in ["Hydro", "Thermal", "Wind"]]
        query = ssh_queries.generating_unit(rdf_types)
        if dry_run:
            return self._query_str(query, limit=limit)
        else:
            columns = {"normalPF": float}
            return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def terminal(self, limit: int = None, dry_run: bool = False) -> pd.DataFrame:
        query = tp_queries.terminal(self.cim_version)
        if dry_run:
            return self._query_str(query, limit=limit)
        else:
            columns = {"connected": bool}
            return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def topological_node(self, limit: int = None, dry_run: bool = False) -> pd.DataFrame:
        query = tp_queries.topological_node()
        if dry_run:
            return self._query_str(query, limit=limit)
        else:
            columns = {"BaseVoltage": float}
            return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def powerflow(
        self, power: Tuple[str] = ("p", "q"), limit: int = None, dry_run: bool = False
    ) -> pd.DataFrame:
        query = sv_queries.powerflow(power)
        if dry_run:
            return self._query_str(query, limit=limit)
        else:
            columns = {x: float for x in power}
            return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def voltage(
        self, voltage_vars: Tuple[str] = ("v", "angle"), limit: int = None, dry_run: bool = False
    ) -> pd.DataFrame:
        query = sv_queries.voltage(voltage_vars)
        if dry_run:
            return self._query_str(query, limit=limit)
        else:
            columns = {x: float for x in voltage_vars}
            return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    def tapstep(self, limit: int = None, dry_run: bool = False) -> pd.DataFrame:
        query = sv_queries.tapstep()
        if dry_run:
            return self._query_str(query, limit=limit)
        else:
            columns = {"position": float}
            return self.get_table_and_convert(query, index="mrid", limit=limit, columns=columns)

    @property
    def regions(self) -> pd.DataFrame:
        """Query regions

        Property:
           regions: List of regions in database

        Example:
           >>> from cimsparql.graphdb import GraphDBClient
           >>> from cimsparql.url import service
           >>> gdbc = GraphDBClient(service('SNMST-MasterCim15-VERSION-LATEST'))
           >>> gdbc.regions
        """
        query = queries.regions_query()
        return self.get_table_and_convert(query, index="mrid")

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
                result.loc[:, column] = pd.to_numeric(result[column], errors="coerce").astype(
                    column_type
                )

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
           >>> gdbc = GraphDBClient(service('SNMST-MasterCim15-VERSION-LATEST'))
           >>> query = 'select * where { ?subject ?predicate ?object }'
           >>> gdbc.get_table(query, limit=10)
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
            return self.mapper.have_cim_version(self.prefixes["cim"])
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
            query, index, limit, map_data_types=True, custom_maps=custom_maps, columns=columns,
        )

        if not self.map_data_types and len(result) > 0:
            result = self.manual_convert_types(result, columns, index)

        return result
