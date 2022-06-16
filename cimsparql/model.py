"""The cimsparql.model module contains the base class CimModel for both graphdb.GraphDBClient with
function get_table as well as a set of predefined CIM queries.
"""

import re
from abc import ABC, abstractmethod
from datetime import datetime
from functools import cached_property
from typing import Dict, Iterable, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from cimsparql import line_queries, queries
from cimsparql import query_support as sup
from cimsparql import ssh_queries, sv_queries, tp_queries
from cimsparql.constants import union_split
from cimsparql.enums import (
    ConverterTypes,
    GeneratorTypes,
    Impedance,
    LoadTypes,
    Power,
    Rates,
    SyncVars,
    TapChangerObjects,
)
from cimsparql.type_mapper import TypeMapper
from cimsparql.typehints import Region
from cimsparql.url import Prefix


class Model(Prefix, ABC):
    def __init__(self, mapper: Optional[TypeMapper], *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._mapper = mapper

    @abstractmethod
    def _get_table(self, *args, **kwargs) -> Tuple[pd.DataFrame, Dict[str, str]]:
        pass

    @abstractmethod
    def _col_map(self) -> Dict[str, str]:
        pass

    @cached_property
    def mapper(self) -> Optional[TypeMapper]:
        """Mapper used to convert str → type described by ontology in Graphdb

        Getter:
           Returns a mapper that can be used by self or other instances
        Setter:
           Sets mapper for self. Query GraphDB if not provided (None)
        """
        if self._mapper is None and "rdfs" in self.prefixes:
            return TypeMapper(self)
        return self._mapper

    def _query_with_header(self, query: str, limit: Optional[int] = None) -> str:
        query = "\n".join([self.header_str(query), query])
        if limit is not None:
            query += f" limit {limit}"
        return query

    @property
    def empty(self) -> bool:
        """Identify empty GraphDB repo"""
        try:
            self._get_table("SELECT * \n WHERE { ?s ?p ?o }", limit=1)
            return False
        except IndexError:
            return True

    @staticmethod
    def _assign_column_types(
        result: pd.DataFrame, columns: Dict[str, Union[bool, str, float, int]]
    ) -> None:
        for column, column_type in columns.items():
            if column_type is str:
                continue
            if column_type is bool:
                result[column] = result[column].str.contains("true", flags=re.IGNORECASE)
            else:
                result.loc[result[column] == "None", column] = ""
                result[column] = pd.to_numeric(result[column], errors="coerce").astype(column_type)

    @classmethod
    def col_map(cls, data_row, columns: Dict[str, str]) -> Dict[str, str]:
        columns = columns or {}
        col_map = cls._col_map(data_row)
        col_map.update(columns)
        return col_map

    def get_table(
        self,
        query: str,
        index: Optional[str] = None,
        limit: Optional[int] = None,
        map_data_types: bool = True,
        columns: Optional[Dict] = None,
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
            return pd.DataFrame([])

        if map_data_types and self.mapper is not None:
            col_map = self.col_map(data_row, columns)
            result = self.mapper.map_data_types(result, col_map)

        if index and not result.empty:
            result.set_index(index, inplace=True)
        return result

    @property
    def map_data_types(self) -> bool:
        return "cim" in self.prefixes and self.mapper.have_cim_version(self.prefixes["cim"])

    @classmethod
    def _manual_convert_types(
        cls, df: pd.DataFrame, columns: Optional[Dict], index: Optional[str]
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

    def _get_table_and_convert(
        self,
        query: str,
        limit: Optional[int] = None,
        index: Optional[str] = None,
        columns: Optional[Dict[str, str]] = None,
    ) -> pd.DataFrame:
        result = self.get_table(query, index, limit, map_data_types=True, columns=columns)

        if not self.map_data_types and len(result) > 0:
            result = self._manual_convert_types(result, columns, index)

        return result


class CimModel(Model):
    """Used to query with sparql queries (typically CIM)"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._date_version = None

    @property
    def date_version(self) -> datetime:
        """Activation date for this repository"""
        if self._date_version:
            return self._date_version
        repository_date = self._get_table_and_convert(queries.version_date())
        self._date_version = repository_date["activationDate"].values[0]
        if isinstance(self._date_version, np.datetime64):
            self._date_version = self._date_version.astype("<M8[s]").astype(datetime)
        return self._date_version

    def full_model(self, dry_run: bool = False) -> Union[pd.DataFrame, str]:
        query = queries.full_model()
        if dry_run:
            return self._query_with_header(query)
        return self._get_table_and_convert(query)

    def bus_data(
        self,
        region: Region = None,
        sub_region: bool = False,
        limit: Optional[int] = None,
        dry_run: bool = False,
        with_dummy_buses: bool = False,
    ) -> Union[pd.DataFrame, str]:
        """Query name of topological nodes (TP query).

        Args:
           region: Limit to region (use None to get all)
           sub_region: True - assume sub regions, False - assume region
           limit: return first 'limit' number of rows
           dry_run: return string with sql query
        """
        query = queries.bus_data(region, sub_region)
        if with_dummy_buses:
            dummy_bus_query = queries.three_winding_dummy_bus(region, sub_region)
            combined = sup.combine_statements(query, dummy_bus_query, split=union_split)
            query = sup.combine_statements(
                sup.select_statement(["?mrid", "?name", "?busname", "?un", "?area", "?bidzone"]),
                f"where {{{{{combined}}}}}",
            )
        if dry_run:
            return self._query_with_header(query, limit)
        return self._get_table_and_convert(query, limit, index="mrid")

    def phase_tap_changers(
        self,
        region: Region = None,
        sub_region: bool = False,
        with_tap_changer_values: bool = True,
        impedance: Iterable[Impedance] = tuple(Impedance),
        tap_changer_objects: Iterable[TapChangerObjects] = tuple(TapChangerObjects),
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> Union[pd.DataFrame, str]:
        """Get list of phase tap changers"""
        query = queries.phase_tap_changer_query(
            region, sub_region, with_tap_changer_values, impedance, tap_changer_objects
        )
        if dry_run:
            return self._query_with_header(query, limit)
        return self._get_table_and_convert(query, limit, index="mrid")

    def loads(
        self,
        load_type: LoadTypes,
        load_vars: Optional[Iterable[Power]] = None,
        region: Region = None,
        sub_region: bool = False,
        connectivity: Optional[Optional[str]] = None,
        nodes: Optional[Optional[str]] = None,
        station_group_optional: bool = True,
        station_group: bool = False,
        with_sequence_number: bool = False,
        network_analysis: bool = True,
        limit: Optional[int] = None,
        dry_run: bool = False,
        ssh_graph: Optional[str] = None,
    ) -> Union[pd.DataFrame, str]:
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
           limit: return first 'limit' number of rows
           dry_run: return string with sql query

        Returns:
           DataFrame: with mrid as index and columns ['terminal_mrid', 'bid_marked_code', 'p', 'q',
           'station_group']

        Example:
           >>> from cimsparql.graphdb import GraphDBClient
           >>> from cimsparql.url import service
           >>> gdbc = GraphDBClient(service('LATEST'))
           >>> gdbc.loads(load_type=['ConformLoad', 'NonConformLoad'])
        """
        query = queries.load_query(
            load_type,
            load_vars,
            region,
            sub_region,
            connectivity,
            nodes,
            station_group_optional,
            with_sequence_number,
            network_analysis,
            station_group,
            self.cim_version,
            ssh_graph,
        )
        if dry_run:
            return self._query_with_header(query, limit)

        return self._get_table_and_convert(query, limit, index="mrid")

    def wind_generating_units(
        self,
        limit: Optional[int] = None,
        network_analysis: bool = True,
        dry_run: bool = False,
    ) -> Union[pd.DataFrame, str]:
        """Query wind generating units

        Args:
           network_analysis: Include only network analysis enabled components
           limit: return first 'limit' number of rows
           dry_run: return string with sql query

        Returns:.
           wind_generating_units: with mrid as index and columns ['station_group', 'market_code',
           'maxP', 'allocationMax', 'allocationWeight', 'minP', 'name', 'power_plant_mrid']

        Example:
           >>> from cimsparql.graphdb import GraphDBClient
           >>> from cimsparql.url import service
           >>> gdbc = GraphDBClient(service('LATEST'))
           >>> gdbc.wind_generating_units(limit=10)

        """
        query = queries.wind_generating_unit_query(network_analysis)
        if dry_run:
            return self._query_with_header(query, limit)
        return self._get_table_and_convert(query, limit, index="mrid")

    def synchronous_machines(
        self,
        sync_vars: Iterable[SyncVars] = tuple(SyncVars),
        region: Region = None,
        sub_region: bool = False,
        connectivity: Optional[str] = None,
        nodes: Optional[str] = None,
        station_group_optional: bool = True,
        with_sequence_number: bool = False,
        network_analysis: bool = True,
        u_groups: bool = False,
        limit: Optional[int] = None,
        dry_run: bool = False,
        ssh_graph: Optional[str] = None,
    ) -> Union[pd.DataFrame, str]:
        """Query synchronous machines

        Args:
           synchronous_vars: additional vars to include in output
           region: Limit to region
           sub_region: Assume region is a sub_region
           connectivity: Include connectivity mrids
           station_group_optional: Assume station group is optional
           with_sequence_number: add this numbers
           network_analysis: query SN:Equipment.networkAnalysisEnable
           u_groups: Filter out station groups where name starts with 'U-'
           limit: return first 'limit' number of rows
           dry_run: return string with sql query

        Example:
           >>> from cimsparql.graphdb import GraphDBClient
           >>> from cimsparql.url import service
           >>> gdbc = GraphDBClient(service('LATEST'))
           >>> gdbc.synchronous_machines(limit=10)
        """
        query = queries.synchronous_machines_query(
            sync_vars,
            region,
            sub_region,
            connectivity,
            nodes,
            station_group_optional,
            self.cim_version,
            with_sequence_number,
            network_analysis,
            u_groups,
            ssh_graph,
        )
        if dry_run:
            return self._query_with_header(query, limit)
        return self._get_table_and_convert(query, limit, index="mrid")

    def connections(
        self,
        rdf_types: Union[str, Iterable[str]] = ("cim:Breaker", "cim:Disconnector"),
        region: Region = None,
        sub_region: bool = False,
        connectivity: Optional[str] = None,
        nodes: Optional[str] = None,
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> Union[pd.DataFrame, str]:
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
           >>> gdbc = GraphDBClient(service('LATEST'))
           >>> gdbc.connections(limit=10)
        """
        query = queries.connection_query(
            self.cim_version, rdf_types, region, sub_region, connectivity, nodes
        )
        if dry_run:
            return self._query_with_header(query, limit)
        return self._get_table_and_convert(query, limit, index="mrid")

    def borders(
        self,
        region: Union[str, List[str]],
        sub_region: bool = False,
        nodes: Optional[str] = None,
        ignore_hvdc: bool = True,
        with_market_code: bool = False,
        market_optional: bool = False,
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> Union[pd.DataFrame, str]:
        """Retrieve ACLineSegments where one terminal is inside and the other is outside the region

        Args:
            region: Inside area
            sub_region: regions is sub areas
            ignore_hvdc: ignore ac lines with HVDC in name
            with_marked_code: include SN:Line.marketCode
            market_optional: include lines without market code if with_marked_code set
            limit: return first 'limit' number of rows
            dry_run: return string with sql query

        """
        query = line_queries.borders_query(
            self.cim_version,
            region,
            sub_region,
            nodes,
            ignore_hvdc,
            with_market_code,
            market_optional,
        )
        if dry_run:
            return self._query_with_header(query, limit)
        return self._get_table_and_convert(query, limit, index="mrid")

    def converters(
        self,
        region: Region = None,
        sub_region: bool = False,
        converter_types: Iterable[ConverterTypes] = tuple(ConverterTypes),
        nodes: Optional[str] = None,
        sequence_numbers: Optional[List[int]] = None,
        dry_run: bool = False,
    ) -> Union[pd.DataFrame, str]:
        query = queries.converters(
            region, sub_region, self.in_prefixes(converter_types), nodes, sequence_numbers
        )
        if dry_run:
            return self._query_with_header(query)
        return self._get_table_and_convert(query, index="mrid")

    def transformers_connected_to_converter(
        self,
        region: Region = None,
        sub_region: bool = False,
        converter_types: Iterable[ConverterTypes] = tuple(ConverterTypes),
        dry_run: bool = False,
    ) -> Union[pd.DataFrame, str]:
        """Query list of transformer connected at a converter (Voltage source or DC)

        Args:
           region: Limit to region
           sub_region: Assume region is a sub_region
           converter_types: VoltageSource, DC (cim:VsConverter, cim:DCConverterUnit)
           dry_run: return string with sql query

        """
        query = queries.transformers_connected_to_converter(
            region, sub_region, self.in_prefixes(converter_types)
        )
        if dry_run:
            return self._query_with_header(query)
        return self._get_table_and_convert(query, index="converter_mrid")

    def ac_lines(
        self,
        region: Region = None,
        sub_region: bool = False,
        limit: Optional[int] = None,
        connectivity: Optional[str] = None,
        nodes: Optional[str] = None,
        with_loss: bool = False,
        rates: Iterable[Rates] = (Rates.Normal,),
        network_analysis: bool = True,
        with_market: bool = False,
        temperatures: Optional[List[int]] = None,
        impedance: Iterable[Impedance] = tuple(Impedance),
        length: bool = False,
        dry_run: bool = False,
    ) -> Union[pd.DataFrame, str]:
        """Query ac line segments

        Args:
           region: Limit to region
           sub_region: Assume region is a sub_region
           limit: return first 'limit' number of rows
           connectivity: Include connectivity mrids as column name '{connectivity}_{1|2}'
           rates: include rates in output (available: "Normal", "Warning", "Overload")
           with_market: include marked connections in output
           temperatures: include temperature correction factors in output
           dry_run: return string with sql query

        Example:
           >>> from cimsparql.graphdb import GraphDBClient
           >>> from cimsparql.url import service
           >>> gdbc = GraphDBClient(service('LATEST'))
           >>> gdbc.ac_lines(limit=10)
        """
        query = line_queries.ac_line_query(
            self.cim_version,
            self.ns["cim"],
            region,
            sub_region,
            connectivity,
            nodes,
            with_loss,
            rates,
            network_analysis,
            with_market,
            temperatures,
            impedance,
            length,
        )
        if dry_run:
            return self._query_with_header(query, limit)
        ac_lines = self._get_table_and_convert(query, limit=limit)
        if temperatures is not None:
            for temperature in temperatures:
                column = f"{sup.negpos(temperature)}_{abs(temperature)}_factor"
                ac_lines.loc[ac_lines[column].isna(), column] = 1.0
        return ac_lines

    def series_compensators(
        self,
        region: Region = None,
        sub_region: bool = False,
        connectivity: Optional[str] = None,
        with_loss: bool = False,
        nodes: Optional[str] = None,
        rates: Iterable[Rates] = (Rates.Normal,),
        network_analysis: bool = True,
        with_market: bool = False,
        impedance: Iterable[Impedance] = tuple(Impedance),
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> Union[pd.DataFrame, str]:
        """Query series compensators

        Args:
           region: Limit to region
           sub_region: Assume region is a sub_region
           limit: return first 'limit' number of rows
           connectivity: Include connectivity mrids
           network_analysis: query SN:Equipment.networkAnalysisEnable
           with_market: include marked connections in output
           dry_run: return string with sql query

        Example:
           >>> from cimsparql.graphdb import GraphDBClient
           >>> from cimsparql.url import service
           >>> gdbc = GraphDBClient(service('LATEST'))
           >>> gdbc.series_compensators(limit=10)
        """
        query = line_queries.series_compensator_query(
            self.cim_version,
            region,
            sub_region,
            connectivity,
            with_loss,
            nodes,
            rates,
            network_analysis,
            with_market,
            impedance,
        )
        if dry_run:
            return self._query_with_header(query, limit)
        return self._get_table_and_convert(query, limit=limit)

    def transformers(
        self,
        region: Region = None,
        sub_region: bool = False,
        connectivity: Optional[str] = None,
        rates: Iterable[Rates] = (Rates.Normal,),
        network_analysis: bool = True,
        with_market: bool = False,
        impedance: Iterable[Impedance] = tuple(Impedance),
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> Union[pd.DataFrame, str]:
        """Query transformer windings

        Args:
           region: Limit to region
           sub_region: Assume region is a sub_region
           connectivity: Include connectivity mrids
           rates: include rates in output (available: "Normal", "Warning", "Overload")
           network_analysis: query SN:Equipment.networkAnalysisEnable
           with_market: include marked connections in output
           impedance: values returned
           limit: return first 'limit' number of rows
           dry_run: return string with sql query

        Example:
           >>> from cimsparql.graphdb import GraphDBClient
           >>> from cimsparql.url import service
           >>> gdbc = GraphDBClient(service('LATEST'))
           >>> gdbc.transformers(limit=10)
        """
        query = queries.transformer_query(
            region, sub_region, connectivity, rates, network_analysis, with_market, impedance
        )
        if dry_run:
            return self._query_with_header(query, limit)
        return self._get_table_and_convert(query, limit=limit)

    def two_winding_transformers(
        self,
        region: Region = None,
        sub_region: bool = False,
        rates: Iterable[Rates] = (Rates.Normal,),
        network_analysis: bool = True,
        with_market: bool = False,
        impedance: Iterable[Impedance] = tuple(Impedance),
        p_mrid: bool = False,
        nodes: Optional[str] = None,
        with_loss: bool = False,
        name: str = "?name",
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> Union[pd.DataFrame, str]:
        """Query two-winding transformer

        Args:
           region: Limit to region
           sub_region: Assume region is a sub_region
           rates: include rates in output (available: "Normal", "Warning", "Overload")
           network_analysis: query SN:Equipment.networkAnalysisEnable
           with_market: include marked connections in output
           impedance: values returned
           limit: return first 'limit' number of rows
           dry_run: return string with sql query

        Example:
           >>> from cimsparql.graphdb import GraphDBClient
           >>> from cimsparql.url import service
           >>> gdbc = GraphDBClient(service('LATEST'))
           >>> gdbc.two_winding_transformers(limit=10)
        """
        query = queries.two_winding_transformer_query(
            region,
            sub_region,
            rates,
            network_analysis,
            with_market,
            p_mrid,
            nodes,
            with_loss,
            name,
            impedance,
            self.cim_version,
        )
        if dry_run:
            return self._query_with_header(query, limit)
        return self._get_table_and_convert(query, limit=limit)

    def three_winding_transformers(
        self,
        region: Region = None,
        sub_region: bool = False,
        rates: Iterable[Rates] = (Rates.Normal,),
        network_analysis: bool = True,
        with_market: bool = False,
        impedance: Iterable[Impedance] = tuple(Impedance),
        p_mrid: bool = False,
        nodes: Optional[str] = None,
        with_loss: bool = False,
        name: str = "?name",
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> Union[pd.DataFrame, str]:
        """Query three-winding transformer. Return as three two-winding transformers.

        Args:
           region: Limit to region
           sub_region: Assume region is a sub_regionb
           rates: include rates in output (available: "Normal", "Warning", "Overload")
           network_analysis: query SN:Equipment.networkAnalysisEnable
           with_market: include marked connections in output
           impedance: values returned
           limit: return first 'limit' number of rows
           dry_run: return string with sql query

        Example:
           >>> from cimsparql.graphdb import GraphDBClient
           >>> from cimsparql.url import service
           >>> gdbc = GraphDBClient(service('LATEST'))
           >>> gdbc.two_winding_transformers(limit=10)
        """
        query = queries.three_winding_transformer_query(
            region,
            sub_region,
            rates,
            network_analysis,
            with_market,
            p_mrid,
            nodes,
            with_loss,
            name,
            impedance,
            self.cim_version,
        )
        if dry_run:
            return self._query_with_header(query, limit)
        return self._get_table_and_convert(query, limit=limit)

    def disconnected(
        self, index: Optional[str] = None, limit: Optional[int] = None, dry_run: bool = False
    ) -> Union[pd.DataFrame, str]:
        """Query disconneced status from ssh profile (not available in GraphDB)

        Args:
           index: Column to use as index
           limit: return first 'limit' number of rows
           dry_run: return string with sql query
        """
        query = ssh_queries.disconnected(self.cim_version)
        if dry_run:
            return self._query_with_header(query, limit)
        return self.get_table(query, index=index, limit=limit)

    def ssh_synchronous_machines(
        self, limit: Optional[int] = None, dry_run: bool = False
    ) -> Union[pd.DataFrame, str]:
        """Query synchronous machines from ssh profile (not available in GraphDB)

        Args:
           limit: return first 'limit' number of rows
           dry_run: return string with sql query
        """
        query = ssh_queries.synchronous_machines()
        if dry_run:
            return self._query_with_header(query, limit)
        return self._get_table_and_convert(query, limit, index="mrid")

    def ssh_load(
        self,
        rdf_types: Optional[List[str]] = None,
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> Union[pd.DataFrame, str]:
        """Query load data from ssh profile (not available in GraphDB)

        Args:
           rdf_types: allowed ["cim:ConformLoad", "cim:NonConformLoad"]
           limit: return first 'limit' number of rows
           dry_run: return string with sql query

        """
        if rdf_types is None:
            rdf_types = ["cim:ConformLoad", "cim:NonConformLoad"]
        query = ssh_queries.load(rdf_types)
        if dry_run:
            return self._query_with_header(query, limit)
        return self._get_table_and_convert(query, limit, index="mrid")

    def ssh_generating_unit(
        self,
        rdf_types: Optional[List[str]] = None,
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> Union[pd.DataFrame, str]:
        """Query generating units from ssh profile (not available in GraphDB)

        Args:
           rdf_types: allowed
               ["cim:HydroGeneratingUnit", "cim:ThermalGeneratingUnit", "cim:WindGeneratingUnit"]
           limit: return first 'limit' number of rows
           dry_run: return string with sql query

        """
        if rdf_types is None:
            rdf_types = [f"cim:{unit}GeneratingUnit" for unit in GeneratorTypes]
        query = ssh_queries.generating_unit(rdf_types)
        if dry_run:
            return self._query_with_header(query, limit)
        return self._get_table_and_convert(query, limit, index="mrid")

    def terminal(
        self, limit: Optional[int] = None, dry_run: bool = False
    ) -> Union[pd.DataFrame, str]:
        """Query terminals from tp profile (not available in GraphDB)

        Args:
           limit: return first 'limit' number of rows
           dry_run: return string with sql query
        """
        query = tp_queries.terminal(self.cim_version)
        if dry_run:
            return self._query_with_header(query, limit)
        return self._get_table_and_convert(query, limit, index="mrid")

    def topological_node(
        self, limit: Optional[int] = None, dry_run: bool = False
    ) -> Union[pd.DataFrame, str]:
        """Query topological nodes from tp profile (not available in GraphDB)

        Args:
           limit: return first 'limit' number of rows
           dry_run: return string with sql query
        """
        query = tp_queries.topological_node()
        if dry_run:
            return self._query_with_header(query, limit)
        return self._get_table_and_convert(query, limit, index="mrid")

    def powerflow(
        self,
        power: Tuple[str, ...] = ("p", "q"),
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> Union[pd.DataFrame, str]:
        """Query powerflow from sv profile (not available in GraphDB)

        Args:
           power: Allowed ['p','q']
           limit: return first 'limit' number of rows
           dry_run: return string with sql query
        """
        query = sv_queries.powerflow(power)
        if dry_run:
            return self._query_with_header(query, limit)
        return self._get_table_and_convert(query, limit, index="mrid")

    def voltage(
        self,
        voltage_vars: Iterable[str] = ("v", "angle"),
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> Union[pd.DataFrame, str]:
        """Query voltage from sv profile (not available in GraphDB)

        Args:
           voltage_vars: allowed ["v", "angle"]
           limit: return first 'limit' number of rows
           dry_run: return string with sql query
        """
        query = sv_queries.voltage(voltage_vars)
        if dry_run:
            return self._query_with_header(query, limit)
        return self._get_table_and_convert(query, limit, index="mrid")

    def tapstep(
        self, limit: Optional[int] = None, dry_run: bool = False
    ) -> Union[pd.DataFrame, str]:
        """Query tapstep from sv profile (not available in GraphDB)

        Args:
           limit: return first 'limit' number of rows
           dry_run: return string with sql query
        """
        query = sv_queries.tapstep()
        if dry_run:
            return self._query_with_header(query, limit)
        return self._get_table_and_convert(query, limit, index="mrid")

    @property
    def regions(self) -> pd.DataFrame:
        """Query regions

        Property:
           regions: List of regions in database

        Example:
           >>> from cimsparql.graphdb import GraphDBClient
           >>> from cimsparql.url import service
           >>> gdbc = GraphDBClient(service('LATEST'))
           >>> gdbc.regions
        """
        query = queries.regions_query()
        return self._get_table_and_convert(query, limit=None, index="mrid")
