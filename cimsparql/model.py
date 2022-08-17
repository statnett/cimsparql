"""The cimsparql.model module contains the base class CimModel for both graphdb.GraphDBClient with
function get_table as well as a set of predefined CIM queries.
"""

import re
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
    Voltage,
)
from cimsparql.graphdb import GraphDBClient, ServiceConfig
from cimsparql.type_mapper import TypeMapper
from cimsparql.typehints import Region


class Model:
    def __init__(self, mapper: Optional[TypeMapper], client: GraphDBClient) -> None:
        self._mapper = mapper
        self.client = client

    @staticmethod
    def _col_map(data_row) -> Dict[str, str]:
        return {
            column: data.get("datatype", data.get("type", None))
            for column, data in data_row.items()
        }

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

    @property
    def map_data_types(self) -> bool:
        pref = self.client.prefixes.prefixes
        return "cim" in pref and self.mapper.have_cim_version(pref["cim"])

    def get_table_and_convert(
        self,
        query: str,
        limit: Optional[int] = None,
        index: Optional[str] = None,
        columns: Optional[Dict[str, str]] = None,
    ) -> pd.DataFrame:
        result, data_row = self.client.get_table(query, limit)

        if self.mapper is not None:
            col_map = self.col_map(data_row, columns)
            result = self.mapper.map_data_types(result, col_map)

        if index and not result.empty:
            result.set_index(index, inplace=True)
        return result


class CimModel(Model):
    """Used to query with sparql queries (typically CIM)"""

    def __init__(self, mapper: Optional[TypeMapper], client: GraphDBClient) -> None:
        super().__init__(mapper, client)
        self._date_version = None

    @property
    def cim_version(self) -> int:
        return self.client.prefixes.cim_version

    @property
    def date_version(self) -> datetime:
        """Activation date for this repository"""
        if self._date_version:
            return self._date_version
        repository_date = self.get_table_and_convert(queries.version_date())
        self._date_version = repository_date["activationDate"].values[0]
        if isinstance(self._date_version, np.datetime64):
            self._date_version = self._date_version.astype("<M8[s]").astype(datetime)
        return self._date_version

    def full_model(self, dry_run: bool = False) -> Union[pd.DataFrame, str]:
        query = queries.full_model()
        if dry_run:
            return self.client.query_with_header(query)
        return self.get_table_and_convert(query)

    def bus_data(
        self,
        region: Region = None,
        sub_region: bool = False,
        with_market: bool = True,
        with_dummy_buses: bool = False,
        container: bool = False,
        delta_power: bool = True,
        network_analysis: bool = True,
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> Union[pd.DataFrame, str]:
        """Query name of topological nodes (TP query).

        Args:
           region: Limit to region (use None to get all)
           sub_region: True - assume sub regions, False - assume region
           limit: return first 'limit' number of rows
           dry_run: return string with sql query
        """
        container_variable = "?container" if container else ""
        query = queries.bus_data(
            region, sub_region, with_market, container_variable, self.cim_version, delta_power
        )
        if with_dummy_buses:
            dummy_bus_query = queries.three_winding_dummy_bus(
                region, sub_region, with_market, container_variable, network_analysis, delta_power
            )
            combined = sup.combine_statements(query, dummy_bus_query, split=union_split)
            variables = ["?node", "?name", "?busname", "?un", "?station"]
            if delta_power:
                variables.append("?delta_p")
            if with_market:
                variables.append("?bidzone")
            if container:
                variables.append("?container")
            query = sup.combine_statements(
                sup.select_statement(variables), f"where {{{{{combined}}}}}"
            )
        if dry_run:
            return self.client.query_with_header(query, limit)
        df = self.get_table_and_convert(query, limit)
        return df.groupby("node").first()

    def phase_tap_changers(
        self,
        region: Region = None,
        sub_region: bool = False,
        with_tap_changer_values: bool = True,
        impedance: Iterable[Impedance] = Impedance,
        tap_changer_objects: Iterable[TapChangerObjects] = TapChangerObjects,
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> Union[pd.DataFrame, str]:
        """Get list of phase tap changers"""
        query = queries.phase_tap_changer_query(
            region, sub_region, with_tap_changer_values, impedance, tap_changer_objects
        )
        if dry_run:
            return self.client.query_with_header(query, limit)
        return self.get_table_and_convert(query, limit, index="mrid")

    def loads(
        self,
        load_type: Iterable[LoadTypes] = tuple(LoadTypes),
        load_vars: Optional[Iterable[Power]] = None,
        region: Region = None,
        sub_region: bool = False,
        connectivity: Optional[str] = None,
        nodes: Optional[str] = None,
        station_group: bool = False,
        with_sequence_number: bool = False,
        network_analysis: bool = True,
        with_bidzone: bool = True,
        limit: Optional[int] = None,
        dry_run: bool = False,
        ssh_graph: Optional[str] = None,
    ) -> Union[pd.DataFrame, str]:
        """Query load data

        Args:
            load_type: List of load types. Allowed: "ConformLoad", "NonConformLoad",
                "EnergyConsumer"
            load_vars: List of additional load vars to return. Possible are 'p' and/or 'q'.
            region: Limit to region
            sub_region: Assume region is a sub_region
            connectivity: Include connectivity mrids
            station_group: return station group mrid (if any)
            with_sequence_number: Include the sequence numbers in output
            network_analysis: Include only network analysis enabled components
            with_bidzone: If True bidzone information is added, otherwise it is omitted
            limit: return first 'limit' number of rows
            dry_run: return string with sql query

        Returns:
           DataFrame: with mrid as index and columns ['terminal_mrid', 'bid_marked_code', 'p', 'q',
           'station_group']

        Example:
           >>> from cimsparql.model import get_cim_model
           >>> server_url = "127.0.0.1:7200"
           >>> model = get_cim_model(server_url, "LATEST")
           >>> model.loads(load_type=['ConformLoad', 'NonConformLoad'])
        """
        query = queries.load_query(
            load_type,
            load_vars,
            region,
            sub_region,
            connectivity,
            nodes,
            station_group,
            with_sequence_number,
            network_analysis,
            self.cim_version,
            with_bidzone,
            ssh_graph,
        )
        if dry_run:
            return self.client.query_with_header(query, limit)

        return self.get_table_and_convert(query, limit, index="mrid")

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
            >>> from cimsparql.model import get_cim_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_cim_model(server_url, "LATEST")
            >>> model.wind_generating_units(limit=10)

        """
        query = queries.wind_generating_unit_query(network_analysis)
        if dry_run:
            return self.client.query_with_header(query, limit)
        return self.get_table_and_convert(query, limit, index="mrid")

    def synchronous_machines(
        self,
        sync_vars: Iterable[SyncVars] = SyncVars,
        region: Region = None,
        sub_region: bool = False,
        connectivity: Optional[str] = None,
        nodes: Optional[str] = None,
        station_group: bool = True,
        with_sequence_number: bool = False,
        network_analysis: bool = True,
        u_groups: bool = False,
        limit: Optional[int] = None,
        dry_run: bool = False,
        with_market: bool = True,
        ssh_graph: Optional[str] = None,
    ) -> Union[pd.DataFrame, str]:
        """Query synchronous machines

        Args:
           synchronous_vars: additional vars to include in output
           region: Limit to region
           sub_region: Assume region is a sub_region
           connectivity: Include connectivity mrids
           station_group: Assume station group is optional
           with_sequence_number: add this numbers
           network_analysis: query SN:Equipment.networkAnalysisEnable
           u_groups: Filter out station groups where name starts with 'U-'
           limit: return first 'limit' number of rows
           dry_run: return string with sql query

        Example:
            >>> from cimsparql.model import get_cim_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_cim_model(server_url, "LATEST")
            >>> model.synchronous_machines(limit=10)
        """
        query = queries.synchronous_machines_query(
            sync_vars,
            region,
            sub_region,
            connectivity,
            nodes,
            station_group,
            self.cim_version,
            with_sequence_number,
            network_analysis,
            u_groups,
            with_market,
            ssh_graph,
        )
        if dry_run:
            return self.client.query_with_header(query, limit)
        return self.get_table_and_convert(query, limit, index="mrid")

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
            >>> from cimsparql.model import get_cim_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_cim_model(server_url, "LATEST")
            >>> model.connections(limit=10)
        """
        query = queries.connection_query(
            self.cim_version, rdf_types, region, sub_region, connectivity, nodes
        )
        if dry_run:
            return self.client.query_with_header(query, limit)
        return self.get_table_and_convert(query, limit, index="mrid")

    def borders(
        self,
        region: Union[str, List[str]],
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
            self.cim_version, region, nodes, ignore_hvdc, with_market_code, market_optional
        )
        if dry_run:
            return self.client.query_with_header(query, limit)
        return self.get_table_and_convert(query, limit, index="mrid")

    def exchange(
        self,
        region: Union[str, List[str]],
        nodes: str,
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
        query = line_queries.exchange_query(
            self.cim_version, region, nodes, ignore_hvdc, with_market_code, market_optional
        )
        if dry_run:
            return self.client.query_with_header(query, limit)
        return self.get_table_and_convert(query, limit, index="mrid")

    def converters(
        self,
        region: Region = None,
        sub_region: bool = False,
        converter_types: Iterable[ConverterTypes] = ConverterTypes,
        nodes: Optional[str] = None,
        sequence_numbers: Optional[List[int]] = None,
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> Union[pd.DataFrame, str]:
        query = queries.converters(
            region,
            sub_region,
            self.client.prefixes.in_prefixes(converter_types),
            nodes,
            sequence_numbers,
            self.cim_version,
        )
        if dry_run:
            return self.client.query_with_header(query)
        return self.get_table_and_convert(query, limit, index="mrid")

    def transformers_connected_to_converter(
        self,
        region: Region = None,
        sub_region: bool = False,
        converter_types: Iterable[ConverterTypes] = ConverterTypes,
        on_primary_side: bool = True,
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> Union[pd.DataFrame, str]:
        """Query list of transformer connected at a converter (Voltage source or DC)

        Args:
           region: Limit to region
           sub_region: Assume region is a sub_region
           converter_types: VoltageSource, DC (cim:VsConverter, cim:DCConverterUnit)
           on_primary_side: Put converter on transformer primary side
           dry_run: return string with sql query

        """
        query = queries.transformers_connected_to_converter(
            region, sub_region, self.client.prefixes.in_prefixes(converter_types), on_primary_side
        )
        if dry_run:
            return self.client.query_with_header(query)
        return self.get_table_and_convert(query, limit, index="converter_mrid")

    def ac_lines(
        self,
        region: Region = None,
        sub_region: bool = False,
        connectivity: Optional[str] = None,
        nodes: Optional[str] = None,
        with_loss: bool = False,
        rates: Iterable[Rates] = (Rates.Normal,),
        network_analysis: bool = True,
        with_market: bool = False,
        temperatures: Optional[List[int]] = None,
        impedance: Iterable[Impedance] = Impedance,
        length: bool = False,
        limit: Optional[int] = None,
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
            >>> from cimsparql.model import get_cim_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_cim_model(server_url, "LATEST")
            >>> model.ac_lines(limit=10)
        """
        query = line_queries.ac_line_query(
            self.cim_version,
            self.client.prefixes.ns["cim"],
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
            return self.client.query_with_header(query, limit)
        ac_lines = self.get_table_and_convert(query, limit, index="mrid")
        if temperatures is not None:
            for temperature in temperatures:
                column = f"{sup.negpos(temperature)}_{abs(temperature)}_factor"
                ac_lines.loc[ac_lines[column].isna(), column] = 1.0
        return ac_lines

    def ac_line_mrids(
        self, limit: Optional[int] = None, dry_run: bool = False
    ) -> Union[pd.DataFrame, str]:
        query = queries.ac_line_mrids()
        if dry_run:
            return self.client.query_with_header(query)
        return self.get_table_and_convert(query, limit)

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
        impedance: Iterable[Impedance] = Impedance,
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
            >>> from cimsparql.model import get_cim_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_cim_model(server_url, "LATEST")
            >>> model.series_compensators(limit=10)
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
            return self.client.query_with_header(query, limit)
        return self.get_table_and_convert(query, limit, index="mrid")

    def transformers(
        self,
        region: Region = None,
        sub_region: bool = False,
        connectivity: Optional[str] = None,
        rates: Iterable[Rates] = (Rates.Normal,),
        network_analysis: bool = True,
        with_market: bool = False,
        impedance: Iterable[Impedance] = Impedance,
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
            >>> from cimsparql.model import get_cim_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_cim_model(server_url, "LATEST")
            >>> model.transformers(limit=10)
        """
        query = queries.transformer_query(
            region, sub_region, connectivity, rates, network_analysis, with_market, impedance
        )
        if dry_run:
            return self.client.query_with_header(query, limit)
        return self.get_table_and_convert(query, limit)

    def two_winding_transformers(
        self,
        region: Region = None,
        sub_region: bool = False,
        rates: Iterable[Rates] = (Rates.Normal,),
        network_analysis: bool = True,
        with_market: bool = False,
        impedance: Iterable[Impedance] = Impedance,
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
            >>> from cimsparql.model import get_cim_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_cim_model(server_url, "LATEST")
            >>> model.two_winding_transformers(limit=10)
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
            return self.client.query_with_header(query, limit)
        return self.get_table_and_convert(query, limit, index="mrid")

    def three_winding_transformers(
        self,
        region: Region = None,
        sub_region: bool = False,
        rates: Iterable[Rates] = (Rates.Normal,),
        network_analysis: bool = True,
        with_market: bool = False,
        impedance: Iterable[Impedance] = Impedance,
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
            >>> from cimsparql.model import get_cim_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_cim_model(server_url, "LATEST")
            >>> model.two_winding_transformers(limit=10)
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
            return self.client.query_with_header(query, limit)
        return self.get_table_and_convert(query, limit, index="mrid")

    def substation_voltage_level(
        self, limit: Optional[int] = None, dry_run: bool = False
    ) -> Union[pd.DataFrame, str]:
        query = queries.substation_voltage_level()
        if dry_run:
            return self.client.query_with_header(query, limit)
        return self.get_table_and_convert(query, limit)

    def delta_node_power(
        self, node: str = "node", limit: Optional[int] = None, dry_run: bool = False
    ):
        query = queries.node_delta_power(node, self.cim_version, "?delta_p")
        if dry_run:
            return self.client.query_with_header(query, limit)
        return self.get_table_and_convert(query, limit, index="mrid")

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
            return self.client.query_with_header(query, limit)
        return self.get_table_and_convert(query, limit, index)

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
            return self.client.query_with_header(query, limit)
        return self.get_table_and_convert(query, limit, index="mrid")

    def ssh_load(
        self,
        rdf_types: Iterable[LoadTypes] = LoadTypes,
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> Union[pd.DataFrame, str]:
        """Query load data from ssh profile (not available in GraphDB)

        Args:
           rdf_types: allowed ["cim:ConformLoad", "cim:NonConformLoad"]
           limit: return first 'limit' number of rows
           dry_run: return string with sql query

        """
        query = ssh_queries.load(rdf_types)
        if dry_run:
            return self.client.query_with_header(query, limit)
        return self.get_table_and_convert(query, limit, index="mrid")

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
            rdf_types = ["cim:GeneratingUnit"] + [
                f"cim:{unit}GeneratingUnit" for unit in GeneratorTypes
            ]
        query = ssh_queries.generating_unit(rdf_types)
        if dry_run:
            return self.client.query_with_header(query, limit)
        return self.get_table_and_convert(query, limit, index="mrid")

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
            return self.client.query_with_header(query, limit)
        return self.get_table_and_convert(query, limit, index="mrid")

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
            return self.client.query_with_header(query, limit)
        return self.get_table_and_convert(query, limit, index="mrid")

    def powerflow(
        self, power: Iterable[Power] = Power, limit: Optional[int] = None, dry_run: bool = False
    ) -> Union[pd.DataFrame, str]:
        """Query powerflow from sv profile (not available in GraphDB)

        Args:
           power: Allowed ['p','q']
           limit: return first 'limit' number of rows
           dry_run: return string with sql query
        """
        query = sv_queries.powerflow(power)
        if dry_run:
            return self.client.query_with_header(query, limit)
        return self.get_table_and_convert(query, limit, index="mrid")

    def branch_flow(
        self, power: Iterable[Power] = Power, limit: Optional[int] = None, dry_run: bool = False
    ) -> Union[pd.DataFrame, pd.Series, str]:
        """Query branch flow from sv profile.

        Args:
           limit: return first 'limit' number of rows
           dry_run: return string with sql query
        """
        query = sv_queries.branch_flow(self.cim_version, power)
        if dry_run:
            return self.client.query_with_header(query, limit)
        return self.get_table_and_convert(query, limit, index="mrid")

    def voltage(
        self,
        voltage_vars: Iterable[Voltage] = Voltage,
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> Union[pd.DataFrame, str]:
        """Query voltage from sv profile (not available in GraphDB).

        Args:
           voltage_vars: allowed ["v", "angle"]
           limit: return first 'limit' number of rows
           dry_run: return string with sql query
        """
        query = sv_queries.voltage(voltage_vars)
        if dry_run:
            return self.client.query_with_header(query, limit)
        return self.get_table_and_convert(query, limit, index="mrid")

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
            return self.client.query_with_header(query, limit)
        return self.get_table_and_convert(query, limit, index="mrid")

    @property
    def regions(self) -> pd.DataFrame:
        """Query regions

        Property:
           regions: List of regions in database

        Example:
            >>> from cimsparql.model import get_cim_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_cim_model(server_url, "LATEST")
            >>> model.regions
        """
        # TODO: Probably deprecate this property in the future. But keep for now.
        # We need to find a solution for custom namespaces in queries
        return self.get_regions()

    def get_regions(self, with_sn_short_name: bool = True):
        query = queries.regions_query(with_sn_short_name)
        return self.get_table_and_convert(query, index="mrid")

    def add_mrid(
        self, rdf_type: str, graph: Optional[str] = "?g", replace: Optional[Tuple[str, str]] = None
    ):
        """
        Add cim:IdentifiedObject.mRID to all records. It is copied from rdf:about (or rdf:ID) if
        replace is not specified

        Args:
            graph: Name of graph where mrids should be added. Note, mrid is added to all objects
                in the graph.
            rdf_type: RDF type where ID should be added
            replace: Tuple with from/to replacements. Example: if the mrid is given by rdf:about
                (or rdf:ID) where uuid is replace by an empty string pass ("uuid", "") as
                replace argument
        """

        from_str, to_str = replace or ("", "")
        query = (
            f"INSERT {{GRAPH {graph} {{?s cim:IdentifiedObject.mRID ?mrid}}}} "
            f"WHERE {{?s rdf:type {rdf_type}\n"
            f'BIND(replace(str(?s), "{from_str}", "{to_str}") as ?mrid)}}'
        )
        query = self.client.query_with_header(query)
        self.client.update_query(query)


def get_cim_model(
    server: str,
    graphdb_repo: str,
    graphdb_path: str = "services/pgm/equipment/",
    protocol: str = "https",
) -> CimModel:
    """Get a CIM Model

    Args:
        server: graphdb server
        graphdb_repo: query this repo
        graphdb_path: Prepend with this path when graphdb_repo is LATEST
        protocol: https or http
    """
    graphdb_path = graphdb_path if graphdb_repo == "LATEST" else ""
    service_cfg = ServiceConfig(graphdb_repo, protocol, server, graphdb_path)
    client = GraphDBClient(service_cfg)
    mapper = TypeMapper(client)
    return CimModel(mapper, client)
