"""The cimsparql.model module contains the base class CimModel."""

from dataclasses import dataclass
from functools import cached_property
from string import Template
from typing import Dict, Optional, Union

import pandas as pd

from cimsparql import templates
from cimsparql.graphdb import GraphDBClient, ServiceConfig
from cimsparql.type_mapper import TypeMapper


@dataclass
class ModelConfig:
    system_state_repo: Optional[str] = None
    ssh_graph: str = "?ssh_graph"


class Model:
    def __init__(
        self,
        mapper: Optional[TypeMapper],
        client: GraphDBClient,
        config: Optional[ModelConfig] = None,
    ) -> None:
        self._mapper = mapper
        self.client = client
        self.config = config or ModelConfig()

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
        add_prefixes: bool = True,
    ) -> pd.DataFrame:
        result, data_row = self.client.get_table(query, limit, add_prefixes)

        if self.mapper is not None:
            col_map = self.col_map(data_row, columns)
            result = self.mapper.map_data_types(result, col_map)

        if index and not result.empty:
            return result.set_index(index)
        return result

    @property
    def state_repo(self) -> str:
        return self.config.system_state_repo or self.client.service_cfg.url

    def template_to_query(
        self, template: Template, substitutes: Optional[Dict[str, str]] = None
    ) -> str:
        """Convert provided template to query."""
        if substitutes is None:
            substitutes = {}

        return template.safe_substitute(
            {"repo": self.state_repo} | substitutes | self.client.prefixes.ns
        )


class CimModel(Model):
    """Used to query with sparql queries (typically CIM)."""

    @property
    def cim_version(self) -> int:
        return self.client.prefixes.cim_version

    @property
    def full_model_query(self) -> str:
        return self.template_to_query(templates.FULL_MODEL_QUERY)

    @cached_property
    def full_model(self) -> pd.DataFrame:
        """Return all models where all depencies has been created and is available

        All profiles EQ/SSH/TP/SV will define a md:FullModel with possible dependencies. One profile
        could be dependent on more than one other. This function will return the models for SSH/TP
        and SV that is available from current repo or provided <repo>.

        Example:
        >> model.full_model()
        """
        return self.get_table_and_convert(self.full_model_query, index="model", add_prefixes=False)

    @property
    def market_dates_query(self) -> str:
        """Market activation date for this repository."""
        return self.template_to_query(templates.MARKET_DATES_QUERY)

    @cached_property
    def market_dates(self) -> pd.DataFrame:
        return self.get_table_and_convert(self.market_dates_query, index="mrid", add_prefixes=False)

    def bus_data_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.BUS_DATA_QUERY, substitutes)

    def three_winding_dummy_nodes_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.THREE_WINDING_DUMMY_NODES_QUERY, substitutes)

    def bus_data(self, region: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """Query name of topological nodes (TP query).

        Args:
           region: Limit to region (use None to get all)
           limit: return first 'limit' number of rows
        """
        return pd.concat(
            [
                self.get_table_and_convert(query(region), limit, index="node", add_prefixes=False)
                for query in [self.bus_data_query, self.three_winding_dummy_nodes_query]
            ]
        )

    def loads_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*", "ssh_graph": self.config.ssh_graph}
        return self.template_to_query(templates.LOADS_QUERY, substitutes)

    def loads(self, region: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """Query load data.

        Args:
           region: regexp that limits to region
           limit: return first 'limit' number of rows
        """
        query = self.loads_query(region)
        return self.get_table_and_convert(query, limit, index="mrid", add_prefixes=False)

    def wind_generating_units_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.WIND_GENERATING_UNITS_QUERY, substitutes)

    def wind_generating_units(
        self, region: Optional[str] = None, limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Query wind generating units.

        Args:
           region:
           limit: return first 'limit' number of rows

        Example:
            >>> from cimsparql.model import get_cim_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_cim_model(server_url, "LATEST")
            >>> model.wind_generating_units(limit=10)

        """
        query = self.wind_generating_units_query(region)
        return self.get_table_and_convert(query, limit, index="mrid", add_prefixes=False)

    def synchronous_machines_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*", "ssh_graph": self.config.ssh_graph}
        return self.template_to_query(templates.SYNCHRONOUS_MACHINES_QUERY, substitutes)

    def synchronous_machines(
        self, region: Optional[str] = None, limit: Optional[int] = None
    ) -> pd.DataFrame:
        query = self.synchronous_machines_query(region)
        return self.get_table_and_convert(query, limit, index="mrid", add_prefixes=False)

    def connections_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.CONNECTIONS_QUERY, substitutes)

    def connections(
        self, region: Optional[str] = None, limit: Optional[int] = None
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
            >>> from cimsparql.model import get_cim_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_cim_model(server_url, "LATEST")
            >>> model.connections(limit=10)
        """
        query = self.connections_query(region)
        return self.get_table_and_convert(query, limit, index="mrid", add_prefixes=False)

    def borders_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.BORDERS_QUERY, substitutes)

    def borders(self, region: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """Retrieve ACLineSegments where one terminal is inside and the other is outside the region

        Args:
            region: Inside area
            limit: return first 'limit' number of rows
        """
        query = self.borders_query(region)
        return self.get_table_and_convert(query, limit, index="mrid", add_prefixes=False)

    def exchange_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.EXCHANGE_QUERY, substitutes)

    def exchange(self, region: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """Retrieve ACLineSegments where one terminal is inside and the other is outside the region.

        Args:
            region: Inside area
            limit: return first 'limit' number of rows
        """

        if region is None:
            return pd.DataFrame([], columns=["name", "node", "status", "p", "market_code"])
        query = self.exchange_query(region)
        return self.get_table_and_convert(query, limit, index="mrid", add_prefixes=False)

    def converters_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*", "ssh_graph": self.config.ssh_graph}
        return self.template_to_query(templates.CONVERTERS_QUERY, substitutes)

    def converters(self, region: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        query = self.converters_query(region)
        return self.get_table_and_convert(query, limit, index="mrid", add_prefixes=False)

    def transformers_connected_to_converter_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(
            templates.TRANSFORMERS_CONNECTED_TO_CONVERTER_QUERY, substitutes
        )

    def transformers_connected_to_converter(
        self, region: Optional[str] = None, limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Query list of transformer connected at a converter (Voltage source or DC)

        Args:
           region: Limit to region

        """
        query = self.transformers_connected_to_converter_query(region)
        return self.get_table_and_convert(query, limit, index="mrid", add_prefixes=False)

    def ac_lines_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.AC_LINE_QUERY, substitutes)

    def ac_lines(self, region: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """Query ac line segments

        Args:
           region: Limit to region
           limit: return first 'limit' number of rows

        Example:
            >>> from cimsparql.model import get_cim_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_cim_model(server_url, "LATEST")
            >>> model.ac_lines(limit=10)
        """
        query = self.ac_lines_query(region)
        return self.get_table_and_convert(query, limit, index="mrid", add_prefixes=False)

    def series_compensators_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.SERIES_COMPENSATORS_QUERY, substitutes)

    def series_compensators(
        self, region: Optional[str] = None, limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Query series compensators

        Args:
           region: Limit to region
           sub_region: Assume region is a sub_region
           limit: return first 'limit' number of rows
        """
        query = self.series_compensators_query(region)
        return self.get_table_and_convert(query, limit, index="mrid", add_prefixes=False)

    def transformers_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.TRANSFORMERS_QUERY, substitutes)

    def transformers(
        self, region: Optional[str] = None, limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Query transformer windings.

        Args:
           region: Limit to region
           sub_region: Assume region is a sub_region
           limit: return first 'limit' number of rows
        """
        query = self.transformers_query(region)
        return self.get_table_and_convert(query, limit, add_prefixes=False)

    def two_winding_transformers_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.TWO_WINDING_QUERY, substitutes)

    def two_winding_angle_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.TWO_WINDING_ANGLE_QUERY, substitutes)

    def two_winding_transformers(
        self, region: Optional[str] = None, limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Query two-winding transformer.

        Args:
           region: Limit to region
           limit: return first 'limit' number of rows

        Example:
            >>> from cimsparql.model import get_cim_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_cim_model(server_url, "LATEST")
            >>> model.two_winding_transformers(limit=10)
        """
        query = self.two_winding_transformers_query(region)
        query_angle = self.two_winding_angle_query(region)
        data = self.get_table_and_convert(query, limit, index="mrid", add_prefixes=False)
        angle = self.get_table_and_convert(query_angle, limit, index="mrid", add_prefixes=False)
        if not angle.empty:
            data["angle"] += angle.reindex(index=data.index, fill_value=0.0).squeeze()
        return data

    def three_winding_loss_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.THREE_WINDING_LOSS_QUERY, substitutes)

    def three_winding_transformers_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.THREE_WINDING_QUERY, substitutes)

    def three_winding_transformers(
        self, region: Optional[str] = None, limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Query three-winding transformer. Return as three two-winding transformers.

        Args:
           region: Limit to region
           limit: return first 'limit' number of rows
           dry_run: return string with sql query

        Example:
            >>> from cimsparql.model import get_cim_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_cim_model(server_url, "LATEST")
            >>> model.two_winding_transformers(limit=10)
        """
        query = self.three_winding_transformers_query(region)
        data = self.get_table_and_convert(query, limit, index="mrid", add_prefixes=False)
        query_loss = self.three_winding_loss_query(region)
        loss = self.get_table_and_convert(query_loss, index="mrid", add_prefixes=False)
        return pd.concat([data.assign(pl_1=0.0), loss.loc[data.index]], axis=1)

    def substation_voltage_level_query(self) -> str:
        return self.template_to_query(templates.SUBSTATION_VOLTAGE_LEVEL_QUERY)

    def substation_voltage_level(self, limit: Optional[int] = None) -> pd.DataFrame:
        query = self.substation_voltage_level_query()
        return self.get_table_and_convert(query, limit, index="substation", add_prefixes=False)

    def disconnected_query(self) -> str:
        return self.template_to_query(templates.DISCONNECTED_QUERY)

    def disconnected(self, limit: Optional[int] = None) -> pd.DataFrame:
        """Query disconneced status from ssh profile (not available in GraphDB)

        Args:
           index: Column to use as index
           limit: return first 'limit' number of rows
           dry_run: return string with sql query
        """
        query = self.disconnected_query()
        return self.get_table_and_convert(query, limit, add_prefixes=False)

    @property
    def powerflow_query(self) -> str:
        return self.template_to_query(templates.POWER_FLOW_QUERY)

    def powerflow(self, limit: Optional[int] = None) -> pd.DataFrame:
        """Query powerflow from sv profile (not available in GraphDB)

        Args:
           limit: return first 'limit' number of rows
        """
        return self.get_table_and_convert(
            self.powerflow_query, limit, index="mrid", add_prefixes=False
        )

    def branch_node_withdraw_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.BRANCH_NODE_WITHDRAW_QUERY, substitutes)

    def branch_node_withdraw(
        self, region: Optional[str] = None, limit: Optional[int] = None
    ) -> Union[pd.DataFrame, pd.Series]:
        """Query branch flow from sv profile.

        Args:
           limit: return first 'limit' number of rows
           dry_run: return string with sql query
        """
        query = self.branch_node_withdraw_query(region)
        return self.get_table_and_convert(query, limit, index="mrid", add_prefixes=False)

    def dc_active_flow_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.DC_ACTIVE_POWER_FLOW_QUERY, substitutes)

    def dc_active_flow(
        self, region: Optional[str] = None, limit: Optional[int] = None
    ) -> pd.DataFrame:
        query = self.dc_active_flow_query(region)
        data = self.get_table_and_convert(query, limit, add_prefixes=False)
        # Unable to group on max within the sparql query so we do it here.
        data = data.iloc[data.groupby("mrid")["p"].idxmax()].set_index("mrid")
        return data.eval("p * direction").rename("p")

    @property
    def regions_query(self) -> str:
        return self.template_to_query(templates.REGIONS_QUERY)

    @cached_property
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
        return self.get_table_and_convert(self.regions_query, index="mrid", add_prefixes=False)

    def add_mrid_query(self, rdf_type: Optional[str] = None, graph: Optional[str] = None) -> str:
        substitutes = {"rdf_type": rdf_type or "?rdf_type", "g": graph or "?g"}
        return self.template_to_query(templates.ADD_MRID_QUERY, substitutes)

    def add_mrid(self, rdf_type: Optional[str] = None, graph: Optional[str] = None) -> None:
        """
        Add cim:IdentifiedObject.mRID to all records. It is copied from rdf:about (or rdf:ID) if
        replace is not specified

        Args:
            graph: Name of graph where mrids should be added. Note, mrid is added to all objects
                in the graph.
            rdf_type: RDF type where ID should be added
        """
        self.client.update_query(self.add_mrid_query(rdf_type, graph))


def get_cim_model(
    service_cfg: Optional[ServiceConfig] = None, model_cfg: Optional[ModelConfig] = None
) -> CimModel:
    """Get a CIM Model."""
    client = GraphDBClient(service_cfg)
    mapper = TypeMapper(client)
    return CimModel(mapper, client, model_cfg)
