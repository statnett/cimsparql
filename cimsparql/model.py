"""The cimsparql.model module contains the base class CimModel."""

import re
from dataclasses import dataclass
from functools import cached_property
from string import Template
from typing import Dict, Optional

import pandas as pd

from cimsparql import templates
from cimsparql.data_models import (
    AcLinesSchema,
    BordersSchema,
    BranchComponentSchema,
    BranchWithdrawSchema,
    BusDataSchema,
    ConnectionsSchema,
    ConvertersSchema,
    DcActiveFlowSchema,
    DisconnectedSchema,
    ExchangeSchema,
    FullModelSchema,
    LoadsSchema,
    MarketDatesSchema,
    PowerFlowSchema,
    RegionsSchema,
    SubstationVoltageSchema,
    SynchronousMachinesSchema,
    TransfConToConverterSchema,
    TransformersSchema,
    TransformerWindingSchema,
    WindGeneratingUnitsSchema,
)
from cimsparql.graphdb import GraphDBClient, ServiceConfig
from cimsparql.type_mapper import TypeMapper


@dataclass
class ModelConfig:
    system_state_repo: Optional[str] = None
    ssh_graph: str = "?ssh_graph"


class Model:
    def __init__(
        self,
        client: GraphDBClient,
        config: Optional[ModelConfig] = None,
        mapper: Optional[TypeMapper] = None,
    ) -> None:
        self.client = client
        self.config = config or ModelConfig()
        self.mapper = mapper or TypeMapper(client)

    @staticmethod
    def _col_map(data_row) -> Dict[str, str]:
        return {
            column: data.get("datatype", data.get("type", None))
            for column, data in data_row.items()
        }

    @classmethod
    def col_map(cls, data_row, columns: Dict[str, str]) -> Dict[str, str]:
        columns = columns or {}
        col_map = cls._col_map(data_row)
        col_map.update(columns)
        return col_map

    @property
    def map_data_types(self) -> bool:
        pref = self.client.prefixes
        return "cim" in pref and self.mapper.have_cim_version(pref["cim"])

    def get_table_and_convert(
        self, query: str, index: Optional[str] = None, columns: Optional[Dict[str, str]] = None
    ) -> pd.DataFrame:
        result, data_row = self.client.get_table(query)

        col_map = self.col_map(data_row, columns)
        result = self.mapper.map_data_types(result, col_map)

        if index:
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
            {"repo": self.state_repo} | substitutes | self.client.prefixes
        )


class CimModel(Model):
    """Used to query with sparql queries (typically CIM)."""

    @cached_property
    def cim_version(self) -> int:
        return int(re.search("cim(\\d+)", self.client.prefixes["cim"]).group(1))

    @property
    def full_model_query(self) -> str:
        return self.template_to_query(templates.FULL_MODEL_QUERY)

    @cached_property
    def full_model(self) -> FullModelSchema:
        """Return all models where all depencies has been created and is available

        All profiles EQ/SSH/TP/SV will define a md:FullModel with possible dependencies. One profile
        could be dependent on more than one other. This function will return the models for SSH/TP
        and SV that is available from current repo or provided <repo>.

        Example:
        >> model.full_model()
        """
        df = self.get_table_and_convert(self.full_model_query)
        return FullModelSchema(df)

    @property
    def market_dates_query(self) -> str:
        """Market activation date for this repository."""
        return self.template_to_query(templates.MARKET_DATES_QUERY)

    @cached_property
    def market_dates(self) -> MarketDatesSchema:
        df = self.get_table_and_convert(self.market_dates_query, index="mrid")
        return MarketDatesSchema(df)

    def bus_data_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.BUS_DATA_QUERY, substitutes)

    def three_winding_dummy_nodes_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.THREE_WINDING_DUMMY_NODES_QUERY, substitutes)

    def bus_data(self, region: Optional[str] = None) -> BusDataSchema:
        """Query name of topological nodes (TP query).

        Args:
           region: Limit to region (use None to get all)
        """
        df = pd.concat(
            [
                self.get_table_and_convert(query(region), index="node")
                for query in [self.bus_data_query, self.three_winding_dummy_nodes_query]
            ]
        )
        return BusDataSchema(df)

    def loads_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*", "ssh_graph": self.config.ssh_graph}
        return self.template_to_query(templates.LOADS_QUERY, substitutes)

    def loads(self, region: Optional[str] = None) -> LoadsSchema:
        """Query load data.

        Args:
           region: regexp that limits to region
        """
        query = self.loads_query(region)
        df = self.get_table_and_convert(query, index="mrid")
        return LoadsSchema(df)

    def wind_generating_units_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.WIND_GENERATING_UNITS_QUERY, substitutes)

    def wind_generating_units(self, region: Optional[str] = None) -> WindGeneratingUnitsSchema:
        """Query wind generating units.

        Args:
           region:

        Example:
            >>> from cimsparql.model import get_cim_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_cim_model(server_url, "LATEST")
            >>> model.wind_generating_units()

        """
        query = self.wind_generating_units_query(region)
        df = self.get_table_and_convert(query, index="mrid")
        return WindGeneratingUnitsSchema(df)

    def synchronous_machines_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*", "ssh_graph": self.config.ssh_graph}
        return self.template_to_query(templates.SYNCHRONOUS_MACHINES_QUERY, substitutes)

    def synchronous_machines(self, region: Optional[str] = None) -> SynchronousMachinesSchema:
        query = self.synchronous_machines_query(region)
        df = self.get_table_and_convert(query, index="mrid")
        return SynchronousMachinesSchema(df)

    def connections_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.CONNECTIONS_QUERY, substitutes)

    def connections(self, region: Optional[str] = None) -> ConnectionsSchema:
        """Query connectors

        Args:
           rdf_types: Only cim:breaker and cim:Disconnector allowed
        Returns:

        Example:
           region: Limit to region

        Example:
            >>> from cimsparql.model import get_cim_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_cim_model(server_url, "LATEST")
            >>> model.connections()
        """
        query = self.connections_query(region)
        df = self.get_table_and_convert(query, index="mrid")
        return ConnectionsSchema(df)

    def borders_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.BORDERS_QUERY, substitutes)

    def borders(self, region: Optional[str] = None) -> BordersSchema:
        """Retrieve ACLineSegments where one terminal is inside and the other is outside the region

        Args:
            region: Inside area
            limit: return first 'limit' number of rows
        """
        query = self.borders_query(region)
        df = self.get_table_and_convert(query, index="mrid")
        return BordersSchema(df)

    def exchange_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.EXCHANGE_QUERY, substitutes)

    def exchange(self, region: Optional[str] = None) -> ExchangeSchema:
        """Retrieve ACLineSegments where one terminal is inside and the other is outside the region.

        Args:
            region: Inside area
        """

        if region is None:
            cols = ["name", "node", "status", "p", "market_code"]
            index = pd.Index([], name="mrid")
            df = pd.DataFrame([], columns=cols, index=index)
            return ExchangeSchema(df)
        query = self.exchange_query(region)
        df = self.get_table_and_convert(query, index="mrid")
        return ExchangeSchema(df)

    def converters_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*", "ssh_graph": self.config.ssh_graph}
        return self.template_to_query(templates.CONVERTERS_QUERY, substitutes)

    def converters(self, region: Optional[str] = None) -> ConvertersSchema:
        query = self.converters_query(region)
        df = self.get_table_and_convert(query, index="mrid")
        return ConvertersSchema(df)

    def transformers_connected_to_converter_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(
            templates.TRANSFORMERS_CONNECTED_TO_CONVERTER_QUERY, substitutes
        )

    def transformers_connected_to_converter(
        self, region: Optional[str] = None
    ) -> TransfConToConverterSchema:
        """Query list of transformer connected at a converter (Voltage source or DC)

        Args:
           region: Limit to region

        """
        query = self.transformers_connected_to_converter_query(region)
        df = self.get_table_and_convert(query, index="mrid")
        return TransfConToConverterSchema(df)

    def ac_lines_query(self, region: Optional[str] = None, rate: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*", "rate": rate or "Normal@20"}
        return self.template_to_query(templates.AC_LINE_QUERY, substitutes)

    def ac_lines(self, region: Optional[str] = None, rate: Optional[str] = None) -> AcLinesSchema:
        """Query ac line segments

        Args:
           region: Limit to region

        Example:
            >>> from cimsparql.model import get_cim_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_cim_model(server_url, "LATEST")
            >>> model.ac_lines()
        """
        query = self.ac_lines_query(region, rate)
        df = self.get_table_and_convert(query, index="mrid")
        return AcLinesSchema(df)

    def series_compensators_query(
        self, region: Optional[str] = None, rate: Optional[str] = None
    ) -> str:
        substitutes = {"region": region or ".*", "rate": rate or "Normal@20"}
        return self.template_to_query(templates.SERIES_COMPENSATORS_QUERY, substitutes)

    def series_compensators(
        self, region: Optional[str] = None, rate: Optional[str] = None
    ) -> BranchComponentSchema:
        """Query series compensators

        Args:
           region: Limit to region
        """
        query = self.series_compensators_query(region, rate)
        df = self.get_table_and_convert(query, index="mrid")
        return BranchComponentSchema(df)

    def transformers_query(self, region: Optional[str] = None, rate: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*", "rate": rate or "Normal@20"}
        return self.template_to_query(templates.TRANSFORMERS_QUERY, substitutes)

    def transformers(
        self, region: Optional[str] = None, rate: Optional[str] = None
    ) -> TransformersSchema:
        """Query transformer windings.

        Args:
           region: Limit to region
        """
        query = self.transformers_query(region, rate)
        df = self.get_table_and_convert(query)
        return TransformersSchema(df)

    def two_winding_transformers_query(
        self, region: Optional[str] = None, rate: Optional[str] = None
    ) -> str:
        substitutes = {"region": region or ".*", "rate": rate or "Normal@20"}
        return self.template_to_query(templates.TWO_WINDING_QUERY, substitutes)

    def two_winding_angle_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.TWO_WINDING_ANGLE_QUERY, substitutes)

    def two_winding_transformers(
        self, region: Optional[str] = None, rate: Optional[str] = None
    ) -> TransformerWindingSchema:
        """Query two-winding transformer.

        Args:
           region: Limit to region

        Example:
            >>> from cimsparql.model import get_cim_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_cim_model(server_url, "LATEST")
            >>> model.two_winding_transformers()
        """
        query = self.two_winding_transformers_query(region, rate)
        query_angle = self.two_winding_angle_query(region)
        data = self.get_table_and_convert(query, index="mrid")
        angle = self.get_table_and_convert(query_angle, index="mrid")
        if not angle.empty:
            data["angle"] += angle.reindex(index=data.index, fill_value=0.0).squeeze()
        return TransformerWindingSchema(data)

    def three_winding_loss_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.THREE_WINDING_LOSS_QUERY, substitutes)

    def three_winding_transformers_query(
        self, region: Optional[str] = None, rate: Optional[str] = None
    ) -> str:
        substitutes = {"region": region or ".*", "rate": rate or "Normal@20"}
        return self.template_to_query(templates.THREE_WINDING_QUERY, substitutes)

    def three_winding_transformers(
        self, region: Optional[str] = None, rate: Optional[str] = None
    ) -> TransformerWindingSchema:
        """Query three-winding transformer. Return as three two-winding transformers.

        Example:
            >>> from cimsparql.model import get_cim_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_cim_model(server_url, "LATEST")
            >>> model.two_winding_transformers()
        """
        query = self.three_winding_transformers_query(region, rate)
        data = self.get_table_and_convert(query, index="mrid")
        query_loss = self.three_winding_loss_query(region)
        loss = self.get_table_and_convert(query_loss, index="mrid")
        df = pd.concat([data.assign(ploss_1=0.0), loss.loc[data.index]], axis=1)
        return TransformerWindingSchema(df)

    @property
    def substation_voltage_level_query(self) -> str:
        return self.template_to_query(templates.SUBSTATION_VOLTAGE_LEVEL_QUERY)

    @cached_property
    def substation_voltage_level(self) -> SubstationVoltageSchema:
        query = self.substation_voltage_level_query
        df = self.get_table_and_convert(query, index="substation")
        return SubstationVoltageSchema(df)

    @property
    def disconnected_query(self) -> str:
        return self.template_to_query(templates.DISCONNECTED_QUERY)

    @cached_property
    def disconnected(self) -> DisconnectedSchema:
        """Query disconneced status from ssh profile (not available in GraphDB)."""
        df = self.get_table_and_convert(self.disconnected_query)
        return DisconnectedSchema(df)

    @property
    def powerflow_query(self) -> str:
        return self.template_to_query(templates.POWER_FLOW_QUERY)

    @cached_property
    def powerflow(self) -> PowerFlowSchema:
        """Query powerflow from sv profile (not available in GraphDB)."""
        df = self.get_table_and_convert(self.powerflow_query, index="mrid")
        return PowerFlowSchema(df)

    def branch_node_withdraw_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.BRANCH_NODE_WITHDRAW_QUERY, substitutes)

    def branch_node_withdraw(self, region: Optional[str] = None) -> BranchWithdrawSchema:
        """Query branch flow from sv profile."""
        query = self.branch_node_withdraw_query(region)
        df = self.get_table_and_convert(query, index="mrid")
        return BranchWithdrawSchema(df)

    def dc_active_flow_query(self, region: Optional[str] = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.DC_ACTIVE_POWER_FLOW_QUERY, substitutes)

    def dc_active_flow(self, region: Optional[str] = None) -> DcActiveFlowSchema:
        query = self.dc_active_flow_query(region)
        data = self.get_table_and_convert(query)
        # Unable to group on max within the sparql query so we do it here.
        data = data.iloc[data.groupby("mrid")["p"].idxmax()].set_index("mrid")
        df = data.eval("p * direction").rename("p")
        return DcActiveFlowSchema(df.to_frame())

    @property
    def regions_query(self) -> str:
        return self.template_to_query(templates.REGIONS_QUERY)

    @cached_property
    def regions(self) -> RegionsSchema:
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
        df = self.get_table_and_convert(self.regions_query, index="mrid")
        return RegionsSchema(df)

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
    return CimModel(client, model_cfg)
