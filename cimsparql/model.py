"""The cimsparql.model module contains the base class Model."""
from __future__ import annotations

import asyncio
import re
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from functools import cached_property
from string import Template
from typing import TYPE_CHECKING

import pandas as pd

from cimsparql import templates
from cimsparql.data_models import (
    AcLinesDataFrame,
    BordersDataFrame,
    BranchComponentDataFrame,
    BranchWithdrawDataFrame,
    BusDataFrame,
    ConnectionsDataFrame,
    ConnectivityNodeDataFrame,
    ConvertersDataFrame,
    CoordinatesDataFrame,
    DcActiveFlowDataFrame,
    DisconnectedDataFrame,
    ExchangeDataFrame,
    FullModelDataFrame,
    HVDCBidzonesDataFrame,
    LoadsDataFrame,
    MarketDatesDataFrame,
    PowerFlowDataFrame,
    RASEquipmentDataFrame,
    RegionsDataFrame,
    StationGroupCodeNameDataFrame,
    SubstationVoltageDataFrame,
    SvInjectionDataFrame,
    SwitchesDataFrame,
    SynchronousMachinesDataFrame,
    TransfConToConverterDataFrame,
    TransformersDataFrame,
    TransformerWindingDataFrame,
    TransformerWindingsDataFrame,
    WindGeneratingUnitsDataFrame,
)
from cimsparql.graphdb import AsyncGraphDBClient, GraphDBClient, ServiceConfig
from cimsparql.type_mapper import TypeMapper

if TYPE_CHECKING:
    from cimsparql.value_mapper import ValueMapper


@dataclass
class ModelConfig:
    system_state_repo: str | None = None
    eq_repo: str | None = None
    value_mappers: Iterable[ValueMapper] = field(default_factory=list)


class Model:
    def __init__(
        self,
        clients: dict[str, GraphDBClient],
        config: ModelConfig | None = None,
        mapper: TypeMapper | None = None,
    ) -> None:
        self.clients = clients
        self.config = config or ModelConfig()
        self.mapper = mapper or TypeMapper(self.get_client("Type mapper").service_cfg)

    @property
    def distinct_clients(self) -> list[GraphDBClient]:
        obj_ids = set()
        distinct = []
        for client in self.clients.values():
            if id(client) not in obj_ids:
                obj_ids.add(id(client))
                distinct.append(client)
        return distinct

    def get_client(self, query_name: str) -> GraphDBClient:
        """
        Return the corret graph db client to execute a query. By default
        there is only one client so the same client is returned in all cases
        """
        return self.clients[query_name]

    @property
    def client(self) -> GraphDBClient:
        return self.get_client("default")

    @client.setter
    def client(self, default_client: GraphDBClient) -> None:
        self.clients["default"] = default_client

    @staticmethod
    def _col_map(data_row: dict[str, str]) -> dict[str, str]:
        return {
            column: data.datatype if data.datatype else data.value_type
            for column, data in data_row.items()
        }

    @classmethod
    def col_map(cls, data_row: dict[str, str], columns: dict[str, str]) -> dict[str, str]:
        columns = columns or {}
        col_map = cls._col_map(data_row)
        col_map.update(columns)
        return col_map

    @property
    def map_data_types(self) -> bool:
        pref = self.client.prefixes
        return "cim" in pref and self.mapper.have_cim_version(pref["cim"])

    def _convert_result(
        self,
        result: pd.DataFrame,
        data_row: list[dict],
        index: str | None = None,
        columns: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        col_map = self.col_map(data_row, columns)
        result = self.mapper.map_data_types(result, col_map)
        for v_mapper in self.config.value_mappers:
            result = v_mapper.map(result)

        if index:
            return result.set_index(index)
        return result

    async def get_table_and_convert(
        self, query: str, index: str | None = None, columns: dict[str, str] | None = None
    ) -> pd.DataFrame:
        name = query_name(query)
        client = self.get_client(name)
        if isinstance(client, AsyncGraphDBClient):
            result, data_row = await client.get_table(query)
        else:
            result, data_row = client.get_table(query)
        return self._convert_result(result, data_row, index, columns)

    def template_to_query(
        self, template: Template, substitutes: dict[str, str] | None = None
    ) -> str:
        """Convert provided template to query."""
        substitutes = substitutes or {}

        # Extract name from the query. We don't need to perform actual substitutions to do this,
        # we just let placeholder be left
        name = query_name(template.safe_substitute())
        client = self.get_client(name)
        state_repo = self.config.system_state_repo or client.service_cfg.url
        eq_repo = self.config.eq_repo or client.service_cfg.url

        return template.safe_substitute(
            {"repo": state_repo, "eq_repo": eq_repo} | substitutes | self.client.prefixes
        )

    @cached_property
    def cim_version(self) -> int:
        return int(re.search("cim(\\d+)", self.client.prefixes["cim"]).group(1))

    @property
    def full_model_query(self) -> str:
        return self.template_to_query(templates.FULL_MODEL_QUERY)

    async def full_model(self) -> FullModelDataFrame:
        """Return all models where all depencies has been created and is available

        All profiles EQ/SSH/TP/SV will define a md:FullModel with possible dependencies. One profile
        could be dependent on more than one other. This function will return the models for SSH/TP
        and SV that is available from current repo or provided <repo>.

        Example:
        >> model.full_model()
        """
        df = await self.get_table_and_convert(self.full_model_query)
        return FullModelDataFrame(df)

    @property
    def market_dates_query(self) -> str:
        """Market activation date for this repository."""
        return self.template_to_query(templates.MARKET_DATES_QUERY)

    async def market_dates(self) -> MarketDatesDataFrame:
        df = await self.get_table_and_convert(self.market_dates_query, index="mrid")
        return MarketDatesDataFrame(df)

    def bus_data_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.BUS_DATA_QUERY, substitutes)

    def three_winding_dummy_nodes_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.THREE_WINDING_DUMMY_NODES_QUERY, substitutes)

    async def bus_data(self, region: str | None = None) -> BusDataFrame:
        """Query name of topological nodes (TP query).

        Args:
           region: Limit to region (use None to get all)
        """
        dfs = await asyncio.gather(
            self.get_table_and_convert(self.bus_data_query(region), index="node"),
            self.get_table_and_convert(self.three_winding_dummy_nodes_query(region), index="node"),
        )
        df = pd.concat(dfs)
        return BusDataFrame(df)

    def loads_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.LOADS_QUERY, substitutes)

    async def loads(self, region: str | None = None) -> LoadsDataFrame:
        """Query load data.

        Args:
           region: regexp that limits to region
        """
        query = self.loads_query(region)
        df = await self.get_table_and_convert(query, index="mrid")
        return LoadsDataFrame(df)

    def wind_generating_units_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.WIND_GENERATING_UNITS_QUERY, substitutes)

    async def wind_generating_units(
        self, region: str | None = None
    ) -> WindGeneratingUnitsDataFrame:
        """Query wind generating units.

        Args:
           region:

        Example:
            >>> from cimsparql.model import get_single_client_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_single_client_model(server_url, "LATEST")
            >>> model.wind_generating_units()

        """
        query = self.wind_generating_units_query(region)
        df = await self.get_table_and_convert(query, index="mrid")
        return WindGeneratingUnitsDataFrame(df)

    def synchronous_machines_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.SYNCHRONOUS_MACHINES_QUERY, substitutes)

    async def synchronous_machines(self, region: str | None = None) -> SynchronousMachinesDataFrame:
        query = self.synchronous_machines_query(region)
        df = await self.get_table_and_convert(query, index="mrid")
        return SynchronousMachinesDataFrame(df)

    def connections_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.CONNECTIONS_QUERY, substitutes)

    async def connections(self, region: str | None = None) -> ConnectionsDataFrame:
        """Query connectors

        Args:
           rdf_types: Only cim:breaker and cim:Disconnector allowed
        Returns:

        Example:
           region: Limit to region

        Example:
            >>> from cimsparql.model import get_single_client_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_single_client_model(server_url, "LATEST")
            >>> model.connections()
        """
        query = self.connections_query(region)
        df = await self.get_table_and_convert(query, index="mrid")
        return ConnectionsDataFrame(df)

    def borders_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.BORDERS_QUERY, substitutes)

    async def borders(self, region: str | None = None) -> BordersDataFrame:
        """Retrieve ACLineSegments where one terminal is inside and the other is outside the region

        Args:
            region: Inside area
            limit: return first 'limit' number of rows
        """
        query = self.borders_query(region)
        df = await self.get_table_and_convert(query, index="mrid")
        return BordersDataFrame(df)

    def exchange_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.EXCHANGE_QUERY, substitutes)

    async def exchange(self, region: str | None = None) -> ExchangeDataFrame:
        """Retrieve ACLineSegments where one terminal is inside and the other is outside the region.

        Args:
            region: Inside area
        """

        if region is None:
            cols = ["name", "node", "status", "p", "market_code"]
            index = pd.Index([], name="mrid")
            df = pd.DataFrame([], columns=cols, index=index)
            return ExchangeDataFrame(df)
        query = self.exchange_query(region)
        df = await self.get_table_and_convert(query, index="mrid")
        return ExchangeDataFrame(df)

    def converters_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.CONVERTERS_QUERY, substitutes)

    async def converters(self, region: str | None = None) -> ConvertersDataFrame:
        query = self.converters_query(region)
        df = await self.get_table_and_convert(query, index="mrid")
        return ConvertersDataFrame(df)

    def transformers_connected_to_converter_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(
            templates.TRANSFORMERS_CONNECTED_TO_CONVERTER_QUERY, substitutes
        )

    async def transformers_connected_to_converter(
        self, region: str | None = None
    ) -> TransfConToConverterDataFrame:
        """Query list of transformer connected at a converter (Voltage source or DC)

        Args:
           region: Limit to region

        """
        query = self.transformers_connected_to_converter_query(region)
        df = await self.get_table_and_convert(query, index="mrid")
        return TransfConToConverterDataFrame(df)

    def ac_lines_query(self, region: str | None = None, rate: str | None = None) -> str:
        substitutes = {"region": region or ".*", "rate": rate or "Normal@20"}
        return self.template_to_query(templates.AC_LINE_QUERY, substitutes)

    async def ac_lines(
        self, region: str | None = None, rate: str | None = None
    ) -> AcLinesDataFrame:
        """Query ac line segments

        Args:
           region: Limit to region

        Example:
            >>> from cimsparql.model import get_single_client_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_single_client_model(server_url, "LATEST")
            >>> model.ac_lines()
        """
        query = self.ac_lines_query(region, rate)
        df = await self.get_table_and_convert(query, index="mrid")
        return AcLinesDataFrame(df)

    def series_compensators_query(self, region: str | None = None, rate: str | None = None) -> str:
        substitutes = {"region": region or ".*", "rate": rate or "Normal@20"}
        return self.template_to_query(templates.SERIES_COMPENSATORS_QUERY, substitutes)

    async def series_compensators(
        self, region: str | None = None, rate: str | None = None
    ) -> BranchComponentDataFrame:
        """Query series compensators

        Args:
           region: Limit to region
        """
        query = self.series_compensators_query(region, rate)
        df = await self.get_table_and_convert(query, index="mrid")
        return BranchComponentDataFrame(df)

    def transformers_query(self, region: str | None = None, rate: str | None = None) -> str:
        substitutes = {"region": region or ".*", "rate": rate or "Normal@20"}
        return self.template_to_query(templates.TRANSFORMERS_QUERY, substitutes)

    async def transformers(
        self, region: str | None = None, rate: str | None = None
    ) -> TransformersDataFrame:
        """Query transformer windings.

        Args:
           region: Limit to region
        """
        query = self.transformers_query(region, rate)
        df = await self.get_table_and_convert(query)
        return TransformersDataFrame(df)

    def two_winding_transformers_query(
        self, region: str | None = None, rate: str | None = None
    ) -> str:
        substitutes = {"region": region or ".*", "rate": rate or "Normal@20"}
        return self.template_to_query(templates.TWO_WINDING_QUERY, substitutes)

    def two_winding_angle_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.TWO_WINDING_ANGLE_QUERY, substitutes)

    async def two_winding_transformers(
        self, region: str | None = None, rate: str | None = None
    ) -> TransformerWindingDataFrame:
        """Query two-winding transformer.

        Args:
           region: Limit to region

        Example:
            >>> from cimsparql.model import get_single_client_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_single_client_model(server_url, "LATEST")
            >>> model.two_winding_transformers()
        """
        query = self.two_winding_transformers_query(region, rate)
        query_angle = self.two_winding_angle_query(region)
        data = await self.get_table_and_convert(query, index="mrid")
        angle = await self.get_table_and_convert(query_angle, index="mrid")
        if not angle.empty:
            data["angle"] += angle.reindex(index=data.index, fill_value=0.0).squeeze()
        return TransformerWindingDataFrame(data)

    @property
    def winding_query(self) -> str:
        return self.template_to_query(templates.WINDING)

    def three_winding_loss_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.THREE_WINDING_LOSS_QUERY, substitutes)

    def three_winding_transformers_query(
        self, region: str | None = None, rate: str | None = None
    ) -> str:
        substitutes = {"region": region or ".*", "rate": rate or "Normal@20"}
        return self.template_to_query(templates.THREE_WINDING_QUERY, substitutes)

    async def three_winding_transformers(
        self, region: str | None = None, rate: str | None = None
    ) -> TransformerWindingDataFrame:
        """Query three-winding transformer. Return as three two-winding transformers.

        Example:
            >>> from cimsparql.model import get_single_client_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_single_client_model(server_url, "LATEST")
            >>> model.two_winding_transformers()
        """
        query = self.three_winding_transformers_query(region, rate)
        data = await self.get_table_and_convert(query, index="mrid")
        query_loss = self.three_winding_loss_query(region)
        loss = await self.get_table_and_convert(query_loss, index="mrid")
        df = pd.concat([data.assign(ploss_1=0.0), loss.loc[data.index]], axis=1)
        return TransformerWindingDataFrame(df)

    @property
    def substation_voltage_level_query(self) -> str:
        return self.template_to_query(templates.SUBSTATION_VOLTAGE_LEVEL_QUERY)

    async def substation_voltage_level(self) -> SubstationVoltageDataFrame:
        query = self.substation_voltage_level_query
        df = await self.get_table_and_convert(query, index="substation")
        return SubstationVoltageDataFrame(df)

    @property
    def disconnected_query(self) -> str:
        return self.template_to_query(templates.DISCONNECTED_QUERY)

    async def disconnected(self) -> DisconnectedDataFrame:
        """Query disconneced status from ssh profile (not available in GraphDB)."""
        df = await self.get_table_and_convert(self.disconnected_query)
        return DisconnectedDataFrame(df)

    @property
    def powerflow_query(self) -> str:
        return self.template_to_query(templates.POWER_FLOW_QUERY)

    async def powerflow(self) -> PowerFlowDataFrame:
        """Query powerflow from sv profile (not available in GraphDB)."""
        df = await self.get_table_and_convert(self.powerflow_query, index="mrid")
        return PowerFlowDataFrame(df)

    @property
    def transformer_windings_query(self) -> str:
        return self.template_to_query(templates.TRANSFORMER_WINDINGS_QUERY)

    async def transformer_windings(self) -> TransformerWindingsDataFrame:
        """Query windings from EQ profile."""
        df = await self.get_table_and_convert(self.transformer_windings_query, index="w_mrid")
        return TransformerWindingsDataFrame(df)

    @property
    def coordinates_query(self) -> str:
        return self.template_to_query(templates.COORDINATES_QUERY)

    async def coordinates(self) -> CoordinatesDataFrame:
        return CoordinatesDataFrame(await self.get_table_and_convert(self.coordinates_query))

    @property
    def st_group_codes_names_query(self) -> str:
        return self.template_to_query(templates.STATION_GROUP_CODE_NAME_QUERY)

    async def station_group_codes_and_names(self) -> StationGroupCodeNameDataFrame:
        df = await self.get_table_and_convert(self.st_group_codes_names_query)
        return StationGroupCodeNameDataFrame(df.set_index("station_group"))

    def branch_node_withdraw_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.BRANCH_NODE_WITHDRAW_QUERY, substitutes)

    async def branch_node_withdraw(self, region: str | None = None) -> BranchWithdrawDataFrame:
        """Query branch flow from sv profile."""
        query = self.branch_node_withdraw_query(region)
        df = await self.get_table_and_convert(query, index="mrid")
        return BranchWithdrawDataFrame(df)

    def dc_active_flow_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.DC_ACTIVE_POWER_FLOW_QUERY, substitutes)

    async def dc_active_flow(self, region: str | None = None) -> DcActiveFlowDataFrame:
        query = self.dc_active_flow_query(region)
        df = await self.get_table_and_convert(query)
        # Unable to group on max within the sparql query so we do it here.
        df = df.iloc[df.groupby("mrid")["p"].idxmax()].set_index("mrid")
        df["p"] *= df["direction"]
        return DcActiveFlowDataFrame(df.drop(columns="direction"))

    @property
    def sv_injection_query(self) -> str:
        return self.template_to_query(templates.SV_INJECTION_QUERY)

    async def sv_injection(self) -> SvInjectionDataFrame:
        df = await self.get_table_and_convert(self.sv_injection_query)
        return SvInjectionDataFrame(df)

    @property
    def regions_query(self) -> str:
        return self.template_to_query(templates.REGIONS_QUERY)

    async def regions(self) -> RegionsDataFrame:
        """Query regions

        Property:
           regions: List of regions in database

        Example:
            >>> from cimsparql.model import get_single_client_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_single_client_model(server_url, "LATEST")
            >>> model.regions
        """
        # TODO: Probably deprecate this property in the future. But keep for now.
        # We need to find a solution for custom namespaces in queries
        df = await self.get_table_and_convert(self.regions_query, index="mrid")
        return RegionsDataFrame(df)

    @property
    def hvdc_converter_bidzone_query(self) -> str:
        return self.template_to_query(templates.HVDC_CONVERTER_BIDZONES)

    async def hvdc_converter_bidzones(self) -> HVDCBidzonesDataFrame:
        """
        Fetching mrid of converters placed on HVDC exchange corridors together with
        to/from bidzone
        """
        df = await self.get_table_and_convert(self.hvdc_converter_bidzone_query, index="mrid")
        return HVDCBidzonesDataFrame(df)

    @property
    def ras_equipment_query(self) -> str:
        return self.template_to_query(templates.RAS_EQUIPMENT_QUERY)

    async def ras_equipment(self) -> RASEquipmentDataFrame:
        df = await self.get_table_and_convert(self.ras_equipment_query)
        return RASEquipmentDataFrame(df)

    def add_mrid_query(self, rdf_type: str | None = None, graph: str | None = None) -> str:
        substitutes = {"rdf_type": rdf_type or "?rdf_type", "g": graph or "?g"}
        return self.template_to_query(templates.ADD_MRID_QUERY, substitutes)

    def add_mrid(
        self,
        rdf_type: str | None = None,
        graph: str | None = None,
        client: GraphDBClient | None = None,
    ) -> None:
        """
        Add cim:IdentifiedObject.mRID to all records. It is copied from rdf:about (or rdf:ID) if
        replace is not specified. The query is executed with the passed client. If not given,
        the default client is used.

        Args:
            graph: Name of graph where mrids should be added. Note, mrid is added to all objects
                in the graph.
            rdf_type: RDF type where ID should be added
            client: Client used to execute the query
        """
        client = client or self.client
        client.update_query(self.add_mrid_query(rdf_type, graph))

    def switches_query(self) -> str:
        return self.template_to_query(templates.SWITCHES_QUERY)

    async def switches(self) -> SwitchesDataFrame:
        df = await self.get_table_and_convert(self.switches_query())
        return SwitchesDataFrame(df)

    def connectivity_nodes_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.CONNECTIVITY_NODES_QUERY, substitutes)

    async def connectivity_nodes(self, region: str | None = None) -> ConnectivityNodeDataFrame:
        df = await self.get_table_and_convert(self.connectivity_nodes_query(region), index="mrid")
        return ConnectivityNodeDataFrame(df)


class SingleClientModel(Model):
    def __init__(
        self,
        client: GraphDBClient,
        config: ModelConfig | None = None,
        mapper: TypeMapper | None = None,
    ) -> None:
        clients = defaultdict(lambda: client)
        super().__init__(clients, config, mapper)


def get_cim_model(
    service_cfg: ServiceConfig | None = None,
    model_cfg: ModelConfig | None = None,
    async_sparql_wrapper: bool = False,
    custom_headers: dict[str, str] | None = None,
) -> SingleClientModel:
    """
    Function kept for backward compatibility. Use `get_single_client_model` instead.
    """
    return get_single_client_model(service_cfg, model_cfg, async_sparql_wrapper, custom_headers)


def get_single_client_model(
    service_cfg: ServiceConfig | None = None,
    model_cfg: ModelConfig | None = None,
    async_sparql_wrapper: bool = False,
    custom_headers: dict[str, str] | None = None,
) -> SingleClientModel:
    """Get a CIM Model.

    Args:
        service_cfg: Configurations for the triple store service
        model_cfg: Configurations for the CIM mode
        async_sparql_wrapper: If True http calls are made via asynchronous requests.
            If False, the native SparqlWrapper sends requests via urllib
        custom_headers: Custom headers to be added to the requests
    """
    Client = AsyncGraphDBClient if async_sparql_wrapper else GraphDBClient  # noqa N806
    return SingleClientModel(Client(service_cfg, custom_headers), model_cfg)


def query_name(query: str) -> str:
    """
    Extract the name of the query provided that the first line starts with # Name: <name>.
    If no match is found, an empty string is returned
    """
    m = re.search("^# Name: ([a-zA-Z0-9 ]+)", query)
    return m.group(1) if m else ""


def get_federated_cim_model(
    eq_client: GraphDBClient,
    tpsvssh_client: GraphDBClient,
    model_cfg: ModelConfig,
    mapper: TypeMapper | None = None,
) -> Model:
    """
    Return a CIM model where the equipment profile is located in one repo and the topology,
    state variables and steady state hypothesis profile is located in another.

    Args:
        eq_client: Client that executes queries from the EQ repository
        tpsvhssh_client: Client that executes queries from the TP/SV/SSH repository
        model_cfg: Mode configurations that provides extra information
    """
    clients = defaultdict(lambda: eq_client)  # By default queries are executed from the EQ repo

    # Setup client based on # Name in the pre-defined queries
    exec_from_tpssvssh = (
        "AC Lines",
        "Bus",
        "Converters",
        "Disconnected",
        "Exchange",
        "Loads",
        "Branch node withdraw",
        "DC Active Power Flow",
        "Series compensators",
        "SV branch",
        "Power flow",
        "SvInjection",
        "Switches",
        "Three winding",
        "Three winding loss",
        "Three winding with dummy nodes",
        "Two winding transformer",
        "Two winding transformer angle",
        "Synchronous machines",
        "Windings",
    )
    for query in exec_from_tpssvssh:
        clients[query] = tpsvssh_client
    return Model(clients, model_cfg, mapper)
