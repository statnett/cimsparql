"""The cimsparql.model module contains the base class Model."""

from __future__ import annotations

import functools
import logging
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from functools import cached_property
from typing import TYPE_CHECKING, ParamSpec, Self, TypeVar
from uuid import uuid4

import pandas as pd

from cimsparql import templates
from cimsparql.data_models import (
    AcLinesDataFrame,
    AssociatedSwitchesDataFrame,
    BaseVoltageDataFrame,
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
    GenUnitAndSyncMachineMridDataFrame,
    HVDCBidzonesDataFrame,
    HVDCDataFrame,
    LoadsDataFrame,
    MarketDatesDataFrame,
    PhaseTapChangerDataFrame,
    PowerFlowDataFrame,
    RASEquipmentDataFrame,
    RegionsDataFrame,
    StationGroupCodeNameDataFrame,
    StationGroupForPowerUnitDataFrame,
    SubstationVoltageDataFrame,
    SvInjectionDataFrame,
    SvPowerDeviationDataFrame,
    SwitchesDataFrame,
    SynchronousMachinesDataFrame,
    TransfConToConverterDataFrame,
    TransformersDataFrame,
    TransformerWindingDataFrame,
    TransformerWindingsDataFrame,
    WindGeneratingUnitsDataFrame,
)
from cimsparql.graphdb import GraphDBClient, ServiceConfig
from cimsparql.type_mapper import TypeMapper
from cimsparql.utils import query_name

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable
    from string import Template
    from types import TracebackType

    from cimsparql.sparql_result_json import SparqlResultValue
    from cimsparql.value_mapper import ValueMapper


@dataclass
class ModelConfig:
    system_state_repo: str | None = None
    eq_repo: str | None = None
    value_mappers: Iterable[ValueMapper] = field(default_factory=list)


logger = logging.getLogger()
T = TypeVar("T")
P = ParamSpec("P")


def time_it(f: Callable[P, T]) -> Callable[P, T]:
    @functools.wraps(f)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
        started = time.time()
        result = f(*args, **kwargs)
        finished = time.time()
        logger.debug("%s took %f seconds", f.__name__, finished - started)
        return result

    return wrapped


class Model:
    def __init__(
        self,
        clients: dict[str, GraphDBClient],
        config: ModelConfig | None = None,
        mapper: TypeMapper | None = None,
    ) -> None:
        self.clients = clients
        self.config = config or ModelConfig()
        self.mapper = mapper or TypeMapper(self.get_client("Type mapper"))

    @property
    def distinct_clients(self) -> list[GraphDBClient]:
        obj_ids = set[int]()
        distinct = list[GraphDBClient]()
        for client in self.clients.values():
            if id(client) not in obj_ids:
                obj_ids.add(id(client))
                distinct.append(client)
        return distinct

    def get_client(self, query_name: str) -> GraphDBClient:
        """Return the correct graph db client to execute a query.

        By default there is only one client so the same client is returned in all cases
        """
        return self.clients[query_name]

    def __enter__(self) -> Self:
        transaction_id = str(uuid4())
        for client in self.clients.values():
            client.add_correlation_id_to_header(transaction_id)
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        _, _, _ = exc_type, exc, exc_tb
        for client in self.clients.values():
            client.clear_correlation_id_from_header()

    @property
    def client(self) -> GraphDBClient:
        return self.get_client("default")

    @client.setter
    def client(self, default_client: GraphDBClient) -> None:
        self.clients["default"] = default_client

    @staticmethod
    def _col_map(data_row: dict[str, SparqlResultValue]) -> dict[str, str]:
        return {column: data.datatype if data.datatype else data.value_type for column, data in data_row.items()}

    @classmethod
    def col_map(cls, data_row: dict[str, SparqlResultValue], columns: dict[str, str]) -> dict[str, str]:
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
        data_row: dict[str, SparqlResultValue],
        index: str | None = None,
        columns: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        col_map = self.col_map(data_row, columns or {})
        result = self.mapper.map_data_types(result, col_map)
        for v_mapper in self.config.value_mappers:
            result = v_mapper.map(result)

        if index:
            return result.set_index(index)
        return result

    def get_table_and_convert(
        self, query: str, index: str | None = None, columns: dict[str, str] | None = None
    ) -> pd.DataFrame:
        name = query_name(query)
        client = self.get_client(name)
        result, data_row = client.get_table(query)
        return self._convert_result(result, data_row, index, columns)

    def template_to_query(self, template: Template, substitutes: dict[str, str] | None = None) -> str:
        """Convert provided template to query."""
        substitutes = substitutes or {}

        # Extract name from the query. We don't need to perform actual substitutions to do this,
        # we just let placeholder be left
        name = query_name(template.safe_substitute())
        client = self.get_client(name)
        state_repo = self.config.system_state_repo or client.service_cfg.url
        eq_repo = self.config.eq_repo or client.service_cfg.url

        return template.safe_substitute({"repo": state_repo, "eq_repo": eq_repo} | substitutes | self.client.prefixes)

    @cached_property
    def cim_version(self) -> int:
        if m := re.search("cim(\\d+)", self.client.prefixes["cim"]):
            return int(m.group(1))
        return 0

    @property
    def full_model_query(self) -> str:
        return self.template_to_query(templates.FULL_MODEL_QUERY)

    @time_it
    def full_model(self) -> FullModelDataFrame:
        """Return all models where all dependencies has been created and is available.

        All profiles EQ/SSH/TP/SV will define a md:FullModel with possible dependencies. One profile
        could be dependent on more than one other. This function will return the models for SSH/TP
        and SV that is available from current repo or provided <repo>.

        Example:
        >> model.full_model()
        """
        df = self.get_table_and_convert(self.full_model_query)
        return FullModelDataFrame(df)

    @property
    def market_dates_query(self) -> str:
        """Market activation date for this repository."""
        return self.template_to_query(templates.MARKET_DATES_QUERY)

    @time_it
    def market_dates(self) -> MarketDatesDataFrame:
        df = self.get_table_and_convert(self.market_dates_query, index="mrid")
        return MarketDatesDataFrame(df)

    def bus_data_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.BUS_DATA_QUERY, substitutes)

    def transformer_center_nodes_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.TRANSFORMER_CENTER_NODES_QUERY, substitutes)

    @time_it
    def bus_data(self, region: str | None = None) -> BusDataFrame:
        """Query name of topological nodes (TP query).

        Args:
           region: Limit to region (use None to get all)
        """
        dfs = [
            self.get_table_and_convert(self.bus_data_query(region), index="node"),
            self.transformer_center_nodes(region),
        ]
        df = pd.concat(dfs)
        return BusDataFrame(df)

    def loads_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.LOADS_QUERY, substitutes)

    @time_it
    def transformer_center_nodes(self, region: str | None = None) -> BusDataFrame:
        df = self.get_table_and_convert(self.transformer_center_nodes_query(region), index="node")
        return BusDataFrame(df)

    @time_it
    def loads(self, region: str | None = None) -> LoadsDataFrame:
        """Query load data.

        Args:
           region: regexp that limits to region
        """
        query = self.loads_query(region)
        df = self.get_table_and_convert(query, index="mrid")
        return LoadsDataFrame(df)

    def wind_generating_units_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.WIND_GENERATING_UNITS_QUERY, substitutes)

    @time_it
    def wind_generating_units(self, region: str | None = None) -> WindGeneratingUnitsDataFrame:
        """Query wind generating units.

        Args:
           region: regexp that limits to region

        Example:
            >>> from cimsparql.model import get_single_client_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_single_client_model(server_url, "LATEST")
            >>> model.wind_generating_units()
        """
        query = self.wind_generating_units_query(region)
        df = self.get_table_and_convert(query, index="mrid")
        return WindGeneratingUnitsDataFrame(df)

    def synchronous_machines_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.SYNCHRONOUS_MACHINES_QUERY, substitutes)

    @time_it
    def synchronous_machines(self, region: str | None = None) -> SynchronousMachinesDataFrame:
        query = self.synchronous_machines_query(region)
        df = self.get_table_and_convert(query, index="mrid")
        return SynchronousMachinesDataFrame(df)

    def connections_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.CONNECTIONS_QUERY, substitutes)

    @time_it
    def connections(self, region: str | None = None) -> ConnectionsDataFrame:
        """Query connectors.

        Args:
           region: regexp that limits to region

        Example:
            >>> from cimsparql.model import get_single_client_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_single_client_model(server_url, "LATEST")
            >>> model.connections()
        """
        query = self.connections_query(region)
        df = self.get_table_and_convert(query, index="mrid")
        return ConnectionsDataFrame(df)

    def borders_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.BORDERS_QUERY, substitutes)

    @time_it
    def borders(self, region: str | None = None) -> BordersDataFrame:
        """Retrieve ACLineSegments where one terminal is inside and the other is outside the region.

        Args:
            region: Inside area
            limit: return first 'limit' number of rows
        """
        query = self.borders_query(region)
        df = self.get_table_and_convert(query, index="mrid")
        return BordersDataFrame(df)

    def exchange_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.EXCHANGE_QUERY, substitutes)

    @time_it
    def exchange(self, region: str | None = None) -> ExchangeDataFrame:
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
        df = self.get_table_and_convert(query, index="mrid")
        return ExchangeDataFrame(df)

    def converters_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.CONVERTERS_QUERY, substitutes)

    @time_it
    def converters(self, region: str | None = None) -> ConvertersDataFrame:
        query = self.converters_query(region)
        df = self.get_table_and_convert(query, index="mrid")
        return ConvertersDataFrame(df)

    def transformers_connected_to_converter_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.TRANSFORMERS_CONNECTED_TO_CONVERTER_QUERY, substitutes)

    @time_it
    def transformers_connected_to_converter(self, region: str | None = None) -> TransfConToConverterDataFrame:
        """Query list of transformer connected at a converter (Voltage source or DC).

        Args:
           region: regexp that limits to region

        """
        query = self.transformers_connected_to_converter_query(region)
        df = self.get_table_and_convert(query, index="mrid")
        return TransfConToConverterDataFrame(df)

    def ac_lines_query(self, region: str | None = None, rate: str | None = None) -> str:
        substitutes = {"region": region or ".*", "rate": rate or "Normal@20"}
        return self.template_to_query(templates.AC_LINE_QUERY, substitutes)

    @time_it
    def ac_lines(self, region: str | None = None, rate: str | None = None) -> AcLinesDataFrame:
        """Query ac line segments.

        Args:
           region: regexp that limits to region
           rate: specific rate (default: Normal@20)

        Example:
            >>> from cimsparql.model import get_single_client_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_single_client_model(server_url, "LATEST")
            >>> model.ac_lines()
        """
        query = self.ac_lines_query(region, rate)
        df = self.get_table_and_convert(query, index="mrid")
        return AcLinesDataFrame(df)

    def series_compensators_query(self, region: str | None = None, rate: str | None = None) -> str:
        substitutes = {"region": region or ".*", "rate": rate or "Normal@20"}
        return self.template_to_query(templates.SERIES_COMPENSATORS_QUERY, substitutes)

    @time_it
    def series_compensators(self, region: str | None = None, rate: str | None = None) -> BranchComponentDataFrame:
        """Query series compensators.

        Args:
           region: regexp that limits to region
           rate: specific rate (default: Normal@20)
        """
        query = self.series_compensators_query(region, rate)
        df = self.get_table_and_convert(query, index="mrid")
        return BranchComponentDataFrame(df)

    def transformers_query(self, region: str | None = None, rate: str | None = None) -> str:
        substitutes = {"region": region or ".*", "rate": rate or "Normal@20"}
        return self.template_to_query(templates.TRANSFORMERS_QUERY, substitutes)

    @time_it
    def transformers(self, region: str | None = None, rate: str | None = None) -> TransformersDataFrame:
        """Query transformer windings.

        Args:
           region: regexp that limits to region
           rate: specific rate (default: Normal@20)
        """
        query = self.transformers_query(region, rate)
        df = self.get_table_and_convert(query)
        return TransformersDataFrame(df)

    @property
    def winding_angle_query(self) -> str:
        return self.template_to_query(templates.TRANSFORMER_WINDING_ANGLE_QUERY)

    def winding_angle(self) -> pd.DataFrame:
        return self.get_table_and_convert(self.winding_angle_query, index="mrid")

    @property
    def winding_query(self) -> str:
        return self.template_to_query(templates.WINDING)

    def winding_loss_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.WINDING_LOSS_QUERY, substitutes)

    def transformer_branches_query(self, region: str | None = None, rate: str | None = None) -> str:
        substitutes = {"region": region or ".*", "rate": rate or "Normal@20"}
        return self.template_to_query(templates.TRANSFORMER_BRANCHES_QUERY, substitutes)

    @time_it
    def transformer_branches(self, region: str | None = None, rate: str | None = None) -> TransformerWindingDataFrame:
        """Query transformer branches.

        For two winding transformers will give two "branches" (x are nodes, o is the fictisous center node)

        x --- o ---- x

        Three winding transformers

            x
            |
        x - o - x



        Example:
            >>> from cimsparql.model import get_single_client_model
            >>> server_url = "127.0.0.1:7200"
            >>> model = get_single_client_model(server_url, "LATEST")
            >>> model.transformer_branches()
        """
        query = self.transformer_branches_query(region, rate)
        data = self.get_table_and_convert(query, index="mrid")
        query_loss = self.winding_loss_query(region)
        loss = self.get_table_and_convert(query_loss, index="mrid")
        df = data.assign(ploss_1=0.0, ploss_2=lambda df: df["connectivity_node_2"].map(loss["ploss_2"]))
        data.update(self.winding_angle())
        return TransformerWindingDataFrame(df)

    @property
    def substation_voltage_level_query(self) -> str:
        return self.template_to_query(templates.SUBSTATION_VOLTAGE_LEVEL_QUERY)

    def substation_voltage_level(self) -> SubstationVoltageDataFrame:
        query = self.substation_voltage_level_query
        df = self.get_table_and_convert(query, index="substation")
        return SubstationVoltageDataFrame(df)

    @property
    def disconnected_query(self) -> str:
        return self.template_to_query(templates.DISCONNECTED_QUERY)

    @time_it
    def disconnected(self) -> DisconnectedDataFrame:
        """Query disconnected status from ssh profile (not available in GraphDB)."""
        df = self.get_table_and_convert(self.disconnected_query)
        return DisconnectedDataFrame(df)

    @property
    def powerflow_query(self) -> str:
        return self.template_to_query(templates.POWER_FLOW_QUERY)

    @time_it
    def powerflow(self) -> PowerFlowDataFrame:
        """Query powerflow from sv profile (not available in GraphDB)."""
        df = self.get_table_and_convert(self.powerflow_query, index="mrid")
        return PowerFlowDataFrame(df)

    @property
    def phase_tap_changer_query(self) -> str:
        return self.template_to_query(templates.PHASE_TAP_CHANGER)

    def phase_tap_changer(self) -> PhaseTapChangerDataFrame:
        return PhaseTapChangerDataFrame(self.get_table_and_convert(self.phase_tap_changer_query))

    @property
    def transformer_windings_query(self) -> str:
        return self.template_to_query(templates.TRANSFORMER_WINDINGS_QUERY)

    @time_it
    def transformer_windings(self) -> TransformerWindingsDataFrame:
        """Query windings from EQ profile."""
        df = self.get_table_and_convert(self.transformer_windings_query, index="w_mrid")
        return TransformerWindingsDataFrame(df)

    @property
    def coordinates_query(self) -> str:
        return self.template_to_query(templates.COORDINATES_QUERY)

    @time_it
    def coordinates(self) -> CoordinatesDataFrame:
        return CoordinatesDataFrame(self.get_table_and_convert(self.coordinates_query))

    @property
    def st_group_codes_names_query(self) -> str:
        return self.template_to_query(templates.STATION_GROUP_CODE_NAME_QUERY)

    @time_it
    def station_group_codes_and_names(self) -> StationGroupCodeNameDataFrame:
        df = self.get_table_and_convert(self.st_group_codes_names_query).set_index("station_group")
        if df.index.has_duplicates:
            duplicate_indices = df.index[df.index.duplicated(keep="first")]
            logger.warning("Found duplicated names for following station groups: %s", df.loc[duplicate_indices])

        return StationGroupCodeNameDataFrame(df[~df.index.duplicated(keep="first")])

    def branch_node_withdraw_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.BRANCH_NODE_WITHDRAW_QUERY, substitutes)

    @time_it
    def branch_node_withdraw(self, region: str | None = None) -> BranchWithdrawDataFrame:
        """Query branch flow from sv profile."""
        query = self.branch_node_withdraw_query(region)
        df = self.get_table_and_convert(query, index="mrid")
        return BranchWithdrawDataFrame(df)

    def dc_active_flow_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.DC_ACTIVE_POWER_FLOW_QUERY, substitutes)

    @time_it
    def dc_active_flow(self, region: str | None = None) -> DcActiveFlowDataFrame:
        query = self.dc_active_flow_query(region)
        df = self.get_table_and_convert(query).astype({"p": float})
        # Unable to group on max within the sparql query so we do it here.
        df = df.iloc[df.groupby("mrid")["p"].idxmax()].set_index("mrid")
        df["p"] *= df["direction"]
        return DcActiveFlowDataFrame(df.drop(columns="direction"))

    @property
    def sv_injection_query(self) -> str:
        return self.template_to_query(templates.SV_INJECTION_QUERY)

    @time_it
    def sv_injection(self) -> SvInjectionDataFrame:
        df = self.get_table_and_convert(self.sv_injection_query)
        return SvInjectionDataFrame(df)

    @property
    def regions_query(self) -> str:
        return self.template_to_query(templates.REGIONS_QUERY)

    @time_it
    def regions(self) -> RegionsDataFrame:
        """Query regions.

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
        df = self.get_table_and_convert(self.regions_query, index="mrid")
        return RegionsDataFrame(df)

    @property
    def hvdc_query(self) -> str:
        return self.template_to_query(templates.HVDC)

    @time_it
    def hvdc(self) -> HVDCDataFrame:
        df = self.get_table_and_convert(self.hvdc_query)
        return HVDCDataFrame(df)

    @property
    def hvdc_converter_bidzone_query(self) -> str:
        return self.template_to_query(templates.HVDC_CONVERTER_BIDZONES)

    @time_it
    def hvdc_converter_bidzones(self) -> HVDCBidzonesDataFrame:
        """Fetch mrid of converters placed on HVDC exchange corridors together with to/from bidzone."""
        df = self.get_table_and_convert(self.hvdc_converter_bidzone_query, index="mrid")
        return HVDCBidzonesDataFrame(df)

    @property
    def ras_equipment_query(self) -> str:
        return self.template_to_query(templates.RAS_EQUIPMENT_QUERY)

    @time_it
    def ras_equipment(self) -> RASEquipmentDataFrame:
        df = self.get_table_and_convert(self.ras_equipment_query)
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
        """Add cim:IdentifiedObject.mRID to all records.

        It is copied from rdf:about (or rdf:ID) if replace is not specified. The query is executed with the passed
        client. If not given, the default client is used.

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

    @time_it
    def switches(self) -> SwitchesDataFrame:
        df = self.get_table_and_convert(self.switches_query(), index="mrid")
        return SwitchesDataFrame(df)

    def connectivity_nodes_query(self, region: str | None = None) -> str:
        substitutes = {"region": region or ".*"}
        return self.template_to_query(templates.CONNECTIVITY_NODES_QUERY, substitutes)

    @time_it
    def connectivity_nodes(self, region: str | None = None) -> ConnectivityNodeDataFrame:
        df = self.get_table_and_convert(self.connectivity_nodes_query(region), index="mrid")
        return ConnectivityNodeDataFrame(df)

    def sv_power_deviation_query(self) -> str:
        return self.template_to_query(templates.SV_POWER_DEVIATION_QUERY)

    @time_it
    def sv_power_deviation(self) -> SvPowerDeviationDataFrame:
        return SvPowerDeviationDataFrame(self.get_table_and_convert(self.sv_power_deviation_query()))

    @time_it
    def base_voltage(self) -> BaseVoltageDataFrame:
        query = self.template_to_query(templates.BASE_VOLTAGE)
        return BaseVoltageDataFrame(self.get_table_and_convert(query))

    @time_it
    def associated_switches(self) -> AssociatedSwitchesDataFrame:
        query = self.template_to_query(templates.ASSOCIATED_SWITCHES)
        return AssociatedSwitchesDataFrame(self.get_table_and_convert(query))

    @time_it
    def gen_unit_and_sync_machine_mrid(self) -> GenUnitAndSyncMachineMridDataFrame:
        query = self.template_to_query(templates.GEN_UNIT_MRID_AND_SYNC_MACHINE)
        return GenUnitAndSyncMachineMridDataFrame(self.get_table_and_convert(query))

    @time_it
    def station_group_for_power_unit(self) -> StationGroupForPowerUnitDataFrame:
        query = self.template_to_query(templates.STATION_GROUP_FOR_POWER_UNIT_QUERY)
        return StationGroupForPowerUnitDataFrame(self.get_table_and_convert(query))

    @time_it
    def busbar_section(self) -> Generator[str]:
        """Return set of mrids for all busbar sections in model."""
        query = self.template_to_query(templates.BUSBAR_SECTION)
        return (val["mrid"] for val in self.get_client(query_name(query)).exec_query(query).results.values_as_dict())


class SingleClientModel(Model):
    def __init__(
        self,
        client: GraphDBClient,
        config: ModelConfig | None = None,
        mapper: TypeMapper | None = None,
    ) -> None:
        clients = defaultdict[str, GraphDBClient](lambda: client)
        super().__init__(clients, config, mapper)


def get_cim_model(
    service_cfg: ServiceConfig | None = None,
    model_cfg: ModelConfig | None = None,
    custom_headers: dict[str, str] | None = None,
) -> SingleClientModel:
    """Get cim model.

    Function kept for backward compatibility. Use `get_single_client_model` instead.
    """
    return get_single_client_model(service_cfg, model_cfg, custom_headers)


def get_single_client_model(
    service_cfg: ServiceConfig | None = None,
    model_cfg: ModelConfig | None = None,
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
    return SingleClientModel(GraphDBClient(service_cfg, custom_headers), model_cfg)


def get_federated_cim_model(
    eq_client: GraphDBClient,
    tpsvssh_client: GraphDBClient,
    model_cfg: ModelConfig,
    mapper: TypeMapper | None = None,
) -> Model:
    """Get federated CIM model.

    Return a CIM model where the equipment profile is located in one repo and the topology,
    state variables and steady state hypothesis profile is located in another.

    Args:
        eq_client: Client that executes queries from the EQ repository
        tpsvssh_client: Client that executes queries from the TP/SV/SSH repository
        model_cfg: Mode configurations that provides extra information
        mapper: Type mapper
    """
    clients = defaultdict[str, GraphDBClient](lambda: eq_client)  # By default queries are executed from the EQ repo

    # Setup client based on # Name in the pre-defined queries
    exec_from_tpssvssh = (
        "AC Lines",
        "Base voltage",
        "Branch node withdraw",
        "Bus",
        "Converters",
        "DC Active Power Flow",
        "Disconnected",
        "Exchange",
        "Full model",
        "Loads",
        "Phase tap changer",
        "Power flow",
        "SV branch",
        "Series compensators",
        "SvInjection",
        "Switches",
        "Synchronous machines",
        "Sv power deviation",
        "Transformer branches loss",
        "Transformer branches",
        "Transformer center nodes",
        "Winding transformer angle",
        "Windings",
    )
    for query in exec_from_tpssvssh:
        clients[query] = tpsvssh_client
    return Model(clients, model_cfg, mapper)
