"""Data models used to describe and validate sparql queries from cimsparql."""

import datetime as dt

import pandera.pandas as pa
from pandera.api.pandas.model_config import BaseConfig
from pandera.typing import DataFrame, Index


class CoercingSchema(pa.DataFrameModel):
    """Base schema that configures coercing."""

    class Config(BaseConfig):
        """Pandas DataFrameSchema options."""

        coerce = True


class FullModelSchema(CoercingSchema):
    """Full model schema that ensures unique time for all profiles."""

    model: str
    time: str
    profile: str
    version: str
    description: str


FullModelDataFrame = DataFrame[FullModelSchema]


class MridResourceSchema(CoercingSchema):
    """Common class for resources with an mrid as index."""

    mrid: Index[str] = pa.Field(unique=True, check_name=True)


class NamedResourceSchema(MridResourceSchema):
    """Common class for resources with an mrid and a name."""

    name: str


class NamedMarketResourceSchema(NamedResourceSchema):
    """Common class for named resources with an (optional) associated market_code."""

    market_code: str = pa.Field(nullable=True)


class MarketDatesSchema(NamedResourceSchema):
    activation_date: dt.datetime = pa.Field()


MarketDatesDataFrame = DataFrame[MarketDatesSchema]


class BusDataSchema(CoercingSchema):
    node: Index[str] = pa.Field(unique=True, check_name=True)
    busname: str = pa.Field()
    substation: str = pa.Field()
    un: float = pa.Field()
    substation_mrid: str = pa.Field()
    bidzone: str = pa.Field(nullable=True)
    sv_voltage: float = pa.Field()
    island: str = pa.Field()
    is_swing_bus: bool = pa.Field()
    base_voltage_mrid: str = pa.Field()


BusDataFrame = DataFrame[BusDataSchema]


class LoadsSchema(NamedResourceSchema):
    substation_mrid: str = pa.Field()
    status: bool = pa.Field()
    p: float = pa.Field(nullable=True)
    q: float = pa.Field(nullable=True)
    station_group: str = pa.Field(nullable=True)
    connectivity_node: str = pa.Field()


LoadsDataFrame = DataFrame[LoadsSchema]


class WindGeneratingUnitsSchema(NamedMarketResourceSchema):
    station_group: str = pa.Field(nullable=True)
    min_p: float = pa.Field()
    max_p: float = pa.Field()
    plant_mrid: str = pa.Field(nullable=True)


WindGeneratingUnitsDataFrame = DataFrame[WindGeneratingUnitsSchema]


class SynchronousMachinesSchema(NamedMarketResourceSchema):
    status: bool = pa.Field()
    station_group: str = pa.Field(nullable=True)
    station_group_name: str = pa.Field(nullable=True)
    substation_mrid: str = pa.Field()
    max_p: float = pa.Field()
    min_p: float = pa.Field()
    merit_order: float = pa.Field(nullable=True)
    sn: float = pa.Field()
    p: float = pa.Field(nullable=True)
    q: float = pa.Field(nullable=True)
    connectivity_node: str = pa.Field()
    generator_type: str = pa.Field()
    storage_type: str = pa.Field(nullable=True)
    schedule_resource: str = pa.Field(nullable=True)
    afrr_prequalified: bool = pa.Field()


SynchronousMachinesDataFrame = DataFrame[SynchronousMachinesSchema]


class ConnectionsSchema(MridResourceSchema):
    t_mrid_1: str = pa.Field()
    t_mrid_2: str = pa.Field()


ConnectionsDataFrame = DataFrame[ConnectionsSchema]


class BordersSchema(NamedMarketResourceSchema):
    area_1: str = pa.Field()
    area_2: str = pa.Field()
    t_mrid_1: str = pa.Field()
    t_mrid_2: str = pa.Field()


BordersDataFrame = DataFrame[BordersSchema]


class ExchangeSchema(NamedMarketResourceSchema):
    node: str = pa.Field()
    status: bool = pa.Field()
    p: float = pa.Field()


ExchangeDataFrame = DataFrame[ExchangeSchema]


class PhaseTapChangerSchema(CoercingSchema):
    mrid: str = pa.Field()
    phase_shift_increment: float = pa.Field()
    enabled: bool = pa.Field()
    neutral_step: int = pa.Field()
    high_step: int = pa.Field()
    low_step: int = pa.Field()
    mode: str = pa.Field()
    target_value: float = pa.Field()
    monitored_winding: str = pa.Field()


PhaseTapChangerDataFrame = DataFrame[PhaseTapChangerSchema]


class ConvertersSchema(NamedResourceSchema):
    alias: str = pa.Field(nullable=True)
    substation_mrid: str = pa.Field()
    status: bool = pa.Field()
    p: float = pa.Field()
    q: float = pa.Field()
    connectivity_node: str = pa.Field()
    controller: str = pa.Field()
    controller_factor: float = pa.Field()
    pole_loss: float = pa.Field()
    loss0: float = pa.Field()
    loss1: float = pa.Field()
    loss2: float = pa.Field()
    vdcn: float = pa.Field()
    un: float = pa.Field()


ConvertersDataFrame = DataFrame[ConvertersSchema]


class TransfConToConverterSchema(NamedResourceSchema):
    t_mrid: str = pa.Field()
    p_mrid: str = pa.Field()


TransfConToConverterDataFrame = DataFrame[TransfConToConverterSchema]


class CoordinatesSchema(CoercingSchema):
    mrid: str = pa.Field()
    x: float = pa.Field()
    y: float = pa.Field()
    epsg: str = pa.Field()
    rdf_type: str = pa.Field()


CoordinatesDataFrame = DataFrame[CoordinatesSchema]


class BranchComponentSchema(NamedResourceSchema):
    ploss_1: float = pa.Field(nullable=True)
    ploss_2: float = pa.Field(nullable=True)
    r: float = pa.Field()
    rate: float = pa.Field(nullable=True)
    status: bool = pa.Field()
    un: float = pa.Field()
    x: float = pa.Field()
    connectivity_node_1: str = pa.Field()
    connectivity_node_2: str = pa.Field()


BranchComponentDataFrame = DataFrame[BranchComponentSchema]


class ShuntComponentSchema(BranchComponentSchema):
    b: float = pa.Field()
    g: float = pa.Field()


ShuntComponentDataFrame = DataFrame[ShuntComponentSchema]


class AcLinesSchema(ShuntComponentSchema):
    length: float = pa.Field()
    g: float = pa.Field(nullable=True)


AcLinesDataFrame = DataFrame[AcLinesSchema]


class TransformersSchema(CoercingSchema):
    name: str = pa.Field()
    p_mrid: str = pa.Field()
    w_mrid: str = pa.Field()
    end_number: int = pa.Field()
    un: float = pa.Field()
    t_mrid: str = pa.Field()
    r: float = pa.Field()
    x: float = pa.Field()
    rate: float = pa.Field(nullable=True)


TransformersDataFrame = DataFrame[TransformersSchema]


class TransformerWindingSchema(ShuntComponentSchema):
    angle: float = pa.Field()
    ratio: float = pa.Field()


TransformerWindingDataFrame = DataFrame[TransformerWindingSchema]


class SubstationVoltageSchema(CoercingSchema):
    substation: Index[str] = pa.Field(check_name=True)
    container: str = pa.Field()
    v: float = pa.Field()


SubstationVoltageDataFrame = DataFrame[SubstationVoltageSchema]


class DisconnectedSchema(CoercingSchema):
    mrid: str = pa.Field(unique=True)


DisconnectedDataFrame = DataFrame[DisconnectedSchema]


class PowerFlowSchema(MridResourceSchema):
    p: float = pa.Field()
    q: float = pa.Field()
    in_service: bool = pa.Field()


PowerFlowDataFrame = DataFrame[PowerFlowSchema]


class BranchWithdrawSchema(MridResourceSchema):
    node: str = pa.Field()
    p: float = pa.Field()
    q: float = pa.Field()


BranchWithdrawDataFrame = DataFrame[BranchWithdrawSchema]


class DcActiveFlowSchema(MridResourceSchema):
    p: float = pa.Field()


DcActiveFlowDataFrame = DataFrame[DcActiveFlowSchema]


class RegionsSchema(MridResourceSchema):
    region: str = pa.Field()
    short_name: str = pa.Field(nullable=True)
    name: str = pa.Field()
    alias_name: str = pa.Field(nullable=True)
    region_name: str = pa.Field(nullable=True)


RegionsDataFrame = DataFrame[RegionsSchema]


class StationGroupCodeNameSchema(CoercingSchema):
    station_group: Index[str] = pa.Field(unique=True, check_name=True)
    name: str = pa.Field()
    alias_name: str = pa.Field(nullable=True)


StationGroupCodeNameDataFrame = DataFrame[StationGroupCodeNameSchema]


class HVDCBidzonesSchema(MridResourceSchema):
    bidzone_1: str = pa.Field()
    bidzone_2: str = pa.Field()


HVDCBidzonesDataFrame = DataFrame[HVDCBidzonesSchema]


class TransformerWindingsSchema(CoercingSchema):
    mrid: str = pa.Field()
    end_number: int = pa.Field(gt=0)
    w_mrid: Index[str] = pa.Field(unique=True, check_name=True)


TransformerWindingsDataFrame = DataFrame[TransformerWindingsSchema]


class SvInjectionSchema(CoercingSchema):
    node: str = pa.Field(unique=True)
    p: float = pa.Field()
    q: float = pa.Field()


SvInjectionDataFrame = DataFrame[SvInjectionSchema]


class RASEquipmentSchema(CoercingSchema):
    mrid: str = pa.Field(unique=True)
    equipment_mrid: str = pa.Field()
    name: str = pa.Field()
    flip: bool = pa.Field()
    collection: str = pa.Field()


RASEquipmentDataFrame = DataFrame[RASEquipmentSchema]


class Switches(CoercingSchema):
    mrid: Index[str] = pa.Field(unique=True, check_name=True)
    is_open: bool = pa.Field()
    equipment_type: str = pa.Field()
    connectivity_node_1: str = pa.Field()
    connectivity_node_2: str = pa.Field()
    network_enabled: bool = pa.Field()
    name: str = pa.Field()


SwitchesDataFrame = DataFrame[Switches]


class ConnectivityNode(CoercingSchema):
    mrid: Index[str] = pa.Field(unique=True, check_name=True)
    container: str = pa.Field()
    container_name: str = pa.Field()
    un: float = pa.Field(nullable=True)
    bidzone: str = pa.Field(nullable=True)
    container_type: str = pa.Field()
    base_voltage_mrid: str = pa.Field()


ConnectivityNodeDataFrame = DataFrame[ConnectivityNode]


class SvPowerDeviationSchema(CoercingSchema):
    node: str = pa.Field(unique=True)
    sum_terminal_flow: float = pa.Field()
    reported_sv_injection: float = pa.Field()
    connectivity_nodes: str = pa.Field()
    terminal_names: str = pa.Field()


SvPowerDeviationDataFrame = DataFrame[SvPowerDeviationSchema]


class HVDC(CoercingSchema):
    converter_mrid_1: str = pa.Field()
    converter_mrid_2: str = pa.Field()
    name: str = pa.Field()
    r: float = pa.Field(ge=0.0)


HVDCDataFrame = DataFrame[HVDC]


class BaseVoltage(CoercingSchema):
    mrid: str = pa.Field(unique=True)
    un: float = pa.Field()
    operating_voltage: float = pa.Field()


BaseVoltageDataFrame = DataFrame[BaseVoltage]


class AssociatedSwitches(CoercingSchema):
    mrid: str = pa.Field(unique=True)
    name: str
    switch_mrids: str
    switch_names: str


AssociatedSwitchesDataFrame = DataFrame[AssociatedSwitches]


class GenUnitAndSyncMachineMridSchema(CoercingSchema):
    gen_unit_mrid: str = pa.Field(unique=True)
    sync_machine_mrid: str = pa.Field(unique=True)


GenUnitAndSyncMachineMridDataFrame = DataFrame[GenUnitAndSyncMachineMridSchema]


class StationGroupForPowerUnitSchema(CoercingSchema):
    power_system_model_mrid: str = pa.Field(unique=True)
    market_unit_mrid: str = pa.Field()
    resource_name: str
    market_code: str


StationGroupForPowerUnitDataFrame = DataFrame[StationGroupForPowerUnitSchema]
