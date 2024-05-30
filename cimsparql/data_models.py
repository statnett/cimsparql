import datetime as dt
from typing import ClassVar

import pandas as pd
import pandera as pa
from pandera.typing import DataFrame, Index, Series


class JsonSchemaOut(pa.SchemaModel):
    class Config:
        to_format = "json"
        to_format_kwargs: ClassVar[dict[str, str]] = {"orient": "table"}
        coerce = True


class FullModelSchema(JsonSchemaOut):
    model: Series[str] = pa.Field()
    time: Series[str] = pa.Field()
    profile: Series[str] = pa.Field()
    version: Series[str] = pa.Field()
    description: Series[str] = pa.Field()

    @pa.dataframe_check
    def unique_time(cls, df: DataFrame) -> bool:
        return len(df["time"].unique()) == 1


FullModelDataFrame = DataFrame[FullModelSchema]


class MridResourceSchema(JsonSchemaOut):
    """
    Common class for resources with an mrid as index
    """

    mrid: Index[str] = pa.Field(unique=True)


class NamedResourceSchema(MridResourceSchema):
    """
    Common class for resources with an mrid and a name
    """

    name: Series[str] = pa.Field()


class NamedMarketResourceSchema(NamedResourceSchema):
    """
    Common class for named resources with an (optional) associated market_code
    """

    market_code: Series[str] = pa.Field(nullable=True)


class MarketDatesSchema(NamedResourceSchema):
    activation_date: Series[dt.datetime] = pa.Field()


MarketDatesDataFrame = DataFrame[MarketDatesSchema]


class BusDataSchema(JsonSchemaOut):
    node: Index[str] = pa.Field(unique=True)
    busname: Series[str] = pa.Field()
    substation: Series[str] = pa.Field()
    un: Series[float] = pa.Field()
    substation_mrid: Series[str] = pa.Field()
    bidzone: Series[str] = pa.Field(nullable=True)
    sv_voltage: Series[float] = pa.Field(nullable=True)
    island: Series[str] = pa.Field()
    is_swing_bus: Series[bool] = pa.Field()


BusDataFrame = DataFrame[BusDataSchema]


class LoadsSchema(NamedResourceSchema):
    node: Series[str] = pa.Field()
    substation_mrid: Series[str] = pa.Field()
    bidzone: Series[str] = pa.Field(nullable=True)
    status: Series[bool] = pa.Field()
    p: Series[float] = pa.Field(nullable=True)
    q: Series[float] = pa.Field(nullable=True)
    station_group: Series[str] = pa.Field(nullable=True)
    connectivity_node: Series[str] = pa.Field()


LoadsDataFrame = DataFrame[LoadsSchema]


class WindGeneratingUnitsSchema(NamedMarketResourceSchema):
    station_group: Series[str] = pa.Field(nullable=True)
    min_p: Series[float] = pa.Field()
    max_p: Series[float] = pa.Field()
    plant_mrid: Series[str] = pa.Field(nullable=True)


WindGeneratingUnitsDataFrame = DataFrame[WindGeneratingUnitsSchema]


class SynchronousMachinesSchema(NamedMarketResourceSchema):
    node: Series[str] = pa.Field()
    status: Series[bool] = pa.Field()
    station_group: Series[str] = pa.Field(nullable=True)
    station_group_name: Series[str] = pa.Field(nullable=True)
    substation_mrid: Series[str] = pa.Field()
    maxP: Series[float] = pa.Field()
    minP: Series[float] = pa.Field()
    MO: Series[float] = pa.Field(nullable=True)
    bidzone: Series[str] = pa.Field(nullable=True)
    sn: Series[float] = pa.Field()
    p: Series[float] = pa.Field(nullable=True)
    q: Series[float] = pa.Field(nullable=True)
    connectivity_node: Series[str] = pa.Field()
    generator_type: Series[str] = pa.Field()
    schedule_resource: Series[str] = pa.Field(nullable=True)


SynchronousMachinesDataFrame = DataFrame[SynchronousMachinesSchema]


class ConnectionsSchema(MridResourceSchema):
    t_mrid_1: Series[str] = pa.Field()
    t_mrid_2: Series[str] = pa.Field()


ConnectionsDataFrame = DataFrame[ConnectionsSchema]


class BordersSchema(NamedMarketResourceSchema):
    area_1: Series[str] = pa.Field()
    area_2: Series[str] = pa.Field()
    t_mrid_1: Series[str] = pa.Field()
    t_mrid_2: Series[str] = pa.Field()


BordersDataFrame = DataFrame[BordersSchema]


class ExchangeSchema(NamedMarketResourceSchema):
    node: Series[str] = pa.Field()
    status: Series[bool] = pa.Field()
    p: Series[float] = pa.Field()


ExchangeDataFrame = DataFrame[ExchangeSchema]


class ConvertersSchema(NamedResourceSchema):
    alias: Series[str] = pa.Field(nullable=True)
    substation_mrid: Series[str] = pa.Field()
    status: Series[bool] = pa.Field()
    node: Series[str] = pa.Field()
    p: Series[float] = pa.Field()
    q: Series[float] = pa.Field()
    connectivity_node: Series[str] = pa.Field()
    controller: Series[str] = pa.Field()
    controller_factor: Series[float] = pa.Field()


ConvertersDataFrame = DataFrame[ConvertersSchema]


class TransfConToConverterSchema(NamedResourceSchema):
    t_mrid: Series[str] = pa.Field()
    p_mrid: Series[str] = pa.Field()


TransfConToConverterDataFrame = DataFrame[TransfConToConverterSchema]


class CoordinatesSchema(JsonSchemaOut):
    mrid: Series[str] = pa.Field()
    x: Series[str] = pa.Field()
    y: Series[str] = pa.Field()
    epsg: Series[pd.CategoricalDtype] = pa.Field()
    rdf_type: Series[pd.CategoricalDtype] = pa.Field()


CoordinatesDataFrame = DataFrame[CoordinatesSchema]


class BranchComponentSchema(NamedResourceSchema):
    bidzone_1: Series[str] = pa.Field(nullable=True)
    bidzone_2: Series[str] = pa.Field(nullable=True)
    node_1: Series[str] = pa.Field()
    node_2: Series[str] = pa.Field()
    ploss_1: Series[float] = pa.Field(nullable=True)
    ploss_2: Series[float] = pa.Field(nullable=True)
    r: Series[float] = pa.Field()
    rate: Series[float] = pa.Field(nullable=True)
    status: Series[bool] = pa.Field()
    un: Series[float] = pa.Field()
    x: Series[float] = pa.Field()
    connectivity_node_1: Series[str] = pa.Field()
    connectivity_node_2: Series[str] = pa.Field()


BranchComponentDataFrame = DataFrame[BranchComponentSchema]


class ShuntComponentSchema(BranchComponentSchema):
    b: Series[float] = pa.Field()
    g: Series[float] = pa.Field()


ShuntComponentDataFrame = DataFrame[ShuntComponentSchema]


class AcLinesSchema(ShuntComponentSchema):
    length: Series[float] = pa.Field()
    g: Series[float] = pa.Field(nullable=True)


AcLinesDataFrame = DataFrame[AcLinesSchema]


class TransformersSchema(JsonSchemaOut):
    name: Series[str] = pa.Field()
    p_mrid: Series[str] = pa.Field()
    w_mrid: Series[str] = pa.Field()
    end_number: Series[int] = pa.Field()
    un: Series[float] = pa.Field()
    t_mrid: Series[str] = pa.Field()
    r: Series[float] = pa.Field()
    x: Series[float] = pa.Field()
    rate: Series[float] = pa.Field(nullable=True)


TransformersDataFrame = DataFrame[TransformersSchema]


class TransformerWindingSchema(ShuntComponentSchema):
    angle: Series[float] = pa.Field()
    ratio: Series[float] = pa.Field()


TransformerWindingDataFrame = DataFrame[TransformerWindingSchema]


class SubstationVoltageSchema(JsonSchemaOut):
    substation: Index[str] = pa.Field()
    container: Series[str] = pa.Field()
    v: Series[float] = pa.Field()


SubstationVoltageDataFrame = DataFrame[SubstationVoltageSchema]


class DisconnectedSchema(JsonSchemaOut):
    mrid: Series[str] = pa.Field(unique=True)


DisconnectedDataFrame = DataFrame[DisconnectedSchema]


class PowerFlowSchema(MridResourceSchema):
    p: Series[float] = pa.Field()
    q: Series[float] = pa.Field()
    in_service: Series[bool] = pa.Field()


PowerFlowDataFrame = DataFrame[PowerFlowSchema]


class BranchWithdrawSchema(MridResourceSchema):
    node: Series[str] = pa.Field()
    p: Series[float] = pa.Field()
    q: Series[float] = pa.Field()


BranchWithdrawDataFrame = DataFrame[BranchWithdrawSchema]


class DcActiveFlowSchema(MridResourceSchema):
    p: Series[float] = pa.Field()


DcActiveFlowDataFrame = DataFrame[DcActiveFlowSchema]


class RegionsSchema(MridResourceSchema):
    region: Series[str] = pa.Field()
    short_name: Series[str] = pa.Field(nullable=True)
    name: Series[str] = pa.Field()
    alias_name: Series[str] = pa.Field(nullable=True)
    region_name: Series[str] = pa.Field(nullable=True)


RegionsDataFrame = DataFrame[RegionsSchema]


class StationGroupCodeNameSchema(JsonSchemaOut):
    station_group: Index[str] = pa.Field(unique=True)
    name: Series[str] = pa.Field()
    alias_name: Series[str] = pa.Field(nullable=True)


StationGroupCodeNameDataFrame = DataFrame[StationGroupCodeNameSchema]


class HVDCBidzonesSchema(MridResourceSchema):
    bidzone_1: Series[str] = pa.Field()
    bidzone_2: Series[str] = pa.Field()


HVDCBidzonesDataFrame = DataFrame[HVDCBidzonesSchema]


class TransformerWindingsSchema(JsonSchemaOut):
    mrid: Series[str] = pa.Field()
    end_number: Series[int] = pa.Field(gt=0)
    w_mrid: Index[str] = pa.Field(unique=True)


TransformerWindingsDataFrame = DataFrame[TransformerWindingsSchema]


class SvInjectionSchema(JsonSchemaOut):
    node: Series[str] = pa.Field(unique=True)
    p: Series[float] = pa.Field()
    q: Series[float] = pa.Field()


SvInjectionDataFrame = DataFrame[SvInjectionSchema]


class RASEquipmentSchema(JsonSchemaOut):
    mrid: Series[str] = pa.Field(unique=True)
    equipment_mrid: Series[str] = pa.Field()
    name: Series[str] = pa.Field()


RASEquipmentDataFrame = DataFrame[RASEquipmentSchema]


class Switches(JsonSchemaOut):
    mrid: Index[str] = pa.Field(unique=True)
    is_open: Series[bool] = pa.Field()
    equipment_type: Series[str] = pa.Field()
    connectivity_node_1: Series[str] = pa.Field()
    connectivity_node_2: Series[str] = pa.Field()


SwitchesDataFrame = DataFrame[Switches]


class ConnectivityNode(JsonSchemaOut):
    mrid: Index[str] = pa.Field(unique=True)
    container: Series[str] = pa.Field()
    container_name: Series[str] = pa.Field()
    un: Series[float] = pa.Field(nullable=True)
    bidzone: Series[str] = pa.Field(nullable=True)
    container_type: Series[str] = pa.Field()


ConnectivityNodeDataFrame = DataFrame[ConnectivityNode]


class SvPowerDeviationSchema(JsonSchemaOut):
    node: Series[str] = pa.Field(unique=True)
    sum_terminal_flow: Series[float] = pa.Field()
    reported_sv_injection: Series[float] = pa.Field()
    connectivity_nodes: Series[str] = pa.Field()
    terminal_names: Series[str] = pa.Field()


SvPowerDeviationDataFrame = DataFrame[SvPowerDeviationSchema]
