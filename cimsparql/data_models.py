import datetime as dt

import pandas as pd
import pandera as pa
from pandera.typing import Index, Series


class JsonSchemaOut(pa.SchemaModel):
    class Config:
        to_format = "json"
        to_format_kwargs = {"orient": "table"}


class FullModelSchema(JsonSchemaOut):
    model: Series[pd.StringDtype] = pa.Field(coerce=True)
    time: Series[pd.StringDtype] = pa.Field(coerce=True)
    profile: Series[pd.StringDtype] = pa.Field(coerce=True)
    version: Series[pd.StringDtype] = pa.Field(coerce=True)
    description: Series[pd.StringDtype] = pa.Field(coerce=True)


class MridResourceSchema(JsonSchemaOut):
    """
    Common class for resources with an mrid as index
    """

    mrid: Index[pd.StringDtype] = pa.Field(unique=True, coerce=True)


class NamedResourceSchema(MridResourceSchema):
    """
    Common class for resources with an mrid and a name
    """

    name: Series[pd.StringDtype] = pa.Field(coerce=True)


class NamedMarketResourceSchema(NamedResourceSchema):
    """
    Common class for named resources with an (optional) associated market_code
    """

    market_code: Series[pd.StringDtype] = pa.Field(nullable=True, coerce=True)


class MarketDatesSchema(NamedResourceSchema):
    activation_date: Series[dt.datetime] = pa.Field(coerce=True)


class BusDataSchema(JsonSchemaOut):
    node: Index[pd.StringDtype] = pa.Field(unique=True)
    name: Series[pd.StringDtype] = pa.Field(coerce=True)
    un: Series[float] = pa.Field(coerce=True)
    station: Series[pd.StringDtype] = pa.Field(coerce=True)
    bidzone: Series[pd.StringDtype] = pa.Field(nullable=True, coerce=True)


class LoadsSchema(NamedResourceSchema):
    node: Series[pd.StringDtype] = pa.Field(coerce=True)
    station: Series[pd.StringDtype] = pa.Field(coerce=True)
    bidzone: Series[pd.StringDtype] = pa.Field(nullable=True, coerce=True)
    status: Series[bool] = pa.Field(coerce=True)
    p: Series[float] = pa.Field(nullable=True, coerce=True)
    q: Series[float] = pa.Field(nullable=True, coerce=True)
    station_group: Series[pd.StringDtype] = pa.Field(nullable=True, coerce=True)


class WindGeneratingUnitsSchema(NamedMarketResourceSchema):
    station_group: Series[pd.StringDtype] = pa.Field(nullable=True, coerce=True)
    min_p: Series[float] = pa.Field(coerce=True)
    max_p: Series[float] = pa.Field(coerce=True)
    plant_mrid: Series[pd.StringDtype] = pa.Field(nullable=True, coerce=True)


class SynchronousMachinesSchema(NamedMarketResourceSchema):
    allocationmax: Series[float] = pa.Field(nullable=True, coerce=True)
    node: Series[pd.StringDtype] = pa.Field(coerce=True)
    status: Series[bool] = pa.Field(coerce=True)
    station_group: Series[pd.StringDtype] = pa.Field(nullable=True, coerce=True)
    station_group_name: Series[pd.StringDtype] = pa.Field(nullable=True, coerce=True)
    station: Series[pd.StringDtype] = pa.Field(coerce=True)
    maxP: Series[float] = pa.Field(nullable=True, coerce=True)
    minP: Series[float] = pa.Field(nullable=True, coerce=True)
    MO: Series[float] = pa.Field(nullable=True, coerce=True)
    bidzone: Series[pd.StringDtype] = pa.Field(nullable=True, coerce=True)
    sn: Series[float] = pa.Field(coerce=True)
    p: Series[float] = pa.Field(nullable=True, coerce=True)
    q: Series[float] = pa.Field(nullable=True, coerce=True)


class ConnectionsSchema(MridResourceSchema):
    t_mrid_1: Series[pd.StringDtype] = pa.Field(coerce=True)
    t_mrid_2: Series[pd.StringDtype] = pa.Field(coerce=True)


class BordersSchema(NamedMarketResourceSchema):
    area_1: Series[pd.StringDtype] = pa.Field(coerce=True)
    area_2: Series[pd.StringDtype] = pa.Field(coerce=True)
    t_mrid_1: Series[pd.StringDtype] = pa.Field(coerce=True)
    t_mrid_2: Series[pd.StringDtype] = pa.Field(coerce=True)


class ExchangeSchema(NamedMarketResourceSchema):
    node: Series[pd.StringDtype] = pa.Field(coerce=True)
    status: Series[bool] = pa.Field(coerce=True)
    p: Series[float] = pa.Field(coerce=True)


class ConvertersSchema(NamedResourceSchema):
    alias: Series[pd.StringDtype] = pa.Field(nullable=True, coerce=True)
    station: Series[pd.StringDtype] = pa.Field(coerce=True)
    status: Series[bool] = pa.Field(coerce=True)
    node: Series[pd.StringDtype] = pa.Field(coerce=True)


class TransfConToConverterSchema(NamedResourceSchema):
    t_mrid: Series[pd.StringDtype] = pa.Field(coerce=True)
    p_mrid: Series[pd.StringDtype] = pa.Field(coerce=True)


class BranchComponentSchema(NamedResourceSchema):
    bidzone_1: Series[pd.StringDtype] = pa.Field(nullable=True, coerce=True)
    bidzone_2: Series[pd.StringDtype] = pa.Field(nullable=True, coerce=True)
    node_1: Series[pd.StringDtype] = pa.Field(coerce=True)
    node_2: Series[pd.StringDtype] = pa.Field(coerce=True)
    ploss_1: Series[float] = pa.Field(nullable=True, coerce=True)
    ploss_2: Series[float] = pa.Field(nullable=True, coerce=True)
    r: Series[float] = pa.Field(coerce=True)
    rate: Series[float] = pa.Field(nullable=True, coerce=True)
    status: Series[bool] = pa.Field(coerce=True)
    un: Series[float] = pa.Field(coerce=True)
    x: Series[float] = pa.Field(coerce=True)


class ShuntComponentSchema(BranchComponentSchema):
    b: Series[float] = pa.Field(coerce=True)
    g: Series[float] = pa.Field(coerce=True)


class AcLinesSchema(ShuntComponentSchema):
    length: Series[float] = pa.Field(coerce=True)
    g: Series[float] = pa.Field(nullable=True, coerce=True)


class TransformersSchema(JsonSchemaOut):
    name: Series[pd.StringDtype] = pa.Field(coerce=True)
    p_mrid: Series[pd.StringDtype] = pa.Field(coerce=True)
    w_mrid: Series[pd.StringDtype] = pa.Field(coerce=True)
    endNumber: Series[int] = pa.Field(coerce=True)
    un: Series[float] = pa.Field(coerce=True)
    t_mrid: Series[pd.StringDtype] = pa.Field(coerce=True)
    r: Series[float] = pa.Field(coerce=True)
    x: Series[float] = pa.Field(coerce=True)
    rate: Series[float] = pa.Field(nullable=True, coerce=True)


class TwoWindingTransformerSchema(ShuntComponentSchema):
    angle: Series[float] = pa.Field(coerce=True)
    ratio: Series[float] = pa.Field(coerce=True)


class SubstationVoltageSchema(JsonSchemaOut):
    substation: Index[pd.StringDtype] = pa.Field(coerce=True)
    container: Series[pd.StringDtype] = pa.Field(coerce=True)
    v: Series[float] = pa.Field(coerce=True)


class DisconnectedSchema(JsonSchemaOut):
    mrid: Series[pd.StringDtype] = pa.Field(coerce=True, unique=True)


class PowerFlowSchema(MridResourceSchema):
    p: Series[float] = pa.Field(coerce=True)
    q: Series[float] = pa.Field(coerce=True)
    in_service: Series[bool] = pa.Field(coerce=True)


class BranchWithdrawSchema(MridResourceSchema):
    node: Series[pd.StringDtype] = pa.Field(coerce=True)
    p: Series[float] = pa.Field(coerce=True)
    q: Series[float] = pa.Field(coerce=True)


class DcActiveFlowSchema(MridResourceSchema):
    p: Series[float] = pa.Field(coerce=True)


class RegionsSchema(MridResourceSchema):
    region: Series[pd.StringDtype] = pa.Field(coerce=True)
    short_name: Series[pd.StringDtype] = pa.Field(nullable=True, coerce=True)
    name: Series[pd.StringDtype] = pa.Field(coerce=True)
    alias_name: Series[pd.StringDtype] = pa.Field(nullable=True, coerce=True)
    region_name: Series[pd.StringDtype] = pa.Field(nullable=True, coerce=True)
