import datetime as dt

import pandas as pd
import pandera as pa
from pandera.typing import DataFrame, Index, Series


class JsonSchemaOut(pa.SchemaModel):
    class Config:
        to_format = "json"
        to_format_kwargs = {"orient": "table"}
        coerce = True


class FullModelSchema(JsonSchemaOut):
    model: Series[pd.StringDtype] = pa.Field()
    time: Series[pd.StringDtype] = pa.Field()
    profile: Series[pd.StringDtype] = pa.Field()
    version: Series[pd.StringDtype] = pa.Field()
    description: Series[pd.StringDtype] = pa.Field()

    @pa.dataframe_check
    def unique_time(cls, df: DataFrame) -> bool:
        return len(df["time"].unique()) == 1


class MridResourceSchema(JsonSchemaOut):
    """
    Common class for resources with an mrid as index
    """

    mrid: Index[pd.StringDtype] = pa.Field(unique=True)


class NamedResourceSchema(MridResourceSchema):
    """
    Common class for resources with an mrid and a name
    """

    name: Series[pd.StringDtype] = pa.Field()


class NamedMarketResourceSchema(NamedResourceSchema):
    """
    Common class for named resources with an (optional) associated market_code
    """

    market_code: Series[pd.StringDtype] = pa.Field(nullable=True)


class MarketDatesSchema(NamedResourceSchema):
    activation_date: Series[dt.datetime] = pa.Field()


class BusDataSchema(JsonSchemaOut):
    node: Index[pd.StringDtype] = pa.Field(
        unique=True,
    )
    name: Series[pd.StringDtype] = pa.Field()
    busname: Series[pd.StringDtype] = pa.Field()
    un: Series[float] = pa.Field()
    station: Series[pd.StringDtype] = pa.Field()
    bidzone: Series[pd.StringDtype] = pa.Field(nullable=True)


class LoadsSchema(NamedResourceSchema):
    node: Series[pd.StringDtype] = pa.Field()
    station: Series[pd.StringDtype] = pa.Field()
    bidzone: Series[pd.StringDtype] = pa.Field(nullable=True)
    status: Series[bool] = pa.Field()
    p: Series[float] = pa.Field(nullable=True)
    q: Series[float] = pa.Field(nullable=True)
    station_group: Series[pd.StringDtype] = pa.Field(nullable=True)


class WindGeneratingUnitsSchema(NamedMarketResourceSchema):
    station_group: Series[pd.StringDtype] = pa.Field(nullable=True)
    min_p: Series[float] = pa.Field()
    max_p: Series[float] = pa.Field()
    plant_mrid: Series[pd.StringDtype] = pa.Field(nullable=True)


class SynchronousMachinesSchema(NamedMarketResourceSchema):
    allocationmax: Series[float] = pa.Field(nullable=True)
    node: Series[pd.StringDtype] = pa.Field()
    status: Series[bool] = pa.Field()
    station_group: Series[pd.StringDtype] = pa.Field(nullable=True)
    station_group_name: Series[pd.StringDtype] = pa.Field(nullable=True)
    station: Series[pd.StringDtype] = pa.Field()
    maxP: Series[float] = pa.Field(nullable=True)
    minP: Series[float] = pa.Field(nullable=True)
    MO: Series[float] = pa.Field(nullable=True)
    bidzone: Series[pd.StringDtype] = pa.Field(nullable=True)
    sn: Series[float] = pa.Field()
    p: Series[float] = pa.Field(nullable=True)
    q: Series[float] = pa.Field(nullable=True)


class ConnectionsSchema(MridResourceSchema):
    t_mrid_1: Series[pd.StringDtype] = pa.Field()
    t_mrid_2: Series[pd.StringDtype] = pa.Field()


class BordersSchema(NamedMarketResourceSchema):
    area_1: Series[pd.StringDtype] = pa.Field()
    area_2: Series[pd.StringDtype] = pa.Field()
    t_mrid_1: Series[pd.StringDtype] = pa.Field()
    t_mrid_2: Series[pd.StringDtype] = pa.Field()


class ExchangeSchema(NamedMarketResourceSchema):
    node: Series[pd.StringDtype] = pa.Field()
    status: Series[bool] = pa.Field()
    p: Series[float] = pa.Field()


class ConvertersSchema(NamedResourceSchema):
    alias: Series[pd.StringDtype] = pa.Field(nullable=True)
    station: Series[pd.StringDtype] = pa.Field()
    status: Series[bool] = pa.Field()
    node: Series[pd.StringDtype] = pa.Field()


class TransfConToConverterSchema(NamedResourceSchema):
    t_mrid: Series[pd.StringDtype] = pa.Field()
    p_mrid: Series[pd.StringDtype] = pa.Field()


class CoordinatesSchema(JsonSchemaOut):
    mrid: Series[pd.StringDtype] = pa.Field()
    x: Series[pd.StringDtype] = pa.Field()
    y: Series[pd.StringDtype] = pa.Field()
    epsg: Series[pd.CategoricalDtype] = pa.Field()
    rdf_type: Series[pd.CategoricalDtype] = pa.Field()


class BranchComponentSchema(NamedResourceSchema):
    bidzone_1: Series[pd.StringDtype] = pa.Field(nullable=True)
    bidzone_2: Series[pd.StringDtype] = pa.Field(nullable=True)
    node_1: Series[pd.StringDtype] = pa.Field()
    node_2: Series[pd.StringDtype] = pa.Field()
    ploss_1: Series[float] = pa.Field(nullable=True)
    ploss_2: Series[float] = pa.Field(nullable=True)
    r: Series[float] = pa.Field()
    rate: Series[float] = pa.Field(nullable=True)
    status: Series[bool] = pa.Field()
    un: Series[float] = pa.Field()
    x: Series[float] = pa.Field()


class ShuntComponentSchema(BranchComponentSchema):
    b: Series[float] = pa.Field()
    g: Series[float] = pa.Field()


class AcLinesSchema(ShuntComponentSchema):
    length: Series[float] = pa.Field()
    g: Series[float] = pa.Field(nullable=True)


class TransformersSchema(JsonSchemaOut):
    name: Series[pd.StringDtype] = pa.Field()
    p_mrid: Series[pd.StringDtype] = pa.Field()
    w_mrid: Series[pd.StringDtype] = pa.Field()
    endNumber: Series[int] = pa.Field()
    un: Series[float] = pa.Field()
    t_mrid: Series[pd.StringDtype] = pa.Field()
    r: Series[float] = pa.Field()
    x: Series[float] = pa.Field()
    rate: Series[float] = pa.Field(nullable=True)


class TransformerWindingSchema(ShuntComponentSchema):
    angle: Series[float] = pa.Field()
    ratio: Series[float] = pa.Field()


class SubstationVoltageSchema(JsonSchemaOut):
    substation: Index[pd.StringDtype] = pa.Field()
    container: Series[pd.StringDtype] = pa.Field()
    v: Series[float] = pa.Field()


class DisconnectedSchema(JsonSchemaOut):
    mrid: Series[pd.StringDtype] = pa.Field(unique=True)


class PowerFlowSchema(MridResourceSchema):
    p: Series[float] = pa.Field()
    q: Series[float] = pa.Field()
    in_service: Series[bool] = pa.Field()


class BranchWithdrawSchema(MridResourceSchema):
    node: Series[pd.StringDtype] = pa.Field()
    p: Series[float] = pa.Field()
    q: Series[float] = pa.Field()


class DcActiveFlowSchema(MridResourceSchema):
    p: Series[float] = pa.Field()


class RegionsSchema(MridResourceSchema):
    region: Series[pd.StringDtype] = pa.Field()
    short_name: Series[pd.StringDtype] = pa.Field(nullable=True)
    name: Series[pd.StringDtype] = pa.Field()
    alias_name: Series[pd.StringDtype] = pa.Field(nullable=True)
    region_name: Series[pd.StringDtype] = pa.Field(nullable=True)


class StationGroupCodeNameSchema(JsonSchemaOut):
    station_group: Index[pd.StringDtype] = pa.Field(unique=True)
    name: Series[pd.StringDtype] = pa.Field()
    alias_name: Series[pd.StringDtype] = pa.Field(nullable=True)
