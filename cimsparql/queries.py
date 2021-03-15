import copy
from typing import Iterable, List, Set, Tuple, Union

import networkx as nx
import numpy as np
import pandas as pd

allowed_load_types = ("ConformLoad", "NonConformLoad", "EnergyConsumer")

con_mrid_str = "connectivity_mrid"
connectivity_columns = [f"{con_mrid_str}_{nr}" for nr in [1, 2]]

ratings = ("Normal", "Warning", "Overload")


def combine_statements(*args, group: bool = False, split: str = "\n") -> str:
    """Join *args

    Args:
       group: return enclosed by {...}
       split: join *args by this

    Example:
       >>> import os
       >>> where_list = ['?mrid rdf:type cim:ACLineSegment', '?mrid cim:ACLineSegment.r ?r']
       >>> combine_statements(where_list,group=True, split=' '+ os.sep)
    """
    return "{\n" + split.join(args) + "\n}" if group else split.join(args)


def xsd_type(cim: str, var: str) -> str:
    return f"^^<{cim}{var}>"


def negpos(val: Union[float, int]) -> str:
    return "minus" if val < 0 else "plus"


def version_date() -> str:
    select_query = "SELECT ?mrid ?name ?activationDate"
    where_list = [
        "?marketDefinitionSet rdf:type SN:MarketDefinitionSet",
        "?marketDefinitionSet cim:IdentifiedObject.mRID ?mrid",
        "?marketDefinitionSet cim:IdentifiedObject.name ?name",
        "?marketDefinitionSet SN:MarketDefinitionSet.activationDate ?activationDate",
        "FILTER regex(?name, 'ScheduleResource')",
    ]
    return combine_statements(select_query, group_query(where_list))


def temperature_list(temperature: float, xsd: str) -> List[str]:
    sign = negpos(temperature)
    mrid = f"?t{sign}_{abs(temperature)}"
    return [
        f"{mrid} ALG:TemperatureCurveData.Curve ?tcur",
        f"{mrid} ALG:TemperatureCurveData.temperature '{temperature:0.1f}'{xsd}",
        f"{mrid} ALG:TemperatureCurveData.percent ?{sign}_{abs(temperature)}_factor",
    ]


def temp_correction_factors(
    mrid: str, cim: str, temperatures: Iterable = tuple(range(-30, 30, 10))
):
    where_list = [
        "?temp_mrid rdf:type ALG:TemperatureCurveDependentLimit",
        f"?temp_mrid ALG:LimitDependency.Equipment {mrid}",
        "?temp_mrid ALG:TemperatureCurveDependentLimit.TemperatureCurve ?tcur",
    ]
    xsd = xsd_type(cim, "Temperature")
    for temperature in temperatures:
        where_list += temperature_list(temperature, xsd)
    return where_list


def group_query(
    x: List[str], command: str = "WHERE", split: str = " .\n", group: bool = True
) -> str:
    """Group Query

    Args:
       x: List of objects to group
       command: to operate on group
       split, group: (see: combine_statements)

    Example:
       >>> import os
       >>> where_list = ['?mrid rdf:type cim:ACLineSegment', '?mrid cim:ACLineSegment.r ?r']
       >>> group_query(where_list, group=True, split= ' .' + os.sep)
    """
    return command + " " + combine_statements(*x, group=group, split=split)


def unionize(*args: str, group: bool = True):
    if group:
        args = tuple(f"{{\n{arg}\n}}" for arg in args)
    return "\nUNION\n".join(args)


def regions_query() -> str:
    select_query = "SELECT ?mrid ?aliasName  ?name ?shortName ?region ?region_name"
    where_list = [
        "?mrid rdf:type cim:SubGeographicalRegion",
        "?mrid cim:IdentifiedObject.aliasName ?aliasName",
        "?mrid cim:IdentifiedObject.name ?name",
        "?mrid SN:IdentifiedObject.shortName ?shortName",
        "?mrid cim:SubGeographicalRegion.Region ?sub_geographical_region",
        "?sub_geographical_region cim:IdentifiedObject.name ?region",
        "?sub_geographical_region cim:IdentifiedObject.aliasName ?region_name",
    ]
    return combine_statements(select_query, group_query(where_list))


def market_code_query(nr: int = None):
    nr_s = "" if nr is None else f"_{nr}"
    return group_query(
        [
            f"?t_mrid{nr_s} cim:Terminal.ConnectivityNode ?con{nr_s}",
            f"?con{nr_s} cim:ConnectivityNode.ConnectivityNodeContainer ?container{nr_s}",
            f"?container{nr_s} cim:VoltageLevel.Substation ?substation{nr_s}",
            f"?substation{nr_s} SN:Substation.MarketDeliveryPoint ?m_d_p{nr_s}",
            f"?m_d_p{nr_s} SN:MarketDeliveryPoint.BiddingArea ?barea{nr_s}",
            f"?barea{nr_s} SN:BiddingArea.marketCode ?bidzone{nr_s}",
        ],
        command="OPTIONAL",
    )


def _region_name_query(region: str, sub_region: bool, sub_geographical_region: str) -> List[str]:
    if sub_region:
        return [f"{sub_geographical_region} SN:IdentifiedObject.shortName {region}"]
    return [
        f"{sub_geographical_region} cim:SubGeographicalRegion.Region ?region",
        f"?region cim:IdentifiedObject.name {region}",
    ]


def region_query(region: Union[str, List[str]], sub_region: bool, container: str) -> List[str]:
    if region is None:
        query = []
    else:
        sub_geographical_region = "?subgeoreg"
        query = [f"?{container} cim:{container}.Region {sub_geographical_region}"]
        if isinstance(region, str):
            query += _region_name_query(f"'{region}'", sub_region, sub_geographical_region)
        elif isinstance(region, List):
            query += _region_name_query("?r_na", sub_region, sub_geographical_region)
            query += ["FILTER regex(?r_na, '" + "|".join(region) + "')"]
        else:
            raise NotImplementedError("region must be either str or List")
    return query


def connectivity_mrid(
    var: str = con_mrid_str, sparql: bool = True, sequence_numbers: Iterable[int] = (1, 2)
) -> Union[str, List[str]]:
    if sparql:
        return " ".join([f"?{var}_{i}" for i in sequence_numbers])
    return [f"{var}_{i}" for i in sequence_numbers]


def acdc_terminal(cim_version: int) -> str:
    return "ACDCTerminal" if cim_version > 15 else "Terminal"


def terminal_where_query(
    cim_version: int = 15, var: str = con_mrid_str, with_sequence_number: bool = False
) -> List[str]:
    out = [
        "?terminal_mrid rdf:type cim:Terminal",
        "?terminal_mrid cim:Terminal.ConductingEquipment ?mrid",
    ]
    if var is not None:
        out += [f"?terminal_mrid cim:Terminal.ConnectivityNode ?{var}"]

    if with_sequence_number:
        out += [f"?terminal_mrid cim:{acdc_terminal(cim_version)}.sequenceNumber ?sequenceNumber"]
    return out


bid_market_code_query = [
    "?mrid cim:Equipment.EquipmentContainer ?eq_container",
    "?eq_container cim:VoltageLevel.Substation ?substation",
    "?substation SN:Substation.MarketDeliveryPoint ?m_d_p",
    "?m_d_p SN:MarketDeliveryPoint.BiddingArea ?barea",
    "?barea SN:BiddingArea.marketCode ?bidzone",
]


def phase_tap_changer_query(
    region: str, sub_region: bool, with_tap_changer_values: bool, impedance: List[str]
) -> str:
    select_query = "SELECT ?mrid"

    where_list = [
        "?mrid rdf:type cim:PowerTransformerEnd",
        "?mrid cim:TransformerEnd.PhaseTapChanger ?tap",
        "?mrid cim:PowerTransformerEnd.PowerTransformer ?pt",
    ]

    if with_tap_changer_values:
        select_query += " ?tap ?low ?high ?neutral ?phase_incr"
        where_list += [
            "?tap cim:TapChanger.highStep ?high",
            "?tap cim:TapChanger.lowStep ?low",
            "?tap cim:TapChanger.neutralStep ?neutral",
            "?tap cim:PhaseTapChangerLinear.stepPhaseShiftIncrement ?phase_incr",
        ]

    if impedance is not None:
        for imp in impedance:
            select_query += f" ?{imp}"
            where_list += [f"?pte_1 cim:PowerTransformerEnd.{imp} ?{imp}"]

    if region is not None:
        where_list += [f"?pt cim:Equipment.EquipmentContainer ?Substation"]
        where_list += region_query(region, sub_region, "Substation")

    for i in [1, 2]:
        select_query += f" ?t_mrid_{i}"
        where_list += [
            f"?pte_{i} cim:PowerTransformerEnd.PowerTransformer ?pt",
            f"?pte_{i} cim:TransformerEnd.Terminal ?term_{i}",
            f"?term_{i} rdf:type cim:Terminal",
            f'?term_{i} cim:Terminal.sequenceNumber "{i}"^^xsd:integer',
            f"?term_{i} cim:IdentifiedObject.mRID ?t_mrid_{i}",
        ]

    return combine_statements(select_query, group_query(where_list))


def terminal_sequence_query(
    cim_version: int, sequence_numbers: Iterable[int] = (1, 2), var: str = con_mrid_str
) -> List[str]:
    query_list = []
    for i in sequence_numbers:
        mrid = f"?t_mrid_{i} "
        query_list += [
            mrid + "rdf:type cim:Terminal",
            mrid + "cim:Terminal.ConductingEquipment ?mrid",
            mrid + f"cim:{acdc_terminal(cim_version)}.sequenceNumber {i}",
        ]
        if var is not None:
            query_list += [mrid + f"cim:Terminal.ConnectivityNode ?{var}_{i}"]

    return query_list


def connectivity_names() -> str:
    select_query = "SELECT ?mrid ?name"
    where_list = ["?mrid rdf:type cim:ConnectivityNode", "?mrid cim:IdentifiedObject.name ?name"]
    return combine_statements(select_query, group_query(where_list))


def bus_data(region: Union[str, List[str]], sub_region: bool = False) -> str:
    container = "Substation"
    select_query = "SELECT ?mrid ?name"
    where_list = ["?mrid rdf:type cim:TopologicalNode", "?mrid cim:IdentifiedObject.name ?name"]

    if region is not None:
        where_list += [
            "?mrid cim:TopologicalNode.ConnectivityNodeContainer ?container",
            f"?container cim:VoltageLevel.Substation ?{container}",
        ]
        where_list += region_query(region, sub_region, container)

    return combine_statements(select_query, group_query(where_list))


def load_query(  # pylint: disable=too-many-arguments
    load_type: List[str],
    load_vars: Iterable[str],
    region: Union[str, List[str]],
    sub_region: bool = False,
    connectivity: str = con_mrid_str,
    station_group_optional: bool = True,
    with_sequence_number: bool = False,
    network_analysis: bool = True,
    station_group: bool = False,
    cim_version: int = 15,
) -> str:

    if not set(load_type).issubset(allowed_load_types) or len(load_type) == 0:
        raise ValueError(f"load_type should be any combination of {allowed_load_types}")

    container = "Substation"
    p_vars = [] if load_vars is None else load_vars
    select_query = "SELECT ?mrid ?terminal_mrid ?bidzone " + " ".join([f"?{p}" for p in p_vars])
    if with_sequence_number:
        select_query += " ?sequenceNumber"

    if connectivity is not None:
        select_query += f" ?{connectivity}"

    cim_types = [f"?mrid rdf:type cim:{cim_type}" for cim_type in load_type]
    where_list = [combine_statements(*cim_types, group=len(cim_types) > 1, split="\n} UNION \n {")]
    where_list += [
        group_query([f"?mrid cim:EnergyConsumer.{p} ?{p}" for p in p_vars], command="OPTIONAL")
    ]
    where_list += terminal_where_query(cim_version, connectivity, with_sequence_number)
    where_list += bid_market_code_query

    if station_group:
        select_query += " ?station_group"
        station_group_list = [
            "?mrid cim:NonConformLoad.LoadGroup ?lg",
            "?lg SN:NonConformLoadGroup.ScheduleResource ?sc_res",
            "?sc_res SN:ScheduleResource.marketCode ?station_group",
        ]
        if station_group_optional:
            where_list += [group_query(station_group_list, command="OPTIONAL")]
        else:
            where_list += station_group_list

    if network_analysis is not None:
        where_list += [f"?mrid SN:Equipment.networkAnalysisEnable {network_analysis}"]

    if region is not None:
        where_list += [
            "?mrid cim:Equipment.EquipmentContainer ?container",
            f"?container cim:VoltageLevel.Substation ?{container}",
        ] + region_query(region, sub_region, container)

    return combine_statements(select_query, group_query(where_list))


def synchronous_machines_query(  # pylint: disable=too-many-arguments
    sync_vars: Iterable[str],
    region: Union[str, List[str]],
    sub_region: bool = False,
    connectivity: str = con_mrid_str,
    station_group_optional: bool = True,
    cim_version: int = 15,
    with_sequence_number: bool = False,
    network_analysis: bool = True,
    u_groups: bool = False,
) -> str:
    var_dict = {"sn": "ratedS", "p": "p", "q": "q"}
    var_dict = {k: var_dict[k] for k in sync_vars}

    select_query = (
        "SELECT ?mrid ?name ?terminal_mrid ?station_group ?market_code ?maxP ?allocationMax "
        "?allocationWeight ?minP ?bidzone" + " ".join([f"?{var}" for var in sync_vars])
    )
    if connectivity is not None:
        select_query += f" ?{connectivity}"

    if with_sequence_number:
        select_query += " ?sequenceNumber"

    where_list = [
        "?mrid rdf:type cim:SynchronousMachine",
        "?mrid cim:IdentifiedObject.name ?name",
        "?mrid cim:SynchronousMachine.maxQ ?maxQ",
        "?mrid cim:SynchronousMachine.minQ ?minQ",
        group_query(
            ["?mrid cim:SynchronousMachine.type ?machine", "?machine rdfs:label 'generator'"],
            command="OPTIONAL",
        ),
    ]

    if network_analysis is not None:
        where_list += [f"?mrid SN:Equipment.networkAnalysisEnable {network_analysis}"]

    where_list += bid_market_code_query

    where_list += [
        group_query([f"?mrid cim:RotatingMachine.{var_dict[var]} ?{var}"], command="OPTIONAL")
        for var in sync_vars
    ]
    station_group = [
        "?mrid cim:SynchronousMachine.GeneratingUnit ?gu",
        "?gu SN:GeneratingUnit.marketCode ?market_code",
        "?gu cim:GeneratingUnit.maxOperatingP ?maxP",
        "?gu cim:GeneratingUnit.minOperatingP ?minP",
        "?gu SN:GeneratingUnit.groupAllocationMax ?allocationMax",
        "?gu SN:GeneratingUnit.groupAllocationWeight ?allocationWeight",
        "?gu SN:GeneratingUnit.ScheduleResource ?ScheduleResource",
        "?ScheduleResource SN:ScheduleResource.marketCode ?station_group",
        "?ScheduleResource cim:IdentifiedObject.aliasName ?st_gr_n",
    ]
    if station_group_optional:
        where_list += [group_query(station_group, command="OPTIONAL")]
    else:
        where_list += station_group
    where_list += terminal_where_query(cim_version, connectivity, with_sequence_number)

    if not u_groups:
        where_list += ["FILTER (!bound(?st_gr_n) || (!regex(?st_gr_n, 'U-')))"]
    if region is not None:
        container = "Substation"
        where_list += [
            "?mrid cim:Equipment.EquipmentContainer ?container",
            f"?container cim:VoltageLevel.Substation ?{container}",
        ] + region_query(region, sub_region, container)
    return combine_statements(select_query, group_query(where_list))


def operational_limit(mrid: str, rate: str, limitset: str = "oplimset") -> List[str]:
    return [
        f"?{limitset} cim:OperationalLimitSet.Equipment {mrid}",
        f"?activepowerlimit{rate} cim:OperationalLimit.OperationalLimitSet ?{limitset}",
        f"?activepowerlimit{rate} rdf:type cim:ActivePowerLimit",
        f"?activepowerlimit{rate} cim:IdentifiedObject.name '{rate}@20'",
        f"?activepowerlimit{rate} cim:ActivePowerLimit.value ?rate{rate}",
    ]


def wind_generating_unit_query(network_analysis: bool = True):

    select_query = (
        "SELECT ?mrid ?station_group ?market_code ?maxP "
        "?allocationMax ?allocationWeight ?minP ?name ?power_plant_mrid"
    )
    where_list = [
        "?mrid rdf:type cim:WindGeneratingUnit",
        "?mrid cim:GeneratingUnit.maxOperatingP ?maxP",
        "?mrid SN:GeneratingUnit.marketCode ?market_code",
        "?mrid cim:GeneratingUnit.minOperatingP ?minP",
        "?mrid cim:IdentifiedObject.name ?name",
        "?mrid SN:WindGeneratingUnit.WindPowerPlant ?power_plant_mrid",
        "?mrid SN:GeneratingUnit.groupAllocationMax ?allocationMax",
        "?mrid SN:GeneratingUnit.groupAllocationWeight ?allocationWeight",
        "?mrid SN:GeneratingUnit.ScheduleResource ?sr",
        "?sr SN:ScheduleResource.marketCode ?station_group",
    ]

    if network_analysis is not None:
        where_list += [f"?mrid SN:Equipment.networkAnalysisEnable {network_analysis}"]

    return combine_statements(select_query, group_query(where_list))


def transformer_query(
    region: Union[str, List[str]],
    sub_region: bool = False,
    connectivity: str = con_mrid_str,
    rates: Iterable[str] = ratings,
    network_analysis: bool = True,
    with_market: bool = False,
) -> str:
    container = "Substation"

    select_query = [
        "SELECT",
        "?name",
        "?mrid",
        "?w_mrid",
        "?x",
        "?r",
        "?endNumber",
        "?un",
        "?t_mrid",
    ]

    where_list = [
        "?mrid rdf:type cim:PowerTransformer",
        "?w_mrid cim:PowerTransformerEnd.PowerTransformer ?mrid",
        "?w_mrid cim:PowerTransformerEnd.x ?x",
        "?w_mrid cim:PowerTransformerEnd.r ?r",
        "?w_mrid cim:PowerTransformerEnd.ratedU ?un",
        "?w_mrid cim:TransformerEnd.endNumber ?endNumber",
        "?w_mrid cim:TransformerEnd.Terminal ?t_mrid",
        "?w_mrid cim:IdentifiedObject.name ?name",
    ]

    if with_market:
        select_query += ["?bidzone"]
        where_list += [market_code_query()]

    if network_analysis is not None:
        where_list += [f"?mrid SN:Equipment.networkAnalysisEnable {network_analysis}"]

    if connectivity is not None:
        select_query += [f"?{connectivity}"]
        where_list += [f"?t_mrid cim:Terminal.ConnectivityNode ?{connectivity}"]

    if region is not None:
        where_list += [f"?mrid cim:Equipment.EquipmentContainer ?{container}"]
        where_list += region_query(region, sub_region, container)

    if rates:
        limitset = "operationallimitset"
        where_rate = [f"?{limitset} cim:OperationalLimitSet.Terminal ?t_mrid"]

        for rate in rates:
            select_query += [f"?rate{rate}"]
            where_rate += operational_limit("?mrid", rate, limitset)
        where_list += [group_query(where_rate, command="OPTIONAL")]
    return combine_statements(" ".join(select_query), group_query(where_list))


def series_compensator_query(
    cim_version: int,
    region: Union[str, List[str]],
    sub_region: bool = False,
    connectivity: str = con_mrid_str,
    network_analysis: bool = True,
    with_market: bool = False,
):
    container = "Substation"

    select_query = ["SELECT", "?mrid", "?x", "?un", "?name", "?t_mrid_1", "?t_mrid_2"]

    if connectivity is not None:
        select_query += [f"{connectivity_mrid(connectivity)}"]

    where_list = terminal_sequence_query(cim_version=cim_version, var=connectivity)
    if with_market:
        terminals = [1, 2]
        select_query += [f"?bidzone_{nr}" for nr in terminals]
        for terminal_nr in terminals:
            where_list += [market_code_query(terminal_nr)]

    where_list += [
        "?mrid rdf:type cim:SeriesCompensator",
        "?mrid cim:SeriesCompensator.x ?x",
        "?mrid cim:ConductingEquipment.BaseVoltage ?obase",
        "?obase cim:BaseVoltage.nominalVoltage ?un",
        "?mrid cim:IdentifiedObject.name ?name",
    ]
    if network_analysis is not None:
        where_list += [f"?mrid SN:Equipment.networkAnalysisEnable {network_analysis}"]

    if region is not None:
        where_list += [
            "?mrid cim:Equipment.EquipmentContainer ?EquipmentContainer",
            "?EquipmentContainer cim:VoltageLevel.Substation ?Substation",
        ]
        where_list += region_query(region, sub_region, container)

    return combine_statements(" ".join(select_query), group_query(where_list))


def transformers_connected_to_converter(
    region: str, sub_region: bool, converter_types: List[str]
) -> str:
    select_query = "select ?mrid ?t_mrid ?name"
    where_list = [
        "?mrid rdf:type cim:PowerTransformer",
        "?mrid cim:IdentifiedObject.aliasName ?name",
        "?t_mrid cim:Terminal.ConductingEquipment ?mrid",
        "?t_mrid cim:Terminal.ConnectivityNode ?con",
        "?t_tvolt cim:Terminal.ConductingEquipment ?volt",
        "?t_tvolt cim:Terminal.ConnectivityNode ?con",
    ]
    converters = [f"?volt rdf:type ALG:{converter}Converter" for converter in converter_types]
    where_list += [
        combine_statements(*converters, group=len(converters) > 1, split="\n} UNION \n {")
    ]
    if region is not None:
        where_list += [f"?mrid cim:Equipment.EquipmentContainer ?Substation"]
        where_list += region_query(region, sub_region, "Substation")
    return combine_statements(select_query, group_query(where_list))


def borders_query(
    cim_version: int,
    region: Union[str, List[str]],
    sub_region: bool,
    ignore_hvdc: bool,
    with_market_code: bool,
    market_optional: bool,
) -> str:
    select_query = ["SELECT", "?name", "?mrid"]
    where_list = terminal_sequence_query(cim_version=cim_version, var="con")
    where_list += ["?mrid cim:IdentifiedObject.name ?name", "?mrid rdf:type cim:ACLineSegment"]
    for nr in [1, 2]:
        select_query += [f"?t_mrid_{nr}", f"?area_{nr}"]
        where_list += [
            f"?con_{nr} cim:ConnectivityNode.ConnectivityNodeContainer ?cont_{nr}",
            f"?cont_{nr} cim:VoltageLevel.Substation ?subs_{nr}",
            f"?subs_{nr} cim:Substation.Region ?reg_{nr}",
            f"?reg_{nr} cim:SubGeographicalRegion.Region ?sreg_{nr}",
            f"?sreg_{nr} cim:IdentifiedObject.name ?area_{nr}",
        ]
    if with_market_code:
        select_query += [f"?market_code"]
        where_marked = [
            "?mrid cim:Equipment.EquipmentContainer ?line_cont",
            "?line_cont SN:Line.marketCode ?market_code",
        ]
        where_list += [group_query(where_marked, command="OPTIONAL" if market_optional else "")]

    regions = "|".join(region) if isinstance(region, list) else region
    filters = [
        combine_statements(
            f"FILTER (regex(?area_1, '{regions}'))", f"FILTER (!regex(?area_2, '{regions}'))"
        ),
        combine_statements(
            f"FILTER (regex(?area_2, '{regions}'))", f"FILTER (!regex(?area_1, '{regions}'))"
        ),
    ]
    where_list += [combine_statements(*filters, group=True, split="\n} UNION\n{\n")]
    if ignore_hvdc:
        where_list += ["{FILTER (!regex(?name, 'HVDC'))}"]

    return combine_statements(" ".join(select_query), group_query(where_list))


def ac_line_query(  # pylint: disable=too-many-arguments
    cim_version: int,
    cim: str,
    region: Union[str, List[str]],
    sub_region: bool = False,
    connectivity: str = con_mrid_str,
    rates: Iterable[str] = ratings,
    network_analysis: bool = True,
    with_market: bool = True,
    temperatures: List = None,
) -> str:
    container = "Line"

    select_query = [
        "SELECT",
        "?name",
        "?mrid",
        "?x",
        "?r",
        "?bch",
        "?length",
        "?un",
        "?t_mrid_1",
        "?t_mrid_2",
    ]

    if connectivity is not None:
        select_query += [f"{connectivity_mrid(connectivity)}"]

    where_list = terminal_sequence_query(cim_version=cim_version, var=connectivity)
    if with_market:
        select_query += [f"?bidzone_{nr}" for nr in [1, 2]]
        for terminal_nr in [1, 2]:
            where_list += [market_code_query(terminal_nr)]

    where_list += [
        "?mrid rdf:type cim:ACLineSegment",
        "?mrid cim:ACLineSegment.x ?x",
        "?mrid cim:ACLineSegment.r ?r",
        "?mrid cim:ACLineSegment.bch ?bch",
        "?mrid cim:Conductor.length ?length",
        "?mrid cim:ConductingEquipment.BaseVoltage ?obase",
        "?obase cim:BaseVoltage.nominalVoltage ?un",
        "?mrid cim:IdentifiedObject.name ?name",
    ]

    if network_analysis is not None:
        where_list += [f"?mrid SN:Equipment.networkAnalysisEnable {network_analysis}"]

    if region is not None:
        where_list += [f"?mrid cim:Equipment.EquipmentContainer ?{container}"]
        where_list += region_query(region, sub_region, container)

    if rates:
        where_rate = []
        select_query += [f"?rate{rate}" for rate in rates]

        for rate in rates:
            where_rate += operational_limit("?mrid", rate)
        where_list += [group_query(where_rate, command="OPTIONAL")]

    if temperatures is not None:
        select_query += [
            f"?{negpos(temperature)}_{abs(temperature)}_factor" for temperature in temperatures
        ]
        where_list += [
            group_query(temp_correction_factors("?mrid", cim, temperatures), command="OPTIONAL")
        ]
    return combine_statements(" ".join(select_query), group_query(where_list))


def connection_query(
    cim_version: int,
    rdf_types: Union[str, List[str]],
    region: Union[str, List[str]],
    sub_region: bool = False,
    connectivity: str = con_mrid_str,
) -> str:

    select_query = ["SELECT", "?mrid", "?t_mrid_1", "?t_mrid_2"]

    if connectivity is not None:
        select_query += [f"{connectivity_mrid(connectivity)}"]

    if isinstance(rdf_types, str):
        rdf_types = [rdf_types]

    cim_types = [f"?mrid rdf:type {rdf_type}" for rdf_type in rdf_types]
    where_list = [combine_statements(*cim_types, group=len(cim_types) > 1, split="\n} UNION \n {")]

    if region is not None:
        where_list += [
            "?mrid cim:Equipment.EquipmentContainer ?EquipmentContainer",
            "?EquipmentContainer cim:Bay.VoltageLevel ?VoltageLevel",
            "?VoltageLevel cim:VoltageLevel.Substation ?Substation",
        ] + region_query(region, sub_region, "Substation")

    where_list += terminal_sequence_query(cim_version=cim_version, var=connectivity)

    return combine_statements(" ".join(select_query), group_query(where_list))


def winding_from_three_tx(three_tx: pd.DataFrame, i: int) -> pd.DataFrame:
    columns = [col for col in three_tx.columns if col.endswith(f"_{i}") or col == "mrid"]
    winding = three_tx[columns]
    t_mrid = f"t_mrid_{i}"
    rename_columns = {
        column: "_".join(column.split("_")[:-1])
        for column in columns
        if column not in [t_mrid, "mrid"]
    }
    rename_columns[t_mrid] = "t_mrid_1"
    return winding.rename(columns=rename_columns)


def winding_list(three_tx: pd.DataFrame) -> List[pd.DataFrame]:
    return [winding_from_three_tx(three_tx, i) for i in [1, 2, 3]]


def three_tx_to_windings(three_tx: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    three_tx.reset_index(inplace=True)
    three_tx.rename(columns={"index": "mrid"}, inplace=True)
    windings = pd.concat(winding_list(three_tx), ignore_index=True)
    windings["b"] = np.divide(1.0, windings["x"])
    windings["ckt"] = windings["w_mrid"]
    windings["t_mrid_2"] = windings["mrid"]
    windings["bidzone_1"] = windings["bidzone_2"] = windings["bidzone"]
    return windings.loc[:, cols]


def windings_set_end(windings: pd.DataFrame, i: int, cols: List[str]):
    columns = {f"{var}": f"{var}_{i}" for var in cols}
    return windings[windings["endNumber"] == i][["mrid"] + cols].rename(columns=columns)


def windings_to_tx(
    windings: pd.DataFrame, phase_tap_changers: pd.DataFrame
) -> Tuple[pd.DataFrame, ...]:
    """Split windings two-windings and three-windings

    Will also update provided phase_tap_changers dataframe with columne winding end one of
    transformer if not empty.

    Args:
        winding: All transformerends (windings)
        phase_tap_changers:

    Returns: Both two-winding and three-winding transformers (as three two-winding transformers).
    """
    possible_columns = [
        "name",
        "x",
        "un",
        "t_mrid",
        "w_mrid",
        "bidzone",
        con_mrid_str,
        "rateNormal",
        "rateWarning",
        "rateOverload",
    ]
    cols = [col for col in possible_columns if col in windings.columns]

    # Three winding includes endNumber == 3
    three_winding_mrid = windings[windings["endNumber"] == 3]["mrid"]
    three_windings = windings.loc[windings["mrid"].isin(three_winding_mrid), :]
    wd = [windings_set_end(three_windings, i, cols).set_index("mrid") for i in range(1, 4)]
    three_tx = pd.concat(wd, axis=1, sort=False)

    # While two winding transformers don't. Combine first and second winding.
    two_tx_group = windings[~windings["mrid"].isin(three_winding_mrid)].groupby("endNumber")
    two_tx = two_tx_group.get_group(1).set_index("mrid")
    two_tx_2 = two_tx_group.get_group(2).set_index("w_mrid")
    for col in ["t_mrid", "bidzone"]:
        two_tx.loc[two_tx_2["mrid"], f"{col}_2"] = two_tx_2[col].values
    two_tx.loc[two_tx_2["mrid"], "x"] += two_tx_2["x"].values

    if not phase_tap_changers.empty:
        phase_tap_changers["w_mrid_1"] = two_tx.loc[
            two_tx_2.loc[phase_tap_changers.index, "mrid"], "w_mrid"
        ].values

    two_tx = two_tx.reset_index().rename(
        columns={"w_mrid": "ckt", "t_mrid": "t_mrid_1", "bidzone": "bidzone_1"}
    )
    return two_tx, three_tx


class Islands(nx.Graph):
    def __init__(self, connections: pd.DataFrame) -> None:
        super().__init__()
        self.add_edges_from(connections.to_numpy())
        self._groups = list(nx.connected_components(self))

    def reference_nodes(self, columns: List[str] = None) -> pd.DataFrame:
        if columns is None:
            columns = ["mrid", "ref_node"]
        keys = []
        values = []
        for group in self.groups():
            ref = list(group)[0]
            keys += list(group)
            values += [ref] * len(group)
        return pd.DataFrame(np.array([keys, values]).transpose(), columns=columns).set_index("mrid")

    def groups(self) -> List[Set]:
        return copy.deepcopy(self._groups)
