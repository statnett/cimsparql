from functools import reduce
from operator import iconcat
from typing import Iterable, List, Optional, Union

import cimsparql.query_support as sup
from cimsparql.cim import (
    ACLINE,
    CNODE_CONTAINER,
    EQUIP_CONTAINER,
    GEO_REG,
    ID_OBJ,
    SUBSTATION,
    SYNC_MACH,
    TR_WINDING,
)
from cimsparql.constants import allowed_load_types, mrid_variable, sequence_numbers, union_split


def version_date() -> str:
    name: str = "?name"
    variables = [mrid_variable, name, "?activationDate"]
    where_list = [
        sup.rdf_type_tripler("?marketDefinitionSet", "SN:MarketDefinitionSet"),
        f"?marketDefinitionSet {ID_OBJ}.mRID {mrid_variable}",
        sup.get_name("?marketDefinitionSet", name),
        "?marketDefinitionSet SN:MarketDefinitionSet.activationDate ?activationDate",
        f"FILTER regex({name}, 'ScheduleResource')",
    ]
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def regions_query(mrid: str) -> str:
    variables = [mrid, "?shortName"]
    region_variable = "?subgeoreg"
    where_list = [
        sup.rdf_type_tripler(mrid, GEO_REG),
        f"{mrid} SN:IdentifiedObject.shortName ?shortName",
        f"{mrid} {GEO_REG}.Region {region_variable}",
    ]
    names = {mrid: ["?name", "?alias_name"], region_variable: ["?region", "?region_name"]}
    for name_mrid, (name, alias_name) in names.items():
        where_list.append(sup.get_name(name_mrid, name))
        where_list.append(sup.get_name(name_mrid, alias_name, alias=True))
        variables.extend([name, alias_name])
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def phase_tap_changer_query(
    region: Union[str, List[str]],
    sub_region: bool,
    with_tap_changer_values: bool,
    impedance: Iterable[str],
    tap_changer_objects: Iterable[str],
    mrid: str,
) -> str:
    variables = [mrid]
    tap = "?tap"
    where_list = [
        sup.rdf_type_tripler(mrid, TR_WINDING),
        f"{mrid} cim:TransformerEnd.PhaseTapChanger {tap}",
        f"{mrid} {TR_WINDING}.PowerTransformer ?pt",
    ]

    if with_tap_changer_values:
        variables.extend([tap, "?phase_incr"] + sup.to_variables(tap_changer_objects))
        properties = {f"{obj}Step": f"?{obj}" for obj in tap_changer_objects}
        where_list.extend(
            [
                *sup.predicate_list(tap, "cim:TapChanger", properties),
                "?tap cim:PhaseTapChangerLinear.stepPhaseShiftIncrement ?phase_incr",
            ]
        )

    if impedance is not None:
        variables.extend(sup.to_variables(impedance))
        where_list.extend([f"?pte_1 {TR_WINDING}.{imp} ?{imp}" for imp in impedance])

    if region is not None:
        where_list.extend([f"?pt {EQUIP_CONTAINER} ?Substation"])
        where_list.extend(sup.region_query(region, sub_region, "Substation", "?subgeoreg"))

    for i in sequence_numbers:
        variables.append(f"?t_mrid_{i}")
        where_list.extend(
            [
                sup.rdf_type_tripler(f"?term_{i}", "cim:Terminal"),
                f"?pte_{i} {TR_WINDING}.PowerTransformer ?pt",
                f"?pte_{i} cim:TransformerEnd.Terminal ?term_{i}",
                f'?term_{i} cim:Terminal.sequenceNumber "{i}"^^xsd:integer',
                f"?term_{i} {ID_OBJ}.mRID ?t_mrid_{i}",
            ]
        )

    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def connectivity_names(mrid: str, name: str = "?name") -> str:
    variables = [mrid, name]
    where_list = [
        sup.rdf_type_tripler(mrid, "cim:ConnectivityNode"),
        sup.get_name(mrid, name),
    ]
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def bus_data(region: Union[str, List[str]], sub_region: bool, mrid: str, name: str) -> str:
    variables = [mrid, name]
    where_list = [sup.rdf_type_tripler(mrid, "cim:TopologicalNode"), sup.get_name(mrid, name)]

    if region is not None:
        where_list.extend(
            [
                f"{mrid} cim:TopologicalNode.ConnectivityNodeContainer ?cont",
                f"?cont {SUBSTATION} ?Substation",
                *sup.region_query(region, sub_region, "Substation", "?subgeoreg"),
            ]
        )
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def load_query(
    load_type: List[str],
    load_vars: Iterable[str],
    region: Union[str, List[str]],
    sub_region: bool,
    connectivity: str,
    station_group_optional: bool,
    with_sequence_number: bool,
    network_analysis: bool,
    station_group: bool,
    cim_version: int,
    mrid: str,
) -> str:

    if not set(load_type).issubset(allowed_load_types) or not load_type:
        raise ValueError(f"load_type should be any combination of {allowed_load_types}")

    variables = [mrid, "?t_mrid", "?bidzone"]

    if with_sequence_number:
        variables.append("?sequenceNumber")

    if connectivity is not None:
        variables.append(f"?{connectivity}")

    cim_types = [sup.rdf_type_tripler(mrid, f"cim:{cim_type}") for cim_type in load_type]

    where_list = [
        sup.combine_statements(*cim_types, group=len(cim_types) > 1, split=union_split),
        *sup.terminal_where_query(cim_version, connectivity, with_sequence_number),
        *sup.bid_market_code_query(),
    ]

    if load_vars is not None:
        variables.extend([f"?{load}" for load in load_vars])
        where_list.append(
            sup.group_query(
                [f"{mrid} cim:EnergyConsumer.{load} ?{load}" for load in load_vars],
                command="OPTIONAL",
            )
        )

    if station_group:
        variables.append("?station_group")
        station_group_list = [
            f"{mrid} cim:NonConformLoad.LoadGroup ?lg",
            "?lg SN:NonConformLoadGroup.ScheduleResource ?sc_res",
            "?sc_res SN:ScheduleResource.marketCode ?station_group",
        ]
        if station_group_optional:
            where_list.append(sup.group_query(station_group_list, command="OPTIONAL"))
        else:
            where_list.extend(station_group_list)

    if network_analysis is not None:
        where_list.append(f"{mrid} SN:Equipment.networkAnalysisEnable {network_analysis}")

    if region is not None:
        where_list.extend(
            [
                f"{mrid} {EQUIP_CONTAINER} ?cont",
                f"?cont {SUBSTATION} ?Substation",
                *sup.region_query(region, sub_region, "Substation", "?subgeoreg"),
            ]
        )

    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def synchronous_machines_query(
    sync_vars: Iterable[str],
    region: Union[str, List[str]],
    sub_region: bool,
    connectivity: str,
    station_group_optional: bool,
    cim_version: int,
    with_sequence_number: bool,
    network_analysis: bool,
    u_groups: bool,
    terminal_mrid: str,
    mrid: str,
    name: str,
) -> str:

    variables = [
        mrid,
        name,
        terminal_mrid,
        "?station_group",
        "?market_code",
        "?maxP",
        "?allocationMax",
        "?allocationWeight",
        "?minP",
        "?bidzone",
        *[f"?{var}" for var in sync_vars],
    ]

    if connectivity is not None:
        variables.append(f"?{connectivity}")

    if with_sequence_number:
        variables.append("?sequenceNumber")

    properties = {"sn": "ratedS", "p": "p", "q": "q"}
    where_list = [
        sup.rdf_type_tripler(mrid, SYNC_MACH),
        sup.get_name(mrid, name),
        *sup.bid_market_code_query(),
        *sup.terminal_where_query(cim_version, connectivity, with_sequence_number, terminal_mrid),
        *[f"{mrid} {SYNC_MACH}.{lim} ?{lim}" for lim in ["maxQ", "minQ"] if lim in variables],
        *[
            sup.group_query(
                [f"{mrid} cim:RotatingMachine.{properties[var]} ?{var}"], command="OPTIONAL"
            )
            for var in sync_vars
        ],
    ]

    if network_analysis is not None:
        where_list.append(f"{mrid} SN:Equipment.networkAnalysisEnable {network_analysis}")

    station_group = [
        f"{mrid} cim:SynchronousMachine.GeneratingUnit ?gu",
        "?gu SN:GeneratingUnit.marketCode ?market_code",
        "?gu cim:GeneratingUnit.maxOperatingP ?maxP",
        "?gu cim:GeneratingUnit.minOperatingP ?minP",
        "?gu SN:GeneratingUnit.groupAllocationMax ?allocationMax",
        "?gu SN:GeneratingUnit.groupAllocationWeight ?allocationWeight",
        "?gu SN:GeneratingUnit.ScheduleResource ?ScheduleResource",
        "?ScheduleResource SN:ScheduleResource.marketCode ?station_group",
        sup.get_name("?ScheduleResource", "?st_gr_n", alias=True),
    ]

    if station_group_optional:
        where_list.append(sup.group_query(station_group, command="OPTIONAL"))
    else:
        where_list.extend(station_group)

    if not u_groups:
        where_list.append("FILTER (!bound(?st_gr_n) || (!regex(?st_gr_n, 'U-')))")

    if region is not None:
        where_list.extend(
            [
                f"{mrid} {EQUIP_CONTAINER} ?cont",
                f"?cont {SUBSTATION} ?Substation",
                *sup.region_query(region, sub_region, "Substation", "?subgeoreg"),
            ]
        )
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def wind_generating_unit_query(network_analysis: bool, mrid: str, name: str):
    variables = [
        mrid,
        "?station_group",
        "?market_code",
        "?maxP",
        "?allocationMax",
        "?allocationWeight",
        "?minP",
        name,
        "?plant_mrid",
    ]
    where_list = [
        sup.rdf_type_tripler(mrid, "cim:WindGeneratingUnit"),
        f"{mrid} cim:GeneratingUnit.maxOperatingP ?maxP",
        f"{mrid} SN:GeneratingUnit.marketCode ?market_code",
        f"{mrid} cim:GeneratingUnit.minOperatingP ?minP",
        sup.get_name(mrid, name),
        f"{mrid} SN:WindGeneratingUnit.WindPowerPlant ?plant_mrid",
        f"{mrid} SN:GeneratingUnit.groupAllocationMax ?allocationMax",
        f"{mrid} SN:GeneratingUnit.groupAllocationWeight ?allocationWeight",
        f"{mrid} SN:GeneratingUnit.ScheduleResource ?sr",
        "?sr SN:ScheduleResource.marketCode ?station_group",
    ]

    if network_analysis is not None:
        where_list.append(f"{mrid} SN:Equipment.networkAnalysisEnable {network_analysis}")

    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def transformer_query(
    region: Union[str, List[str]],
    sub_region: bool,
    connectivity: str,
    rates: Iterable[str],
    network_analysis: bool,
    with_market: bool,
    mrid: str,
    name: str,
    impedance: Iterable[str],
) -> str:
    variables = [name, mrid, "?w_mrid", "?endNumber", "?un", "?t_mrid"]
    variables.extend(sup.to_variables(impedance))
    where_list = [
        sup.rdf_type_tripler(mrid, "cim:PowerTransformer"),
        f"?w_mrid {TR_WINDING}.PowerTransformer {mrid}",
        f"?w_mrid {TR_WINDING}.ratedU ?un",
        "?w_mrid cim:TransformerEnd.endNumber ?endNumber",
        "?w_mrid cim:TransformerEnd.Terminal ?t_mrid",
        f"?w_mrid {ID_OBJ}.name {name}",
        *sup.predicate_list("?w_mrid", TR_WINDING, {z: f"?{z}" for z in impedance}),
    ]
    if with_market:
        variables.append("?bidzone")
        where_list.append(sup.market_code_query())

    if network_analysis is not None:
        where_list.append(f"{mrid} SN:Equipment.networkAnalysisEnable {network_analysis}")

    if connectivity is not None:
        variables.append(f"?{connectivity}")
        where_list.append(f"?t_mrid cim:Terminal.ConnectivityNode ?{connectivity}")

    if region is not None:
        where_list.append(f"{mrid} {EQUIP_CONTAINER} ?Substation")
        where_list.extend(sup.region_query(region, sub_region, "Substation", "?subgeoreg"))

    if rates:
        where_rate = ["?oplimitset cim:OperationalLimitSet.Terminal ?t_mrid"]

        for rate in rates:
            variables.append(f"?rate{rate}")
            where_rate.extend(sup.operational_limit(mrid, rate, "oplimitset"))
        where_list.append(sup.group_query(where_rate, command="OPTIONAL"))
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def series_compensator_query(
    cim_version: int,
    region: Union[str, List[str]],
    sub_region: bool,
    connectivity: str,
    network_analysis: Optional[bool],
    with_market: bool,
    mrid: str,
    name: str,
) -> str:
    variables = [mrid, "?x", "?un", name] + sup.sequence_variables("t_mrid")
    if connectivity is not None:
        variables += sup.sequence_variables(connectivity)

    where_list = [
        *sup.terminal_sequence_query(cim_version=cim_version, var=connectivity),
        sup.rdf_type_tripler(mrid, "cim:SeriesCompensator"),
        f"{mrid} cim:SeriesCompensator.x ?x",
        f"{mrid} cim:ConductingEquipment.BaseVoltage ?obase",
        "?obase cim:BaseVoltage.nominalVoltage ?un",
        sup.get_name(mrid, name),
    ]
    sup.include_market(with_market, variables, where_list)

    if network_analysis is not None:
        where_list.append(f"{mrid} SN:Equipment.networkAnalysisEnable {network_analysis}")

    if region is not None:
        where_list += [
            f"{mrid} {EQUIP_CONTAINER} ?EquipmentContainer",
            f"?EquipmentContainer {SUBSTATION} ?Substation",
            *sup.region_query(region, sub_region, "Substation", "?subgeoreg"),
        ]

    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def transformers_connected_to_converter(
    region: str, sub_region: bool, converter_types: List[str], mrid: str, name: str
) -> str:
    variables = [mrid, "?t_mrid", name]
    converters = [
        sup.rdf_type_tripler("?volt", f"ALG:{converter}Converter") for converter in converter_types
    ]
    where_list = [
        sup.rdf_type_tripler(mrid, "cim:PowerTransformer"),
        sup.get_name(mrid, name, alias=True),
        f"?t_mrid cim:Terminal.ConductingEquipment {mrid}",
        "?t_mrid cim:Terminal.ConnectivityNode ?con",
        "?t_tvolt cim:Terminal.ConductingEquipment ?volt",
        "?t_tvolt cim:Terminal.ConnectivityNode ?con",
        sup.combine_statements(*converters, group=len(converters) > 1, split=union_split),
    ]
    if region is not None:
        where_list.append(f"{mrid} {EQUIP_CONTAINER} ?Substation")
        where_list.extend(sup.region_query(region, sub_region, "Substation", "?subgeoreg"))
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def borders_query(
    cim_version: int,
    region: Union[str, List[str]],
    sub_region: bool,
    ignore_hvdc: bool,
    with_market_code: bool,
    market_optional: bool,
    mrid: str,
    name: str,
) -> str:
    areas = sup.sequence_variables("area")
    variables = [name, mrid, *sup.sequence_variables("t_mrid"), *areas]
    border_filter = sup.border_filter(region, *areas)
    where_list = [
        sup.get_name(mrid, name),
        sup.rdf_type_tripler(mrid, ACLINE),
        *sup.terminal_sequence_query(cim_version=cim_version, var="con"),
        sup.combine_statements(*border_filter, group=True, split=union_split),
    ]
    for nr in sequence_numbers:
        where_list.extend(
            [
                f"?con_{nr} {CNODE_CONTAINER} ?cont_{nr}",
                f"?cont_{nr} {SUBSTATION} ?subs_{nr}",
                f"?subs_{nr} cim:Substation.Region ?reg_{nr}",
                *sup.region_name_query(f"?area_{nr}", sub_region, f"?reg_{nr}", f"?sreg_{nr}"),
            ]
        )

    if with_market_code:
        variables.append("?market_code")
        where_market = [
            f"{mrid} {EQUIP_CONTAINER} ?line_cont",
            "?line_cont SN:Line.marketCode ?market_code",
        ]
        where_list.append(
            sup.group_query(where_market, command="OPTIONAL" if market_optional else "")
        )

    if ignore_hvdc:
        where_list.append(sup.combine_statements(f"FILTER (!regex({name}, 'HVDC'))", group=True))

    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def ac_line_query(
    cim_version: int,
    cim: str,
    region: Union[str, List[str]],
    sub_region: bool,
    connectivity: str,
    rates: Iterable[str],
    network_analysis: bool,
    with_market: bool,
    temperatures: Optional[List[int]],
    impedance: Iterable[str],
    mrid: str,
    name: str,
) -> str:
    variables = [
        name,
        mrid,
        "?length",
        "?un",
        *sup.sequence_variables("t_mrid"),
        *sup.to_variables(impedance),
    ]

    acline_properties = {z: f"?{z}" for z in impedance}

    where_list = [
        sup.rdf_type_tripler(mrid, ACLINE),
        f"{mrid} cim:Conductor.length ?length",
        sup.get_name(mrid, name),
        *sup.base_voltage(mrid, "?un"),
        *sup.terminal_sequence_query(cim_version=cim_version, var=connectivity),
        *sup.predicate_list(mrid, ACLINE, acline_properties),
    ]

    if connectivity is not None:
        variables.extend(sup.sequence_variables(connectivity))

    sup.include_market(with_market, variables, where_list)

    if network_analysis is not None:
        where_list.append(f"{mrid} SN:Equipment.networkAnalysisEnable {network_analysis}")

    if region is not None:
        where_list.extend(
            [
                f"{mrid} {EQUIP_CONTAINER} ?Line",
                *sup.region_query(region, sub_region, "Line", "?subgeoreg"),
            ]
        )

    if rates:
        variables.extend([f"?rate{rate}" for rate in rates])
        where_rate: List[str] = reduce(
            iconcat, [sup.operational_limit(mrid, rate) for rate in rates], []
        )
        where_list.append(sup.group_query(where_rate, command="OPTIONAL"))

    if temperatures is not None:
        variables.extend(
            [
                f"?{sup.negpos(temperature)}_{abs(temperature)}_factor"
                for temperature in temperatures
            ]
        )
        where_list.append(
            sup.group_query(
                sup.temp_correction_factors(mrid, cim, temperatures), command="OPTIONAL"
            )
        )
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def connection_query(
    cim_version: int,
    rdf_types: Union[str, List[str]],
    region: Union[str, List[str]],
    sub_region: bool,
    connectivity: str,
    mrid: str,
) -> str:
    variables = [mrid, *sup.sequence_variables("t_mrid")]

    if connectivity is not None:
        variables.extend(sup.sequence_variables(connectivity))

    rdf_types = [rdf_types] if isinstance(rdf_types, str) else rdf_types
    cim_types = [sup.rdf_type_tripler(mrid, rdf_type) for rdf_type in rdf_types]

    where_list = [
        sup.combine_statements(*cim_types, group=len(cim_types) > 1, split=union_split),
        *sup.terminal_sequence_query(cim_version=cim_version, var=connectivity),
    ]

    if region is not None:
        where_list.extend(
            [
                f"{mrid} {EQUIP_CONTAINER} ?EquipmentContainer",
                "?EquipmentContainer cim:Bay.VoltageLevel ?VoltageLevel",
                f"?VoltageLevel {SUBSTATION} ?Substation",
            ]
        )
        where_list.extend(sup.region_query(region, sub_region, "Substation", "?subgeoreg"))
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))
