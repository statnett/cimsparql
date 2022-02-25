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
    PHASE_CHANGER,
    SUBSTATION,
    SYNC_MACH,
    TR_WINDING,
)
from cimsparql.constants import allowed_load_types, mrid_variable, sequence_numbers, union_split
from cimsparql.transformer_windings import terminal, transformer_common


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
        where_list.append(sup.get_name(name_mrid, alias_name, alias=False))
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
    variables = [mrid, "?w_mrid_1", "?w_mrid_2", "?t_mrid_1", "?t_mrid_2"]
    tap = "?tap"
    where_list = [
        sup.rdf_type_tripler(mrid, PHASE_CHANGER),
        f"{mrid} cim:TapChanger.TapChangerControl {tap}",
        f"{mrid} cim:PhaseTapChanger.TransformerEnd ?pt",
    ]

    if with_tap_changer_values:
        variables.extend([mrid, "?phase_incr"] + sup.to_variables(tap_changer_objects))
        properties = {f"{obj}Step": f"?{obj}" for obj in tap_changer_objects}
        where_list.extend(
            [
                *sup.predicate_list(mrid, "cim:TapChanger", properties),
                "?mrid cim:PhaseTapChangerLinear.stepPhaseShiftIncrement ?phase_incr",
            ]
        )

    pt = "?pt"
    where_list.extend(
        [
            sup.rdf_type_tripler(pt, TR_WINDING),
            f"{pt} cim:TransformerEnd.Terminal ?term_1",
            f"{pt} cim:TransformerEnd.endNumber ?endNumber",
        ]
    )

    if impedance is not None:
        variables.extend(sup.to_variables(impedance))
        where_list.extend([f"{pt} {TR_WINDING}.{imp} ?{imp}" for imp in impedance])

    where_list.extend(
        [
            "bind(?pt as ?w_mrid_1)",
            "bind(?pt as ?w_mrid_2)",
            "bind(?term_1 as ?t_mrid_1)",
            "bind(?term_1 as ?t_mrid_2)",
        ]
    )

    if region is not None:
        where_list.extend([f"?pt {EQUIP_CONTAINER} ?Substation"])
        where_list.extend(sup.region_query(region, sub_region, "Substation", "?subgeoreg"))

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
        *sup.bid_market_code_query_sv(),
    ]

    if load_vars is not None:
        variables.extend([f"?{load}" for load in load_vars])
        where_list.append(
            sup.group_query(
                [f"{mrid} cim:EnergyConsumer.pfixed ?{load}" for load in load_vars],
                command="OPTIONAL",
            )
        )

    if station_group:
        variables.append("?station_group")
        station_group_list = [
            f"{mrid} cim:NonConformLoad.LoadGroup ?lg",
            "?lg svk:IdentifiedObject.aristoID ?station_group",
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
        *sup.bid_market_code_query_sv(),
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
        f"{mrid} cim:RotatingMachine.GeneratingUnit ?gu",
        f"{mrid} svk:IdentifiedObject.aristoID ?market_code",
        "?gu cim:GeneratingUnit.maxOperatingP ?maxP",
        "?gu cim:GeneratingUnit.minOperatingP ?minP",
        f"{mrid} cim:IdentifiedObject.name ?station_group",
        "bind('100.0' as ?allocationMax)",
        "bind('1.0' as ?allocationWeight)",
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


def two_winding_transformer_query(
    region: Union[str, List[str]],
    sub_region: bool,
    rates: Iterable[str],
    end_count: int,
    network_analysis: bool,
    with_market: bool,
    mrid: str,
    name: str,
    impedance: Iterable[str],
) -> str:
    variables = ["?t_mrid_1", "?t_mrid_2"]
    where_list = [*terminal(mrid, 1), *terminal(mrid, 2)]
    transformer_common(
        2,
        mrid,
        name,
        impedance,
        variables,
        where_list,
        with_market,
        region,
        sub_region,
        rates,
        end_count,
        network_analysis,
    )
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def three_winding_transformer_query(
    region: Union[str, List[str]],
    sub_region: bool,
    rates: Iterable[str],
    end_count: int,
    network_analysis: bool,
    with_market: bool,
    mrid: str,
    name: str,
    impedance: Iterable[str],
) -> str:
    variables = ["?t_mrid_1", f"({mrid} as ?t_mrid_2)"]
    where_list = [*terminal(mrid, 1, lock_end_number=False)]
    transformer_common(
        3,
        mrid,
        name,
        impedance,
        variables,
        where_list,
        with_market,
        region,
        sub_region,
        rates,
        end_count,
        network_analysis,
    )
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
        where_list.append(sup.market_code_query_sv())

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
    sup.include_market_sv(with_market, variables, where_list)

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
    region: str, sub_region: bool, converter_types: Iterable[str], mrid: str, name: str
) -> str:
    variables = [mrid, "?t_mrid", name]
    converters = [sup.rdf_type_tripler("?volt", converter) for converter in converter_types]
    where_sub_list = [
        sup.rdf_type_tripler(mrid, "cim:PowerTransformer"),
        sup.get_name(mrid, name, alias=False),
        f"?tt_mrid cim:Terminal.ConductingEquipment {mrid}",
        "?tt_mrid cim:Terminal.ConnectivityNode ?con",
        "?t_tvolt cim:Terminal.ConductingEquipment ?volt",
        "?t_tvolt cim:Terminal.ConnectivityNode ?con",
        sup.rdf_type_tripler("?volt", "cim:CsConverter"),
        "?t_mrid cim:Terminal.ConductingEquipment ?volt",
    ]
    sub_statement = sup.combine_statements(
        sup.select_statement(variables), sup.group_query(where_sub_list), group=True
    )
    where_list_to_add = [
        sup.rdf_type_tripler(mrid, "cim:PowerTransformer"),
        sup.get_name(mrid, name, alias=False),
        f"?t_mrid cim:Terminal.ConductingEquipment {mrid}",
        "?t_mrid cim:Terminal.ConnectivityNode ?con",
        "?t_tvolt cim:Terminal.ConductingEquipment ?volt",
        "?t_tvolt cim:Terminal.ConnectivityNode ?con",
        sup.combine_statements(*converters, group=len(converters) > 1, split=union_split),
    ]

    if region is not None:
        where_list_to_add.append(f"{mrid} {EQUIP_CONTAINER} ?Substation")
        where_list_to_add.extend(sup.region_query(region, sub_region, "Substation", "?subgeoreg"))

    where_list_combined = (
        "WHERE {{\n" + ".\n".join(where_list_to_add) + "\n}\n" + "UNION " + sub_statement + "\n}\n"
    )

    return sup.combine_statements(sup.select_statement(variables), where_list_combined)


def converters(
    region: str,
    sub_region: bool,
    converter_types: Iterable[str],
    mrid: str,
    name: str,
    sequence_numbers: Optional[List[int]],
) -> str:
    variables = [mrid, name]
    converters = [sup.rdf_type_tripler(mrid, converter) for converter in converter_types]
    where_list = [
        sup.get_name(mrid, name, alias=True),
        sup.combine_statements(*converters, group=len(converters) > 1, split=union_split),
    ]
    if sequence_numbers is not None:
        for num in sequence_numbers:
            variables += [f"?t_mrid_{num}"]
            where_list += [
                f"?t_{num} cim:Terminal.ConductingEquipment {mrid}",
                f'?t_{num} cim:Terminal.sequenceNumber "{num}"^^xsd:integer',
                f"?t_{num} cim:IdentifiedObject.mRID ?t_mrid_{num}",
            ]
    if region is not None:
        vc = f"{mrid} {EQUIP_CONTAINER} ?cont.\n?cont {SUBSTATION} ?Substation."
        dc = f"{mrid} ALG:DCConverter.DCPole ?pole.\n?pole {EQUIP_CONTAINER} ?Substation."
        where_list.extend(
            [
                sup.combine_statements(vc, dc, group=True, split=union_split),
                *sup.region_query(region, sub_region, "Substation", "?subgeoreg"),
            ]
        )
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
    border_filter = sup.border_filter_sv(region, *areas)
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
            f"{mrid} svk:IdentifiedObject.aristoID ?market_code",
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
    external: bool,
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
        sup.get_name(mrid, name),
        *sup.base_voltage(mrid, "?un"),
        *sup.terminal_sequence_query(cim_version=cim_version, var=connectivity),
        *sup.predicate_list(mrid, ACLINE, acline_properties),
    ]

    if connectivity is not None:
        variables.extend(sup.sequence_variables(connectivity))

    sup.include_market_sv(with_market, variables, where_list)

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
        if external is True:
            for nr in sequence_numbers:
                variables.extend([f"?rate{rate}_t{nr}" for rate in rates])
                variables.extend([f"?lim{nr}_mrid"])
                where_rate: List[str] = reduce(
                    iconcat,
                    [sup.operational_limit_sv_external(rate, end_count=nr) for rate in rates],
                    [],
                )
                where_list.append(sup.group_query(where_rate, command="OPTIONAL"))
        else:
            for nr in sequence_numbers:
                variables.extend([f"?rate{rate}_t{nr}" for rate in rates])
                variables.extend([f"?lim{nr}_mrid"])
                where_rate: List[str] = reduce(
                    iconcat, [sup.operational_limit_sv(rate, end_count=nr) for rate in rates], []
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
