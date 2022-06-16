from operator import eq, ne
from typing import Callable, Iterable, List, Optional, Union

import cimsparql.query_support as sup
from cimsparql.cim import (
    EQUIP_CONTAINER,
    GEO_REG,
    ID_OBJ,
    SUBSTATION,
    SYNC_MACH,
    TC_EQUIPMENT,
    TC_NODE,
    TR_WINDING,
)
from cimsparql.constants import sequence_numbers, union_split
from cimsparql.enums import (
    ConverterTypes,
    Impedance,
    LoadTypes,
    Power,
    Rates,
    SyncVars,
    TapChangerObjects,
)
from cimsparql.transformer_windings import number_of_windings, terminal, transformer_common
from cimsparql.typehints import Region


def version_date() -> str:
    name: str = "?name"
    variables = ["?mrid", name, "?activationDate"]
    where_list = [
        sup.rdf_type_tripler("?marketDefinitionSet", "SN:MarketDefinitionSet"),
        f"?marketDefinitionSet {ID_OBJ}.mRID ?mrid",
        sup.get_name("?marketDefinitionSet", name),
        "?marketDefinitionSet SN:MarketDefinitionSet.activationDate ?activationDate",
        f"FILTER regex({name}, 'ScheduleResource')",
    ]
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def regions_query() -> str:
    mrid_subject = "?_mrid"
    variables = ["?mrid", "?shortName"]
    region_variable = "?subgeoreg"
    where_list = [
        f"{mrid_subject} {ID_OBJ}.mRID ?mrid",
        sup.rdf_type_tripler(mrid_subject, GEO_REG),
        f"{mrid_subject} SN:IdentifiedObject.shortName ?shortName",
        f"{mrid_subject} {GEO_REG}.Region {region_variable}",
    ]
    names = {mrid_subject: ["?name", "?alias_name"], region_variable: ["?region", "?region_name"]}
    for name_mrid, (name, alias_name) in names.items():
        where_list.append(sup.get_name(name_mrid, name))
        where_list.append(sup.get_name(name_mrid, alias_name, alias=True))
        variables.extend([name, alias_name])
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def phase_tap_changer_query(
    region: Region,
    sub_region: bool,
    with_tap_changer_values: bool,
    impedance: Iterable[Impedance],
    tap_changer_objects: Iterable[TapChangerObjects],
) -> str:
    mrid_subject = "?_mrid"
    variables = ["?mrid", "?w_mrid_1", "?w_mrid_2", "?t_mrid_1", "?t_mrid_2"]
    tap = "?tap"
    where_list = [
        f"{mrid_subject} {ID_OBJ}.mRID ?mrid",
        sup.rdf_type_tripler(mrid_subject, TR_WINDING),
        f"{mrid_subject} cim:TransformerEnd.PhaseTapChanger {tap}",
        f"{mrid_subject} {TR_WINDING}.PowerTransformer ?pt",
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

    if impedance:
        variables.extend(sup.to_variables(impedance))
        where_list.extend([f"?w_mrid_1 {TR_WINDING}.{imp} ?{imp}" for imp in impedance])

    if region:
        where_list.extend([f"?pt {EQUIP_CONTAINER} ?Substation"])
        where_list.extend(sup.region_query(region, sub_region, "Substation"))

    for i in sequence_numbers:
        where_list.extend(
            [
                sup.rdf_type_tripler(f"?term_{i}", "cim:Terminal"),
                f"?w_mrid_{i} {TR_WINDING}.PowerTransformer ?pt",
                f"?w_mrid_{i} cim:TransformerEnd.Terminal ?term_{i}",
                f"?term_{i} cim:Terminal.sequenceNumber {i}",
                f"?term_{i} {ID_OBJ}.mRID ?t_mrid_{i}",
            ]
        )

    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def connectivity_names(mrid_subject: str, name: str = "?name") -> str:
    variables = [mrid_subject, name]
    where_list = [
        sup.rdf_type_tripler(mrid_subject, "cim:ConnectivityNode"),
        sup.get_name(mrid_subject, name),
    ]
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def full_model() -> str:
    variables = ["?model", "?time", "?profile", "?description", "?version", "?created", "?dependon"]
    where_list = [
        sup.rdf_type_tripler("?model", "md:FullModel"),
        "?model md:Model.profile ?profile",
        "?model md:Model.scenarioTime ?time",
        "?model md:Model.description ?description",
        "?model md:Model.version ?version",
        "?model md:Model.created ?created",
        "?model md:Model.DependentOn ?dependon",
        "?dependon rdf:type md:FullModel",
    ]
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def bus_data(region: Region, sub_region: bool, with_market: bool = True) -> str:
    mrid_subject = "?t_mrid"
    bus_name = "?busname"

    variables = [f"({mrid_subject} as ?mrid)", "?name", bus_name, "?un"]
    where_list = [
        sup.rdf_type_tripler(mrid_subject, "cim:TopologicalNode"),
        sup.get_name(mrid_subject, bus_name),
        f"{mrid_subject} cim:TopologicalNode.BaseVoltage/cim:BaseVoltage.nominalVoltage ?un",
        f"{mrid_subject} cim:TopologicalNode.ConnectivityNodeContainer ?cont",
        f"?cont {ID_OBJ}.aliasName ?name",
        f"?cont {SUBSTATION} ?Substation",
    ]
    if with_market:
        variables.append("?bidzone")
        where_list.append(sup.market_code_query(substation="?Substation"))

    if region:
        where_list.extend(sup.region_query(region, sub_region, "Substation"))

    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def three_winding_dummy_bus(region: Region, sub_region: bool) -> str:
    name = "?name"
    variables = ["?mrid", name, f"({name} as ?busname)", "?un"]
    where_list = [
        f"?p_mrid {ID_OBJ}.mRID ?mrid",
        "?w_mrid cim:TransformerEnd.endNumber 1",
        f"?w_mrid {TR_WINDING}.ratedU ?un",
        f"?w_mrid {TR_WINDING}.PowerTransformer ?p_mrid",
        sup.get_name("?p_mrid", name),
        number_of_windings("?p_mrid", 3),
    ]
    if region:
        where_list.append(f"?p_mrid {EQUIP_CONTAINER} ?Substation")
        where_list.extend(sup.region_query(region, sub_region, "Substation"))
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def load_query(
    load_type: Iterable[LoadTypes],
    load_vars: Optional[Iterable[Power]],
    region: Region,
    sub_region: bool,
    connectivity: Optional[str],
    nodes: Optional[str],
    station_group_optional: bool,
    with_sequence_number: bool,
    network_analysis: bool,
    station_group: bool,
    cim_version: int,
    ssh_graph: Optional[str],
) -> str:
    mrid_subject = "?_mrid"

    if not (load_type and set(load_type).issubset(LoadTypes)):
        raise ValueError(f"load_type should be any combination of {set(LoadTypes)}")

    variables = ["?mrid", "?t_mrid" if nodes is None else f"?{nodes}", "?bidzone", "?name"]

    if with_sequence_number:
        variables.append("?sequenceNumber")

    if connectivity:
        variables.append(f"?{connectivity}")

    cim_types = [sup.rdf_type_tripler(mrid_subject, f"cim:{cim_type}") for cim_type in load_type]

    where_list = [
        f"{mrid_subject} {ID_OBJ}.mRID ?mrid",
        f"{mrid_subject} {ID_OBJ}.aliasName ?name",
        f"?_t_mrid {ID_OBJ}.mRID ?t_mrid",
        sup.combine_statements(*cim_types, group=len(cim_types) > 1, split=union_split),
        *sup.terminal_where_query(
            cim_version, connectivity, nodes, mrid_subject, with_sequence_number
        ),
        *sup.bid_market_code_query(mrid_subject),
    ]

    if load_vars:
        variables.extend([f"?{load}" for load in load_vars])
        where_list.append(
            sup.graph(
                ssh_graph,
                sup.group_query(
                    [f"{mrid_subject} cim:EnergyConsumer.{load} ?{load}" for load in load_vars],
                    command="OPTIONAL",
                ),
            )
        )

    if station_group:
        variables.append("?station_group")
        predicate = "/".join(
            [
                "cim:NonConformLoad.LoadGroup",
                "SN:NonConformLoadGroup.ScheduleResource",
                "SN:ScheduleResource.marketCode",
            ]
        )
        station_group_str = f"{mrid_subject} {predicate} ?station_group"
        where_list.append(
            f"optional {{{station_group_str}}}" if station_group_optional else station_group_str
        )

    if network_analysis:
        where_list.append(f"{mrid_subject} SN:Equipment.networkAnalysisEnable {network_analysis}")

    if region:
        where_list.extend(
            [
                f"{mrid_subject} {EQUIP_CONTAINER}/{SUBSTATION} ?Substation",
                *sup.region_query(region, sub_region, "Substation"),
            ]
        )

    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def synchronous_machines_query(
    sync_vars: Iterable[SyncVars],
    region: Region,
    sub_region: bool,
    connectivity: Optional[str],
    nodes: Optional[str],
    station_group_optional: bool,
    cim_version: int,
    with_sequence_number: bool,
    network_analysis: bool,
    u_groups: bool,
    ssh_graph: Optional[str],
) -> str:
    mrid_subject = "?_mrid"
    name = "?name"

    variables = [
        "?mrid",
        name,
        "?t_mrid" if nodes is None else f"?{nodes}",
        "?station_group",
        "?market_code",
        "?maxP",
        "?allocationmax",
        "?allocationWeight",
        "?minP",
        "?bidzone",
        *[f"?{var}" for var in sync_vars],
    ]

    if connectivity:
        variables.append(f"?{connectivity}")

    if with_sequence_number:
        variables.append("?sequenceNumber")

    properties = {"sn": "ratedS", "p": "p", "q": "q"}

    def _power(mrid: str, sync_vars: Iterable[str], op: Callable[[str, str], bool]) -> List[str]:
        return [
            f"{mrid} cim:RotatingMachine.{properties[var]} ?{var}"
            for var in sync_vars
            if op(var, "sn")
        ]

    where_list = [
        f"{mrid_subject} {ID_OBJ}.mRID ?mrid",
        f"?_t_mrid {ID_OBJ}.mRID ?t_mrid",
        sup.get_name(mrid_subject, name),
        sup.rdf_type_tripler(mrid_subject, SYNC_MACH),
        *sup.bid_market_code_query(mrid_subject),
        *sup.terminal_where_query(
            cim_version, connectivity, nodes, mrid_subject, with_sequence_number
        ),
        *[
            f"{mrid_subject} {SYNC_MACH}.{lim} ?{lim}"
            for lim in ["maxQ", "minQ"]
            if lim in variables
        ],
        *_power(mrid_subject, sync_vars, eq),
        sup.graph(
            ssh_graph, sup.group_query(_power(mrid_subject, sync_vars, ne), command="OPTIONAL")
        ),
    ]

    if network_analysis:
        where_list.append(f"{mrid_subject} SN:Equipment.networkAnalysisEnable {network_analysis}")

    station_group = [
        f"{mrid_subject} cim:SynchronousMachine.GeneratingUnit ?gu",
        "?gu SN:GeneratingUnit.marketCode ?market_code",
        "?gu cim:GeneratingUnit.maxOperatingP ?maxP",
        "?gu cim:GeneratingUnit.minOperatingP ?minP",
        "?gu SN:GeneratingUnit.groupAllocationMax ?apctmax",
        "bind(xsd:float(str(?apctmax))*xsd:float(str(?maxP)) / 100.0 as ?allocationmax)",
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

    if region:
        where_list.extend(
            [
                f"{mrid_subject} {EQUIP_CONTAINER}/{SUBSTATION} ?Substation",
                *sup.region_query(region, sub_region, "Substation"),
            ]
        )
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def wind_generating_unit_query(network_analysis: bool) -> str:
    mrid_subject = "?_mrid"
    name = "?name"
    variables = [
        "?mrid",
        "?station_group",
        "?market_code",
        "?maxP",
        "?allocationmax",
        "?allocationWeight",
        "?minP",
        name,
        "?plant_mrid",
    ]
    where_list = [
        f"{mrid_subject} {ID_OBJ}.mRID ?mrid",
        sup.rdf_type_tripler(mrid_subject, "cim:WindGeneratingUnit"),
        f"{mrid_subject} cim:GeneratingUnit.maxOperatingP ?maxP",
        f"{mrid_subject} SN:GeneratingUnit.marketCode ?market_code",
        f"{mrid_subject} cim:GeneratingUnit.minOperatingP ?minP",
        sup.get_name(mrid_subject, name),
        f"{mrid_subject} SN:WindGeneratingUnit.WindPowerPlant ?plant_mrid",
        f"{mrid_subject} SN:GeneratingUnit.groupAllocationMax ?apctmax",
        "bind(xsd:float(str(?apctmax))*xsd:float(str(?maxP)) / 100.0 as ?allocationmax)",
        f"{mrid_subject} SN:GeneratingUnit.groupAllocationWeight ?allocationWeight",
        f"{mrid_subject} SN:GeneratingUnit.ScheduleResource ?sr",
        "?sr SN:ScheduleResource.marketCode ?station_group",
    ]

    if network_analysis:
        where_list.append(f"{mrid_subject} SN:Equipment.networkAnalysisEnable {network_analysis}")

    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def two_winding_transformer_query(
    region: Union[str, List[str]],
    sub_region: bool,
    rates: Iterable[Rates],
    network_analysis: bool,
    with_market: bool,
    p_mrid: bool,
    nodes: Optional[str],
    with_loss: bool,
    name: str,
    impedance: Iterable[Impedance],
    cim_version: int,
) -> str:
    term = nodes if nodes else "t_mrid"
    variables = [f"?{term}_{nr}" for nr in sequence_numbers]
    where_list = [*terminal("?p_mrid", 1), *terminal("?p_mrid", 2)]
    for nr in sequence_numbers:
        if nodes:
            sup.node_list(f"?{nodes}_{nr}", where_list, cim_version, mrid=f"?_t_mrid_{nr}")
        else:
            where_list.append(f"?_t_mrid_{nr} {ID_OBJ}.mRID ?t_mrid_{nr}")

    transformer_common(
        2,
        p_mrid,
        name,
        impedance,
        variables,
        where_list,
        with_market,
        region,
        sub_region,
        rates,
        network_analysis,
        with_loss,
    )
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def three_winding_transformer_query(
    region: Union[str, List[str]],
    sub_region: bool,
    rates: Iterable[Rates],
    network_analysis: bool,
    with_market: bool,
    p_mrid: bool,
    nodes: Optional[str],
    with_loss: bool,
    name: str,
    impedance: Iterable[Impedance],
    cim_version: int,
) -> str:
    term = nodes if nodes else "t_mrid"
    variables = [f"?{term}_1", f"(?p_mrid_object as ?{term}_2)"]
    where_list = [
        *terminal("?p_mrid", 1, lock_end_number=False),
        f"?p_mrid {ID_OBJ}.mRID ?p_mrid_object",
    ]
    if nodes:
        sup.node_list(f"?{nodes}_1", where_list, cim_version, mrid="?_t_mrid_1")
    else:
        where_list.append(f"?_t_mrid_1 {ID_OBJ}.mRID ?t_mrid_1")

    transformer_common(
        3,
        p_mrid,
        name,
        impedance,
        variables,
        where_list,
        with_market,
        region,
        sub_region,
        rates,
        network_analysis,
        with_loss,
    )
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def transformer_query(
    region: Union[str, List[str]],
    sub_region: bool,
    connectivity: str,
    rates: Iterable[Rates],
    network_analysis: bool,
    with_market: bool,
    impedance: Iterable[Impedance],
) -> str:
    mrid_subject = "?_p_mrid"
    name = "?name"

    variables = [name, "?p_mrid", "?w_mrid", "?endNumber", "?un", "?t_mrid"]
    variables.extend(sup.to_variables(impedance))
    where_list = [
        f"{mrid_subject} {ID_OBJ}.mRID ?p_mrid",
        f"?_w_mrid {ID_OBJ}.mRID ?w_mrid",
        f"?_t_mrid {ID_OBJ}.mRID ?t_mrid",
        sup.rdf_type_tripler(mrid_subject, "cim:PowerTransformer"),
        f"?_w_mrid {TR_WINDING}.PowerTransformer {mrid_subject}",
        f"?_w_mrid {TR_WINDING}.ratedU ?un",
        "?_w_mrid cim:TransformerEnd.endNumber ?endNumber",
        "?_w_mrid cim:TransformerEnd.Terminal ?_t_mrid",
        f"?_w_mrid {ID_OBJ}.name {name}",
        *sup.predicate_list("?_w_mrid", TR_WINDING, {z: f"?{z}" for z in impedance}),
    ]
    if with_market:
        variables.append("?bidzone")
        where_list.append(sup.market_code_query())

    if network_analysis:
        where_list.append(f"{mrid_subject} SN:Equipment.networkAnalysisEnable {network_analysis}")

    if connectivity:
        variables.append(f"?{connectivity}")
        where_list.append(f"?_t_mrid {TC_NODE} ?{connectivity}")

    if region:
        where_list.append(f"{mrid_subject} {EQUIP_CONTAINER} ?Substation")
        where_list.extend(sup.region_query(region, sub_region, "Substation"))

    if rates:
        where_rate = []
        for rate in rates:
            variables.append(f"?rate{rate}")
            where_rate.extend(sup.operational_limit(mrid_subject, rate))
        where_list.append(sup.group_query(where_rate, command="OPTIONAL"))
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def transformers_connected_to_converter(
    region: Region, sub_region: bool, converter_types: Iterable[ConverterTypes]
) -> str:
    mrid_subject = "?_mrid"
    name = "?name"
    variables = ["?mrid", "?t_mrid", "?converter_mrid", name]
    converters = [sup.rdf_type_tripler("?volt", converter) for converter in converter_types]
    where_list = [
        f"?volt {ID_OBJ}.mRID ?converter_mrid",
        f"{mrid_subject} {ID_OBJ}.mRID ?mrid",
        f"?_t_mrid {ID_OBJ}.mRID ?t_mrid",
        sup.rdf_type_tripler(mrid_subject, "cim:PowerTransformer"),
        sup.get_name(mrid_subject, name, alias=True),
        f"?_t_mrid {TC_EQUIPMENT} {mrid_subject}",
        f"?_t_mrid {TC_NODE}/^{TC_NODE}/{TC_EQUIPMENT} ?volt",
        sup.combine_statements(*converters, group=len(converters) > 1, split=union_split),
    ]
    if region:
        where_list.append(f"{mrid_subject} {EQUIP_CONTAINER} ?Substation")
        where_list.extend(sup.region_query(region, sub_region, "Substation"))
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def converters(
    region: Region,
    sub_region: bool,
    converter_types: Iterable[ConverterTypes],
    nodes: Optional[str],
    sequence_numbers: Optional[List[int]],
) -> str:
    mrid_subject = "?_mrid"
    name = "?name"

    variables = ["?mrid", name, "?p"]
    converters = [sup.rdf_type_tripler(mrid_subject, converter) for converter in converter_types]
    where_list = [
        f"{mrid_subject} {ID_OBJ}.mRID ?mrid",
        sup.get_name(mrid_subject, name, alias=True),
        sup.combine_statements(*converters, group=len(converters) > 1, split=union_split),
        f"Optional{{{mrid_subject} cim:ACDCConverter.p ?p}}",
    ]

    if sequence_numbers:
        for num in sequence_numbers:
            if nodes:
                node = f"?{nodes}" + (f"_{num}" if len(sequence_numbers) > 1 else "")
                sup.node_list(node, where_list, cim_version=16, mrid=f"?_t_mrid_{num}")
                variables.append(node)
            else:
                variables.append(f"?t_mrid_{num}")
            where_list.extend(
                [
                    f"?_t_mrid_{num} {TC_EQUIPMENT} {mrid_subject}",
                    f"?_t_mrid_{num} cim:Terminal.sequenceNumber {num}",
                    f"?_t_mrid_{num} {ID_OBJ}.mRID ?t_mrid_{num}",
                ]
            )

    if region:
        container = "Substation"
        vc = f"{mrid_subject} {EQUIP_CONTAINER}/{SUBSTATION} ?{container}."
        dc = f"{mrid_subject} ALG:DCConverter.DCPole/{EQUIP_CONTAINER} ?{container}."
        where_list.extend(
            [
                sup.combine_statements(vc, dc, group=True, split=union_split),
                *sup.region_query(region, sub_region, container),
            ]
        )
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def connection_query(
    cim_version: int,
    rdf_types: Union[str, Iterable[str]],
    region: Region,
    sub_region: bool,
    connectivity: Optional[str],
    nodes: Optional[str],
) -> str:
    mrid_subject = "?_mrid"

    variables = ["?mrid", *sup.sequence_variables("t_mrid")]

    if connectivity:
        variables.extend(sup.sequence_variables(connectivity))

    rdf_types = [rdf_types] if isinstance(rdf_types, str) else rdf_types
    cim_types = [sup.rdf_type_tripler(mrid_subject, rdf_type) for rdf_type in rdf_types]

    where_list = [
        f"{mrid_subject} {ID_OBJ}.mRID ?mrid",
        sup.combine_statements(*cim_types, group=len(cim_types) > 1, split=union_split),
        *sup.terminal_sequence_query(cim_version, connectivity, nodes, mrid_subject),
    ]

    if region:
        predicate = f"{EQUIP_CONTAINER}/cim:Bay.VoltageLevel/{SUBSTATION}"
        where_list.append(f"{mrid_subject} {predicate} ?Substation")
        where_list.extend(sup.region_query(region, sub_region, "Substation"))
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))
