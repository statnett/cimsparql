from operator import eq, ne
from typing import Callable, Iterable, List, Optional, Union

import cimsparql.query_support as sup
from cimsparql.cim import (
    EQUIP_CONTAINER,
    GEO_REG,
    ID_OBJ,
    SUBSTATION,
    SYNC_MACH,
    T_SEQUENCE,
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
from cimsparql.query_support import common_subject
from cimsparql.transformer_windings import number_of_windings, terminal, transformer_common
from cimsparql.typehints import Region


def version_date() -> str:
    name: str = "?name"
    variables = ["?mrid", name, "?activationDate"]
    where_list = [
        common_subject(
            "?marketDefinitionSet",
            [
                sup.rdf_type_tripler("", "SN:MarketDefinitionSet"),
                f"{ID_OBJ}.mRID ?mrid",
                sup.get_name("", name),
                "SN:MarketDefinitionSet.activationDate ?activationDate",
            ],
        ),
        f"FILTER regex({name}, 'ScheduleResource')",
    ]
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def regions_query() -> str:
    mrid_subject = "?_mrid"
    variables = ["?mrid", "?shortName"]
    region_variable = "?subgeoreg"
    where_list = [
        common_subject(
            mrid_subject,
            [
                f"{ID_OBJ}.mRID ?mrid",
                sup.rdf_type_tripler("", GEO_REG),
                "SN:IdentifiedObject.shortName ?shortName",
                f"{GEO_REG}.Region {region_variable}",
            ],
        )
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
        common_subject(
            mrid_subject,
            [
                f"{ID_OBJ}.mRID ?mrid",
                sup.rdf_type_tripler("", TR_WINDING),
                "cim:TransformerEnd.PhaseTapChanger ?_tap",
                f"{TR_WINDING}.PowerTransformer ?pt",
            ],
        )
    ]

    if with_tap_changer_values:
        variables.extend([tap, "?phase_incr"] + sup.to_variables(tap_changer_objects))
        properties = {f"{obj}Step": f"?{obj}" for obj in tap_changer_objects}
        where_list.append(
            common_subject(
                "?_tap",
                [
                    *sup.predicate_list("", "cim:TapChanger", properties),
                    "cim:PhaseTapChangerLinear.stepPhaseShiftIncrement ?phase_incr",
                    f"cim:IdentifiedObject.mRID {tap}",
                ],
            )
        )

    if impedance:
        variables.extend(sup.to_variables(impedance))
        where_list.append(
            common_subject("?_w_mrid_1", [f"{TR_WINDING}.{imp} ?{imp}" for imp in impedance])
        )

    if region:
        where_list.extend([f"?pt {EQUIP_CONTAINER} ?Substation"])
        where_list.extend(sup.region_query(region, sub_region, "Substation"))

    for i in sequence_numbers:
        w_mrid = common_subject(
            f"?_w_mrid_{i}",
            [
                f"{TR_WINDING}.PowerTransformer ?pt",
                f"cim:TransformerEnd.Terminal ?term_{i}",
                f"cim:IdentifiedObject.mRID ?w_mrid_{i}",
            ],
        )
        t_mrid = common_subject(
            f"?term_{i}",
            [
                sup.rdf_type_tripler("", "cim:Terminal"),
                f"cim:Terminal.sequenceNumber {i}",
                f"{ID_OBJ}.mRID ?t_mrid_{i}",
            ],
        )
        where_list.extend([w_mrid, t_mrid])

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
        common_subject(
            "?model",
            [
                sup.rdf_type_tripler("", "md:FullModel"),
                "md:Model.profile ?profile",
                "md:Model.scenarioTime ?time",
                "md:Model.description ?description",
                "md:Model.version ?version",
                "md:Model.created ?created",
                "md:Model.DependentOn ?dependon",
            ],
        ),
        "?dependon rdf:type md:FullModel",
    ]
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def bus_data(region: Region, sub_region: bool, with_market: bool = True) -> str:
    mrid_subject = "?t_mrid"
    bus_name = "?busname"

    variables = [f"({mrid_subject} as ?mrid)", "?name", bus_name, "?un"]
    where_list = [
        common_subject(
            mrid_subject,
            [
                sup.rdf_type_tripler("", "cim:TopologicalNode"),
                sup.get_name("", bus_name),
                "cim:TopologicalNode.BaseVoltage/cim:BaseVoltage.nominalVoltage ?un",
                "cim:TopologicalNode.ConnectivityNodeContainer ?cont",
            ],
        ),
        common_subject("?cont", [f"{ID_OBJ}.aliasName ?name", f"{SUBSTATION} ?Substation"]),
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
        common_subject("?p_mrid", [f"{ID_OBJ}.mRID ?mrid", sup.get_name("", name)]),
        common_subject(
            "?w_mrid",
            [
                "cim:TransformerEnd.endNumber 1",
                f"{TR_WINDING}.ratedU ?un",
                f"{TR_WINDING}.PowerTransformer ?p_mrid",
            ],
        ),
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
        sup.terminal_where_query(
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
        sup.terminal_where_query(
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
        common_subject(
            "?gu",
            [
                "SN:GeneratingUnit.marketCode ?market_code",
                "cim:GeneratingUnit.maxOperatingP ?maxP",
                "cim:GeneratingUnit.minOperatingP ?minP",
                "SN:GeneratingUnit.groupAllocationMax ?apctmax",
                "SN:GeneratingUnit.groupAllocationWeight ?allocationWeight",
                "SN:GeneratingUnit.ScheduleResource ?ScheduleResource",
            ],
        ),
        common_subject(
            "?ScheduleResource",
            [
                "SN:ScheduleResource.marketCode ?station_group",
                sup.get_name("", "?st_gr_n", alias=True),
            ],
        ),
        "bind(xsd:float(str(?apctmax))*xsd:float(str(?maxP)) / 100.0 as ?allocationmax)",
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
        common_subject(
            mrid_subject,
            [
                f"{ID_OBJ}.mRID ?mrid",
                sup.rdf_type_tripler("", "cim:WindGeneratingUnit"),
                "cim:GeneratingUnit.maxOperatingP ?maxP",
                "SN:GeneratingUnit.marketCode ?market_code",
                "cim:GeneratingUnit.minOperatingP ?minP",
                sup.get_name("", name),
                "SN:WindGeneratingUnit.WindPowerPlant ?plant_mrid",
                "SN:GeneratingUnit.groupAllocationMax ?apctmax",
                "SN:GeneratingUnit.groupAllocationWeight ?allocationWeight",
                "SN:GeneratingUnit.ScheduleResource/SN:ScheduleResource.marketCode ?station_group",
            ],
        ),
        "bind(xsd:float(str(?apctmax))*xsd:float(str(?maxP)) / 100.0 as ?allocationmax)",
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
    w_mrid = common_subject(
        "?_w_mrid",
        [
            f"{ID_OBJ}.mRID ?w_mrid",
            f"{ID_OBJ}.name {name}",
            f"{TR_WINDING}.PowerTransformer {mrid_subject}",
            f"{TR_WINDING}.ratedU ?un",
            "cim:TransformerEnd.endNumber ?endNumber",
            "cim:TransformerEnd.Terminal ?_t_mrid",
            *sup.predicate_list("", TR_WINDING, {z: f"?{z}" for z in impedance}),
        ],
    )
    where_list = [
        f"{mrid_subject} {ID_OBJ}.mRID ?p_mrid",
        sup.rdf_type_tripler(mrid_subject, "cim:PowerTransformer"),
        f"?_t_mrid {ID_OBJ}.mRID ?t_mrid",
        w_mrid,
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
            where_rate.append(sup.operational_limit(mrid_subject, rate))
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
            t_mrid = f"?t_mrid_{num}"
            t_mrid_ref = f"?_t_mrid_{num}"
            sequence_where = [f"{TC_EQUIPMENT} {mrid_subject}", f"{T_SEQUENCE} {num}"]
            if nodes:
                node = f"?{nodes}" + (f"_{num}" if len(sequence_numbers) > 1 else "")
                sup.node_list(node, where_list, cim_version=16, mrid=t_mrid_ref)
                variables.append(node)
            else:
                variables.append(t_mrid)
                sequence_where.append(f"{ID_OBJ}.mRID {t_mrid}")
            where_list.append(common_subject(t_mrid_ref, sequence_where))

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
