from operator import eq, ne
from typing import Callable, Iterable, List, Optional, Union

import cimsparql.query_support as sup
from cimsparql.cim import (
    CNODE_CONTAINER,
    EQUIP_CONTAINER,
    GEO_REG,
    ID_OBJ,
    SUBSTATION,
    SYNC_MACH,
    T_SEQUENCE,
    TC_EQUIPMENT,
    TC_NODE,
    TN,
    TR_END,
    TR_WINDING,
    LineTypes,
)
from cimsparql.constants import sequence_numbers, union_split
from cimsparql.enums import (
    BaseVoltage,
    ConverterTypes,
    GeneratingUnit,
    Impedance,
    LoadTypes,
    Power,
    Rates,
    SvPowerFlow,
    SvTapStep,
    SynchronousMachine,
    SyncVars,
    TapChangerObjects,
    VoltageLevel,
    WindGeneratingUnit,
)
from cimsparql.query_support import common_subject
from cimsparql.sv_queries import sv_status
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
        f"filter regex({name}, 'ScheduleResource')",
    ]
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def regions_query(with_sn_short_name: bool = True) -> str:
    mrid_subject = "?_mrid"
    variables = ["?mrid"]
    region_variable = "?subgeoreg"
    where_list = [
        common_subject(
            mrid_subject,
            [
                f"{ID_OBJ}.mRID ?mrid",
                sup.rdf_type_tripler("", GEO_REG),
                f"{GEO_REG}.Region {region_variable}",
            ],
        )
    ]

    if with_sn_short_name:
        where_list.append(f"{mrid_subject} SN:IdentifiedObject.shortName ?shortName")
        variables.append("?shortName")

    names = {mrid_subject: ["?name", "?alias_name"], region_variable: ["?region", "?region_name"]}
    for name_mrid, (name, alias_name) in names.items():
        where_list.append(sup.get_name(name_mrid, name))
        alias_name_triple = sup.get_name(name_mrid, alias_name, alias=True)
        where_list.append(f"optional {{{alias_name_triple}}}")
        variables.extend([name, alias_name])
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def ac_line_mrids() -> str:
    variables = ["?mrid"]
    where_list = [
        common_subject("?_mrid", [f"rdf:type {LineTypes.ACLineSegment}", f"{ID_OBJ}.mRID ?mrid"])
    ]
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
                f"{TR_END}.PhaseTapChanger ?_tap",
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
                    f"{ID_OBJ}.mRID {tap}",
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
                f"{TR_END}.Terminal ?term_{i}",
                f"{ID_OBJ}.mRID ?w_mrid_{i}",
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


def node_delta_power(node: str, cim_version: int) -> str:
    """Calculate the sum of node injections (should be zero)."""
    variables = [f"?_{node} (-sum(xsd:float(str(?p))) as ?delta_p)"]
    where_list = [
        common_subject(
            "?_t_mrid",
            [
                "rdf:type cim:Terminal",
                f"cim:Terminal.TopologicalNode ?_{node}",
                f"cim:{sup.acdc_terminal(cim_version)}.connected True",
            ],
        ),
        common_subject("?_obj", [f"{SvPowerFlow.Terminal} ?_t_mrid", f"{SvPowerFlow.p} ?p"]),
    ]
    return sup.groupby(variables, where_list, f"?_{node}")


def bus_data(
    region: Region,
    sub_region: bool,
    with_market: bool,
    container: str,
    cim_version: int,
    delta_power: bool,
) -> str:
    node = "node"
    mrid_subject = f"?_{node}"
    bus_name = "?busname"

    variables = ["?node", "?name", bus_name, "?un", "?station"]
    where_list = [
        common_subject(
            mrid_subject,
            [
                sup.rdf_type_tripler("", TN),
                sup.get_name("", bus_name),
                f"{ID_OBJ}.mRID ?{node}",
                f"{TN}.BaseVoltage/cim:BaseVoltage.nominalVoltage ?un",
                f"{TN}.ConnectivityNodeContainer ?cont",
            ],
        ),
        common_subject(
            "?cont", [f"{ID_OBJ}.aliasName|{ID_OBJ}.name ?name", f"{SUBSTATION} ?Substation"]
        ),
        f"?Substation {ID_OBJ}.mRID ?station",
    ]
    if delta_power:
        variables.append("?delta_p")
        where_list.extend([f"optional {{{node_delta_power(node, cim_version)}}}"])

    if container:
        where_list.append(f"?cont {ID_OBJ}.mRID {container}")
        variables.append(container)

    if with_market:
        variables.append("?bidzone")
        where_list.append(sup.market_code_query(substation="?Substation"))

    if region:
        where_list.extend(sup.region_query(region, sub_region, "Substation"))

    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def three_winding_dummy_bus(
    region: Region,
    sub_region: bool,
    with_market: bool,
    container: str,
    network_analysis: bool,
    delta_power: bool,
) -> str:
    name = "?name"
    variables = ["?node", name, f"({name} as ?busname)", "?un", "?station"]
    where_list = [
        common_subject(
            "?_p_mrid",
            [f"{ID_OBJ}.mRID ?node", sup.get_name("", name), f"{EQUIP_CONTAINER} ?Substation"],
        ),
        common_subject(
            "?w_mrid",
            [
                f"{TR_END}.endNumber 1",
                f"{TR_WINDING}.ratedU ?un",
                f"{TR_WINDING}.PowerTransformer ?_p_mrid",
            ],
        ),
        f"?Substation {ID_OBJ}.mRID ?station",
        number_of_windings("?_p_mrid", 3),
    ]

    if delta_power:
        variables.append("?delta_p")
        where_list.append("bind(0.0 as ?delta_p)")

    if network_analysis:
        where_list.append("?_p_mrid SN:Equipment.networkAnalysisEnable True")

    if container:
        where_list.append(
            f"?w_mrid {TR_END}.Terminal/{TC_NODE}/{CNODE_CONTAINER}/{ID_OBJ}.mRID {container}"
        )
        variables.append(container)

    if with_market:
        variables.append("?bidzone")
        where_list.extend([sup.market_code_query(substation="?Substation")])

    if region:
        where_list.extend(sup.region_query(region, sub_region, "Substation"))
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def load_query(
    load_type: Iterable[LoadTypes],
    load_vars: Optional[Iterable[Power]],
    region: Region,
    sub_region: bool,
    connectivity: Optional[str],
    nodes: Optional[str],
    station_group: bool,
    with_sequence_number: bool,
    network_analysis: bool,
    cim_version: int,
    with_bidzone: bool,
    ssh_graph: Optional[str],
) -> str:
    mrid_subject = "?_mrid"

    if not (load_type and set(load_type).issubset(LoadTypes)):
        raise ValueError(f"load_type should be any combination of {set(LoadTypes)}")

    variables = ["?mrid", "?t_mrid" if nodes is None else f"?{nodes}", "?name", "?station"]

    if with_sequence_number:
        variables.append("?sequenceNumber")

    if connectivity:
        variables.append(f"?{connectivity}")

    cim_types = [sup.rdf_type_tripler(mrid_subject, cim_type) for cim_type in load_type]

    where_list = [
        sup.combine_statements(*cim_types, group=len(cim_types) > 1, split=union_split),
        sup.terminal_where_query(
            cim_version, connectivity, nodes, mrid_subject, with_sequence_number
        ),
        f"?_t_mrid {ID_OBJ}.mRID ?t_mrid",
        common_subject(mrid_subject, [f"{ID_OBJ}.mRID ?mrid", f"{ID_OBJ}.name ?name"]),
    ]

    if with_bidzone:
        variables.append("?bidzone")
        where_list += sup.bid_market_code_query(mrid_subject)

    if nodes:
        variables.append("?status")
        where_list.extend([f"?_{nodes} {ID_OBJ}.mRID ?{nodes}", *sv_status("?connected")])

    if load_vars:
        variables.extend([f"?{load}" for load in load_vars])
        where_list.append(
            sup.graph(
                ssh_graph,
                sup.group_query(
                    [f"{mrid_subject} cim:EnergyConsumer.{load} ?{load}" for load in load_vars],
                    command="optional",
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
        where_list.append(f"optional {{{station_group_str}}}")

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
    station_group: bool,
    cim_version: int,
    with_sequence_number: bool,
    network_analysis: bool,
    u_groups: bool,
    with_market: bool,
    ssh_graph: Optional[str],
) -> str:
    mrid_subject = "?_mrid"
    name = "?name"

    variables = [
        "?mrid",
        name,
        "?t_mrid" if nodes is None else f"?{nodes}",
        "?station_group",
        "?station",
        "?market_code",
        "?maxP",
        "?allocationmax",
        "(?allocationWeight as ?MO)",
        "?minP",
        "?bidzone",
        *[f"?{var}" for var in sync_vars],
    ]

    if connectivity:
        variables.append(f"?{connectivity}")

    if with_sequence_number:
        variables.append("?sequenceNumber")

    properties = {"sn": "ratedS", "p": "p", "q": "q"}

    def _power(mrid: str, sync_vars: Iterable[str], op: Callable[[str, str], bool]) -> str:
        return common_subject(
            mrid,
            [f"cim:RotatingMachine.{properties[var]} ?{var}" for var in sync_vars if op(var, "sn")],
        )

    where_list = [
        sup.get_name(mrid_subject, name),
        sup.rdf_type_tripler(mrid_subject, SYNC_MACH),
        sup.terminal_where_query(
            cim_version, connectivity, nodes, mrid_subject, with_sequence_number
        ),
        _power(mrid_subject, sync_vars, eq),
        sup.graph(
            ssh_graph, sup.group_query([_power(mrid_subject, sync_vars, ne)], command="OPTIONAL")
        ),
        f"{mrid_subject} {ID_OBJ}.mRID ?mrid",
        f"?_t_mrid {ID_OBJ}.mRID ?t_mrid",
    ]

    if with_market:
        where_list += sup.bid_market_code_query(mrid_subject)

    if nodes:
        variables.append("?status")
        where_list.extend([f"?_{nodes} {ID_OBJ}.mRID ?{nodes}", *sv_status("?connected")])

    if network_analysis:
        where_list.append(f"{mrid_subject} SN:Equipment.networkAnalysisEnable {network_analysis}")

    station_group_query = [
        f"{mrid_subject} {SynchronousMachine.GeneratingUnit} ?gu",
        common_subject(
            "?gu",
            [
                f"{GeneratingUnit.marketCode} ?market_code",
                f"{GeneratingUnit.maxOperatingP} ?maxP",
                f"{GeneratingUnit.minOperatingP} ?minP",
                f"{GeneratingUnit.groupAllocationMax} ?apctmax",
                f"{GeneratingUnit.groupAllocationWeight} ?allocationWeight",
                f"{GeneratingUnit.ScheduleResource} ?ScheduleResource",
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

    if station_group:
        where_list.append(sup.group_query(station_group_query, command="OPTIONAL"))

    if not u_groups:
        where_list.append("filter (!bound(?st_gr_n) || (!regex(?st_gr_n, 'U-')))")

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
                sup.rdf_type_tripler("", WindGeneratingUnit.pred()),
                f"{GeneratingUnit.maxOperatingP} ?maxP",
                f"{GeneratingUnit.marketCode} ?market_code",
                f"{GeneratingUnit.minOperatingP} ?minP",
                sup.get_name("", name),
                f"{WindGeneratingUnit.WindPowerPlant} ?plant_mrid",
                f"{GeneratingUnit.groupAllocationMax} ?apctmax",
                f"{GeneratingUnit.groupAllocationWeight} ?allocationWeight",
                f"{GeneratingUnit.ScheduleResource}/SN:ScheduleResource.marketCode ?station_group",
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
    variables = [*[f"?{term}_{nr}" for nr in sequence_numbers], "?status", "?angle"]
    where_list = [
        *terminal("?_p_mrid", 1),
        *terminal("?_p_mrid", 2),
        "bind((?connected_1 && ?connected_2) as ?status)",
    ]
    for nr in sequence_numbers:
        where_list.append(f"?_{nodes}_{nr} {ID_OBJ}.mRID ?{nodes}_{nr}")
        mrid = f"?_t_mrid_{nr}"
        connected = f"?connected_{nr}"
        if nodes:
            sup.node_list(f"?_{nodes}_{nr}", where_list, cim_version, mrid, connected)
        else:
            where_list.append(f"{mrid} cim:{sup.acdc_terminal(cim_version)}.connected {connected}")

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
    where_list.extend(
        [
            "?w_mrid_2 cim:PowerTransformerEnd.phaseAngleClock ?angleclock",
            f"optional {{{tap_phase_angle('?w_mrid_2', '?_angle')}}}",
            "bind(coalesce (?_angle+xsd:float(30.0)*?angleclock, xsd:float(0.0)) as ?angle)",
        ]
    )

    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def tap_phase_angle(mrid: str = "?mrid", angle: str = "?angle") -> str:
    variables = [mrid, angle]
    where_list = [
        common_subject(
            "?tap_step",
            [
                f"rdf:type {SvTapStep.pred()}",
                f"{SvTapStep.TapChanger} ?tap_changer",
                f"{SvTapStep.position} ?position",
            ],
        ),
        common_subject(
            "?tap_changer",
            [
                "cim:PhaseTapChangerLinear.stepPhaseShiftIncrement ?inc",
                "cim:TapChanger.normalStep ?normalstep",
                f"cim:PhaseTapChanger.TransformerEnd {mrid}",
            ],
        ),
        f"bind(((?position-?normalstep)*xsd:float(str(?inc))) as {angle})",
    ]
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

    connected = "?connected"
    variables = [f"?{term}_1", f"(?p_mrid as ?{term}_2)", "(xsd:boolean(?connected) as ?status)"]
    where_list = [
        *terminal("?_p_mrid", 1, lock_end_number=False),
        f"?_p_mrid {ID_OBJ}.mRID ?p_mrid",
        f"?_{nodes}_1 {ID_OBJ}.mRID ?{nodes}_1",
    ]
    mrid = "?_t_mrid_1"

    if nodes:
        sup.node_list(f"?_{nodes}_1", where_list, cim_version, mrid, connected)
    else:
        where_list.append(f"{mrid} cim:{sup.acdc_terminal(cim_version)}.connected {connected}")

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
            f"{TR_END}.endNumber ?endNumber",
            f"{TR_END}.Terminal ?_t_mrid",
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
    region: Region,
    sub_region: bool,
    converter_types: Iterable[ConverterTypes],
    on_primary_side: bool,
) -> str:
    mrid_subject = "?_mrid"
    name = "?name"
    variables = ["?mrid", "?t_mrid", "?converter_mrid", name]
    converters = [sup.rdf_type_tripler("?volt", converter) for converter in converter_types]
    where_list = [
        f"?volt {ID_OBJ}.mRID ?converter_mrid",
        f"{mrid_subject} {ID_OBJ}.mRID ?mrid",
        sup.rdf_type_tripler(mrid_subject, "cim:PowerTransformer"),
        sup.get_name(mrid_subject, name, alias=True),
        common_subject(
            "?_t_mrid",
            [f"{TC_EQUIPMENT} {mrid_subject}", f"{TC_NODE}/^{TC_NODE}/{TC_EQUIPMENT} ?volt"],
        ),
        sup.combine_statements(*converters, group=len(converters) > 1, split=union_split),
    ]
    if on_primary_side:
        where_list.extend(
            [
                common_subject(
                    "?_winding_1",
                    [
                        f"{TR_WINDING}.PowerTransformer ?_mrid",
                        f"{TR_END}.endNumber 1",
                        f"{TR_END}.Terminal ?_t_mrid_1",
                    ],
                ),
                f"?_t_mrid_1 {ID_OBJ}.mRID ?t_mrid",
            ]
        )
    else:
        where_list.append(f"?_t_mrid {ID_OBJ}.mRID ?t_mrid")
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
    cim_version: int,
) -> str:
    mrid_subject = "?_mrid"
    name = "?name"

    variables = ["?mrid", name, "?p", "?station"]
    converters = [sup.rdf_type_tripler(mrid_subject, converter) for converter in converter_types]
    where_list = [
        common_subject(
            mrid_subject,
            [
                f"{ID_OBJ}.mRID ?mrid",
                sup.get_name("", name, alias=True),
                f"{EQUIP_CONTAINER}/{SUBSTATION}/{ID_OBJ}.mRID ?station",
            ],
        ),
        sup.combine_statements(*converters, group=len(converters) > 1, split=union_split),
        f"Optional{{{mrid_subject} cim:ACDCConverter.p ?p}}",
    ]

    if sequence_numbers:
        variables.append("(xsd:boolean(?connected) as ?status)")
        for num in sequence_numbers:
            t_mrid = f"?t_mrid_{num}"
            t_mrid_ref = f"?_t_mrid_{num}"
            sequence_where = [f"{TC_EQUIPMENT} {mrid_subject}", f"{T_SEQUENCE} {num}"]
            if nodes:
                node = nodes + (f"_{num}" if len(sequence_numbers) > 1 else "")
                sup.node_list(f"?_{node}", where_list, cim_version, t_mrid_ref, "?connected")
                where_list.append(f"?_{node} {ID_OBJ}.mRID ?{node}")
                variables.append(f"?{node}")
            else:
                variables.append(t_mrid)
                sequence_where.extend([f"{ID_OBJ}.mRID {t_mrid}", ""])

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


def substation_voltage_level() -> str:
    def _substation(volt: str) -> str:
        return common_subject(
            "?_container",
            [
                f"rdf:type {VoltageLevel.pred()}",
                f"{VoltageLevel.BaseVoltage}/{BaseVoltage.nominalVoltage} {volt}",
                f"{VoltageLevel.Substation} ?_substation",
            ],
        )

    sub_group = sup.groupby(
        ["(max(?volt) as ?v)", "?_substation"], [_substation("?volt")], "?_substation"
    )
    variables = ["?container", "?substation"]
    where_list = [
        _substation("?v"),
        f"{{{sub_group}}}",
        *[f"?_{key[1:]} {ID_OBJ}.mRID {key}" for key in variables],
    ]
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))
