from typing import Iterable, List, Optional, Tuple, Union

import cimsparql.query_support as sup
from cimsparql.cim import CNODE_CONTAINER, EQUIP_CONTAINER, GEO_REG, ID_OBJ, SUBSTATION, LineTypes
from cimsparql.constants import sequence_numbers, union_split
from cimsparql.enums import IdentifiedObject, Impedance, Rates
from cimsparql.sv_queries import sv_status, sv_terminal_injection
from cimsparql.typehints import Region


def line_loss(variables: List[str], where_list: List[str]) -> None:
    variables.extend(["?pl_1", "?pl_2"])
    terminal_injection = [sv_terminal_injection(nr) for nr in [1, 2]]
    loss_list = [
        "bind((xsd:double(str(?p_1)) + xsd:double(str(?p_2))) as ?pl)",
        "bind(if(xsd:double(str(?p_1))>xsd:double(str(?p_2)),?pl,xsd:double(0.0)) as ?pl_2)",
        "bind(if(xsd:double(str(?p_1))<xsd:double(str(?p_2)),?pl,xsd:double(0.0)) as ?pl_1)",
    ]
    terminal_injection.extend(loss_list)
    where_list.append(sup.group_query(terminal_injection, command="optional"))


def _line_query(
    cim_version: int,
    line_type: LineTypes,
    connectivity: Optional[str],
    nodes: Optional[str],
    with_loss: bool,
    rates: Iterable[Rates],
    network_analysis: bool,
    with_market: bool,
    impedance: Iterable[Impedance],
) -> Tuple[List[str], List[str], str]:
    mrid_subject = "?_mrid"
    name = "?name"

    variables = [name, "?mrid", *sup.to_variables(impedance), "?un"]
    variables.extend(sup.sequence_variables(nodes if nodes else "t_mrid"))

    if connectivity:
        variables.extend(sup.sequence_variables(connectivity))

    where_list = [
        sup.common_subject(
            mrid_subject,
            [
                f"{ID_OBJ}.mRID ?mrid",
                sup.rdf_type_tripler("", line_type),
                sup.get_name("", name),
                sup.base_voltage("", "?un"),
                *sup.predicate_list("", line_type, {z: f"?{z}" for z in impedance}),
            ],
        ),
        *sup.terminal_sequence_query(cim_version, connectivity, nodes, mrid_subject),
    ]

    sup.include_market(with_market, variables, where_list)

    if nodes:
        variables.append("?status")
        where_list.extend(sv_status("?connected_1 && ?connected_2"))
        where_list.extend(
            [f"?_{nodes}_{nr} {ID_OBJ}.mRID ?{nodes}_{nr}" for nr in sequence_numbers]
        )

    if with_loss:
        line_loss(variables, where_list)

    if network_analysis:
        where_list.append(f"{mrid_subject} SN:Equipment.networkAnalysisEnable {network_analysis}")

    if rates:
        limit_type = "ActivePowerLimit" if line_type == LineTypes.ACLineSegment else "CurrentLimit"
        variables.extend([f"?rate{rate}" for rate in rates])
        where_rate = [sup.operational_limit(mrid_subject, rate, limit_type) for rate in rates]
        where_list.append(sup.group_query(where_rate, command="optional"))
    return variables, where_list, mrid_subject


def ac_line_query(
    cim_version: int,
    cim: str,
    region: Region,
    sub_region: bool,
    connectivity: Optional[str],
    nodes: Optional[str],
    with_loss: bool,
    rates: Iterable[Rates],
    network_analysis: bool,
    with_market: bool,
    temperatures: Optional[List[int]],
    impedance: Iterable[Impedance],
    length: bool,
) -> str:
    variables, where_list, mrid_subject = _line_query(
        cim_version,
        LineTypes.ACLineSegment,
        connectivity,
        nodes,
        with_loss,
        rates,
        network_analysis,
        with_market,
        impedance,
    )
    if length:
        variables.append("?length")
        where_list.append(f"{mrid_subject} cim:Conductor.length ?length")

    if region:
        region_str = region if isinstance(region, str) else "|".join(region)
        area_p = "/".join(
            [
                "cim:Terminal.ConnectivityNode",
                "cim:ConnectivityNode.ConnectivityNodeContainer",
                "cim:VoltageLevel.Substation",
                "cim:Substation.Region",
                "cim:SubGeographicalRegion.Region",
                IdentifiedObject.name,
            ]
        )
        where_list.extend(
            [
                f"?_t_mrid_{nr} {area_p} ?_area_{nr} . filter(regex(?_area_{nr}, '{region_str}'))"
                for nr in [1, 2]
            ]
        )

    if temperatures:
        variables.extend(
            [
                f"?{sup.negpos(temperature)}_{abs(temperature)}_factor"
                for temperature in temperatures
            ]
        )
        where_list.append(
            sup.group_query(
                sup.temp_correction_factors(mrid_subject, cim, temperatures), command="OPTIONAL"
            )
        )
    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def series_compensator_query(
    cim_version: int,
    region: Region,
    sub_region: bool,
    connectivity: Optional[str],
    with_loss: bool,
    nodes: Optional[str],
    rates: Iterable[Rates],
    network_analysis: bool,
    with_market: bool,
    impedance: Iterable[Impedance],
) -> str:
    variables, where_list, mrid_subject = _line_query(
        cim_version,
        LineTypes.SeriesCompensator,
        connectivity,
        nodes,
        with_loss,
        rates,
        network_analysis,
        with_market,
        impedance,
    )

    if region:
        where_list += [
            f"{mrid_subject} {EQUIP_CONTAINER}/{SUBSTATION} ?Substation",
            *sup.region_query(region, sub_region, "Substation"),
        ]

    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def borders_query(
    cim_version: int,
    region: Union[str, List[str]],
    nodes: Optional[str],
    ignore_hvdc: bool,
    with_market_code: bool,
    market_optional: bool,
) -> str:
    mrid_subject = "?_mrid"
    name = "?name"

    areas = sup.sequence_variables("area")
    variables = [name, "?mrid", *areas]
    variables.extend(sup.sequence_variables(nodes if nodes else "t_mrid"))

    border_filter = sup.border_filter(region, *areas)
    where_list = [
        sup.common_subject(
            mrid_subject,
            [
                f"{ID_OBJ}.mRID ?mrid",
                sup.get_name("", name),
                sup.rdf_type_tripler("", LineTypes.ACLineSegment),
            ],
        ),
        *sup.terminal_sequence_query(cim_version, "con", nodes, mrid_subject),
        sup.combine_statements(*border_filter, group=True, split=union_split),
    ]
    predicate = (
        f"{CNODE_CONTAINER}/{SUBSTATION}/cim:Substation.Region/{GEO_REG}.Region/{ID_OBJ}.name"
    )
    where_list.extend([f"?con_{nr} {predicate} ?area_{nr}" for nr in sequence_numbers])

    if nodes:
        variables.append("?status")
        where_list.append("bind((?connected_1 && ?connected_2) as ?status)")
        where_list.extend(
            [f"?_{nodes}_{nr} {ID_OBJ}.mRID ?{nodes}_{nr}" for nr in sequence_numbers]
        )

    if with_market_code:
        variables.append("?market_code")
        where_market = [
            f"{mrid_subject} {EQUIP_CONTAINER}/SN:Line.marketCode ?market_code",
        ]
        where_list.append(
            sup.group_query(where_market, command="OPTIONAL" if market_optional else "")
        )

    if ignore_hvdc:
        where_list.append(sup.combine_statements(f"FILTER (!regex({name}, 'HVDC'))", group=True))

    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))


def exchange_query(
    cim_version: int,
    region: Union[str, List[str]],
    nodes: str,
    ignore_hvdc: bool,
    with_market_code: bool,
    market_optional: bool,
) -> str:
    mrid_subject = "?_mrid"
    name = "?name"

    areas = sup.sequence_variables("area")
    variables = [name, f"?{nodes}", "?mrid"]

    border_filter = sup.border_filter(region, *areas)
    where_list = [
        sup.common_subject(
            mrid_subject,
            [
                sup.get_name("", name),
                sup.rdf_type_tripler("", LineTypes.ACLineSegment),
                f"{ID_OBJ}.mRID ?mrid",
            ],
        ),
        *sup.terminal_sequence_query(cim_version, "con", nodes, mrid_subject),
        sup.combine_statements(*border_filter, group=True, split=union_split),
    ]
    predicate = (
        f"{CNODE_CONTAINER}/{SUBSTATION}/cim:Substation.Region/{GEO_REG}.Region/{ID_OBJ}.name"
    )
    where_list.extend([f"?con_{nr} {predicate} ?area_{nr}" for nr in sequence_numbers])

    variables.append("?status")
    where_list.extend(
        ["bind((?connected_1 && ?connected_2) as ?status)", f"?_{nodes}_1 {ID_OBJ}.mRID ?{nodes}"]
    )
    variables.append("(?p_1 as ?p)")
    where_list.append(sv_terminal_injection(1))

    if with_market_code:
        variables.append("?market_code")
        where_market = f"{mrid_subject} {EQUIP_CONTAINER}/SN:Line.marketCode ?market_code"
        where_list.append(
            sup.group_query([where_market], command="OPTIONAL") if market_optional else where_market
        )

    if ignore_hvdc:
        where_list.append(sup.combine_statements(f"FILTER (!regex({name}, 'HVDC'))", group=True))

    return sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))
