from functools import reduce
from operator import iconcat
from typing import Iterable, List, Literal, Optional, Tuple, Union

import cimsparql.query_support as sup
from cimsparql.cim import ACLINE, CNODE_CONTAINER, EQUIP_CONTAINER, ID_OBJ, SUBSTATION
from cimsparql.constants import sequence_numbers, union_split
from cimsparql.enums import Impedance, Rates
from cimsparql.typehints import Region


def _sv_terminal_injection(nr: int) -> str:
    return "\n".join(
        [
            f"?sv_t_{nr} cim:SvPowerFlow.Terminal ?_t_mrid_{nr}.",
            f"?sv_t_{nr} cim:SvPowerFlow.p ?sv_p_{nr}",
        ]
    )


def _line_query(
    cim_version: int,
    line_type: str,
    connectivity: Optional[str],
    nodes: Optional[str],
    with_loss: bool,
    rates: Iterable[Literal["Normal", "Warning", "Overload"]],
    network_analysis: bool,
    with_market: bool,
    impedance: Iterable[str],
) -> Tuple[List[str], List[str], str]:
    mrid_subject = "?_mrid"
    name = "?name"

    variables = [name, "?mrid", *sup.to_variables(impedance), "?un"]
    variables.extend(sup.sequence_variables(nodes if nodes else "t_mrid"))

    if connectivity:
        variables.extend(sup.sequence_variables(connectivity))

    impedance_properties = {z: f"?{z}" for z in impedance}
    where_list = [
        f"{mrid_subject} {ID_OBJ}.mRID ?mrid",
        sup.rdf_type_tripler(mrid_subject, line_type),
        sup.get_name(mrid_subject, name),
        sup.base_voltage(mrid_subject, "?un"),
        *sup.terminal_sequence_query(cim_version, connectivity, nodes, mrid_subject),
        *sup.predicate_list(mrid_subject, line_type, impedance_properties),
    ]

    sup.include_market(with_market, variables, where_list)

    if with_loss:
        variables.append("(?pl as ?pl_1) (?pl as ?pl_2)")
        loss_list = [_sv_terminal_injection(nr) for nr in [1, 2]]
        loss_list.append("bind((xsd:float(?sv_p_1) + xsd:float(?sv_p_2) ) / 2 as ?pl)")
        where_list.append(sup.group_query(loss_list, command="OPTIONAL"))

    if network_analysis:
        where_list.append(f"{mrid_subject} SN:Equipment.networkAnalysisEnable {network_analysis}")

    if rates:
        limit_type = "ActivePowerLimit" if line_type == ACLINE else "CurrentLimit"
        variables.extend([f"?rate{rate}" for rate in rates])
        where_rate: List[str] = reduce(
            iconcat, [sup.operational_limit(mrid_subject, rate, limit_type) for rate in rates], []
        )
        where_list.append(sup.group_query(where_rate, command="OPTIONAL"))
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
        ACLINE,
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
        where_list.extend(
            [
                f"{mrid_subject} {EQUIP_CONTAINER} ?Line",
                *sup.region_query(region, sub_region, "Line"),
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
        "cim:SeriesCompensator",
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
    sub_region: bool,
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
        f"{mrid_subject} {ID_OBJ}.mRID ?mrid",
        sup.get_name(mrid_subject, name),
        sup.rdf_type_tripler(mrid_subject, ACLINE),
        *sup.terminal_sequence_query(cim_version, "con", nodes, mrid_subject),
        sup.combine_statements(*border_filter, group=True, split=union_split),
    ]
    for nr in sequence_numbers:
        where_list.extend(
            [
                f"?con_{nr} {CNODE_CONTAINER}/{SUBSTATION}/cim:Substation.Region ?reg_{nr}",
                sup.region_name_query(f"?area_{nr}", sub_region, f"?reg_{nr}"),
            ]
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
