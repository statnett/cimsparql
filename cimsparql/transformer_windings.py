from typing import Iterable, List, Union

import cimsparql.query_support as sup
from cimsparql.cim import (
    BIDDINGAREA,
    DELIVERYPOINT,
    EQUIP_CONTAINER,
    ID_OBJ,
    MARKETCODE,
    TR_END,
    TR_WINDING,
)
from cimsparql.enums import SvPowerFlow
from cimsparql.line_queries import line_loss


def _end_number(nr: int, lock_end_number: bool) -> List[str]:
    """Lock winding mrid to specific end number

    Args:
        nr: Lock to this end number
        lock_end_number: return empty list if false
    """
    return [f"?w_mrid_{nr} {TR_END}.endNumber {nr}"] if lock_end_number else []


def terminal(mrid: str, nr: int, lock_end_number: bool = True) -> List[str]:
    """Where statements for transformer terminals"""
    return [
        *_end_number(nr, lock_end_number),
        sup.common_subject(
            f"?w_mrid_{nr}",
            [f"{TR_END}.Terminal ?_t_mrid_{nr}", f"{TR_WINDING}.PowerTransformer {mrid}"],
        ),
    ]


def number_of_windings(mrid: str, winding_count: int, with_loss: bool = False) -> str:
    """Where statement to lock query to transformers with specific number of windings"""
    variables = [mrid, "(count(distinct ?nr) as ?winding_count)"]
    where_list = [
        f"{mrid} rdf:type cim:PowerTransformer",
        sup.common_subject(
            "?wwmrid", [f"{TR_WINDING}.PowerTransformer {mrid}", f"{TR_END}.endNumber ?nr"]
        ),
    ]
    if with_loss:
        variables.append("(sum(xsd:double(str(?sv_p))) / sum(?nr_connected) as ?pl)")
        sv_flow = sup.common_subject(
            "?_sv_t", [f"{SvPowerFlow.Terminal} ?p_t_mrid", f"{SvPowerFlow.p} ?_sv_p"]
        )
        where_list.extend(
            [
                f"?wwmrid {TR_END}.Terminal ?p_t_mrid",
                "optional{?p_t_mrid cim:ACDCTerminal.connected ?connected}",
                "bind(coalesce(if(?connected, 1, 0), 0) as ?nr_connected)",
                f"optional {{{sv_flow}}}",
                "bind(coalesce(?_sv_p, xsd:double(0.0)) as ?sv_p)",
            ]
        )
    select = sup.combine_statements(sup.select_statement(variables), sup.group_query(where_list))
    return sup.group_query(
        [select, f"group by {mrid}", f"having (?winding_count = {winding_count})"],
        command="",
        split="\n",
    )


def _market(variables: List[str], where_list: List[str], with_market: bool) -> None:
    if with_market:
        variables.extend(["?bidzone_1", "?bidzone_2"])
        where_list.extend(
            [
                f"optional {{?Substation {DELIVERYPOINT}/{BIDDINGAREA}/{MARKETCODE} ?bidzone}}",
                "bind(?bidzone as ?bidzone_1)",
                "bind(?bidzone as ?bidzone_2)",
            ]
        )


def transformer_common(
    winding_count: int,
    p_mrid: bool,
    name: str,
    impedance: Iterable[str],
    variables: List[str],
    where_list: List[str],
    with_market: bool,
    region: Union[str, List[str]],
    sub_region: bool,
    rates: Iterable[str],
    network_analysis: bool,
    with_loss: bool,
) -> None:
    variables.extend([name, "?mrid", "?un"])
    if p_mrid:
        variables.append("?p_mrid")
    if with_loss:
        if winding_count == 2:
            line_loss(variables, where_list)
        elif winding_count == 3:
            variables.append("(xsd:double(0.0) as ?pl_1) (?pl as ?pl_2)")
    where_list.extend(
        [
            sup.get_name("?_p_mrid", name),
            sup.common_subject("?w_mrid_1", [f"{TR_WINDING}.ratedU ?un", f"{ID_OBJ}.mRID ?mrid"]),
            number_of_windings("?_p_mrid", winding_count, with_loss and winding_count == 3),
        ]
    )
    if winding_count == 2:
        for z in impedance:
            variables.append(f"(xsd:double(str(?{z}_1)) + xsd:double(str(?{z}_2)) as ?{z})")
        for nr in [1, 2]:
            where_list.extend(
                sup.predicate_list(
                    f"?w_mrid_{nr}", TR_WINDING, {z: f"?{z}_{nr}" for z in impedance}
                )
            )
    else:
        variables.extend(sup.to_variables(impedance))
        where_list.extend(
            sup.predicate_list("?w_mrid_1", TR_WINDING, {z: f"?{z}" for z in impedance})
        )

    if with_market or region is not None:
        where_list.append(f"?_p_mrid {EQUIP_CONTAINER} ?Substation")
        where_list.extend(sup.region_query(region, sub_region, "Substation"))
        _market(variables, where_list, with_market)

    if network_analysis:
        where_list.append(f"?_p_mrid SN:Equipment.networkAnalysisEnable {network_analysis}")

    if rates:
        where_rate = []
        for rate in rates:
            variables.append(f"?rate{rate}")
            where_rate.append(sup.operational_limit("?_t_mrid_1", rate, limit_set="Terminal"))
        where_list.append(sup.group_query(where_rate, command="optional"))
