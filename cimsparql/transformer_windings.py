from functools import reduce
from operator import iconcat
from typing import Iterable, List, Union

import cimsparql.query_support as sup
from cimsparql.cim import EQUIP_CONTAINER, ID_OBJ, TR_WINDING


def end_number(nr: int, lock_end_number: bool) -> List[str]:
    """Lock winding mrid to specific end number

    Args:
        nr: Lock to this end number
        lock_end_number: return empty list if false
    """
    return [f"?w_mrid_{nr} cim:TransformerEnd.endNumber '{nr}' "] if lock_end_number else []


def terminal(mrid: str, nr: int, lock_end_number: bool = True) -> List[str]:
    """Where statements for transformer terminals"""
    return [
        *end_number(nr, lock_end_number),
        f"?w_mrid_{nr} cim:TransformerEnd.Terminal ?t_mrid_{nr}",
        f"?w_mrid_{nr} {TR_WINDING}.PowerTransformer {mrid}",
    ]


def number_of_windings(mrid: str, winding_count: int) -> str:
    """Where statement to lock query to transformers with specific number of windings"""
    variables = [mrid, "(count(distinct ?nr) as ?winding_count)"]
    where_list = [
        f"{mrid} rdf:type cim:PowerTransformer",
        f"?wwmrid {TR_WINDING}.PowerTransformer {mrid}",
        "?wwmrid cim:TransformerEnd.endNumber ?nr",
    ]
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
                "?Substation SN:Substation.MarketDeliveryPoint ?mdp",
                sup.group_query(
                    [
                        "?mdp SN:MarketDeliveryPoint.BiddingArea ?b_area",
                        "?b_area SN:BiddingArea.marketCode ?bidzone",
                        "bind(?bidzone as ?bidzone_1)",
                        "bind(?bidzone as ?bidzone_2)",
                    ],
                    command="OPTIONAL",
                ),
            ]
        )


def _market_sv(variables: List[str], where_list: List[str], with_market: bool) -> None:
    if with_market:
        variables.extend(["?bidzone_1", "?bidzone_2"])
        where_list.extend(
            [
                "?Substation cim:Substation.Region ?mdp",
                sup.group_query(
                    [
                        "?mdp cim:IdentifiedObject.name ?bidzone",
                        "bind(?bidzone as ?bidzone_1)",
                        "bind(?bidzone as ?bidzone_2)",
                    ],
                    command="OPTIONAL",
                ),
            ]
        )


def transformer_common(
    winding_count: int,
    mrid: str,
    name: str,
    impedance: List[str],
    variables: List[str],
    where_list: List[str],
    with_market: bool,
    region: Union[str, List[str]],
    sub_region: bool,
    rates: Iterable[str],
    end_count: int,
    network_analysis: bool,
) -> None:
    variables.extend([name, mrid, "?mrid", *sup.to_variables(impedance), "?un"])
    where_list.extend(
        [
            f"{mrid} {ID_OBJ}.name {name}",
            f"?w_mrid_1 {TR_WINDING}.ratedU ?un",
            "bind(?w_mrid_1 as ?mrid)",
            number_of_windings(mrid, winding_count),
        ]
    )
    where_list.extend(sup.predicate_list("?w_mrid_1", TR_WINDING, {z: f"?{z}" for z in impedance}))

    if with_market or region is not None:
        where_list.append(f"{mrid} {EQUIP_CONTAINER} ?Substation")
        where_list.extend(sup.region_query(region, sub_region, "Substation", "?subgeoreg"))
        _market_sv(variables, where_list, with_market)

    if network_analysis is not None:
        where_list.append(f"{mrid} SN:Equipment.networkAnalysisEnable {network_analysis}")

    if rates:
        for nr in list(range(1, end_count + 1)):
            variables.extend([f"?rate{rate}_t{nr}" for rate in rates])
            where_rate: List[str] = reduce(
                iconcat, [sup.operational_limit_sv_transf(rate, end_count=nr) for rate in rates], []
            )
            where_list.append(sup.group_query(where_rate, command="OPTIONAL"))
