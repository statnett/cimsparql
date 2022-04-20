from typing import Dict, Iterable, List, Optional, Union

from cimsparql.cim import (
    CNODE_CONTAINER,
    DELIVERYPOINT,
    EQUIP_CONTAINER,
    GEO_REG,
    ID_OBJ,
    SUBSTATION,
)
from cimsparql.constants import con_mrid_str, sequence_numbers


def base_voltage(mrid: str, var: str) -> List[str]:
    return [
        f"{mrid} cim:ConductingEquipment.BaseVoltage ?obase",
        f"?obase cim:BaseVoltage.nominalVoltage {var}",
    ]


def terminal_sequence_query(
    cim_version: int, con: Optional[str], nodes: Optional[str], t_mrid: str = "?t_mrid"
) -> List[str]:
    query_list = []
    for nr in sequence_numbers:
        t_sequence_mrid = f"{t_mrid}_{nr}"
        query_list.extend(
            [
                rdf_type_tripler(t_sequence_mrid, "cim:Terminal"),
                f"{t_sequence_mrid} cim:Terminal.ConductingEquipment ?mrid",
                f"{t_sequence_mrid} cim:{acdc_terminal(cim_version)}.sequenceNumber {nr}",
            ]
        )
        if con:
            query_list.append(f"{t_sequence_mrid} cim:Terminal.ConnectivityNode ?{con}_{nr}")
        if nodes:
            query_list.extend(
                [
                    f"{t_sequence_mrid} cim:{acdc_terminal(cim_version)}.connected 'true'",
                    f"{t_sequence_mrid} cim:Terminal.TopologicalNode ?{nodes}_{nr}",
                ]
            )
    return query_list


def operational_limit(mrid: str, rate: str, limitset: str = "oplimset") -> List[str]:
    return [
        f"?{limitset} cim:OperationalLimitSet.Equipment {mrid}",
        f"?p_lim{rate} cim:OperationalLimit.OperationalLimitSet ?{limitset}",
        rdf_type_tripler(f"?p_lim{rate}", "cim:ActivePowerLimit"),
        f"?p_lim{rate} {ID_OBJ}.name '{rate}@20'",
        f"?p_lim{rate} cim:ActivePowerLimit.value ?rate{rate}",
    ]


def region_name_query(
    region: str, sub_region: bool, sub_geographical_region: str, region_var: str = "?region"
) -> List[str]:
    if sub_region:
        return [f"{sub_geographical_region} SN:IdentifiedObject.shortName {region}"]
    return [
        f"{sub_geographical_region} {GEO_REG}.Region {region_var}",
        f"{region_var} {ID_OBJ}.name {region}",
    ]


def region_query(
    region: Optional[Union[str, List[str]]],
    sub_region: bool,
    container: str,
    sub_geographical_region: str,
) -> List[str]:
    if region is None:
        return []
    query = [f"?{container} cim:{container}.Region {sub_geographical_region}"]
    if isinstance(region, str):
        query.extend(region_name_query(f"'{region}'", sub_region, sub_geographical_region))
    elif isinstance(region, list):
        query.extend(region_name_query("?r_na", sub_region, sub_geographical_region))
        query.append("FILTER regex(?r_na, '" + "|".join(region) + "')")
    else:
        raise NotImplementedError("region must be either str or List")
    return query


def sequence_variables(var: str = con_mrid_str) -> List[str]:
    return [f"?{var}_{nr}" for nr in sequence_numbers]


def _xsd_type(cim: str, var: str) -> str:
    return f"^^<{cim}{var}>"


def acdc_terminal(cim_version: int) -> str:
    return "ACDCTerminal" if cim_version > 15 else "Terminal"


def predicate_list(subject: str, predicate: str, properties: Dict[str, str]) -> List[str]:
    return [f"{subject} {predicate}.{property} {object}" for property, object in properties.items()]


def rdf_type_tripler(subject: str, predicate: str) -> str:
    return f"{subject} rdf:type {predicate}"


def include_market(with_market: bool, variables: List[str], where_list: List[str]) -> None:
    if with_market:
        variables.extend(sequence_variables("bidzone"))
        where_list.extend([market_code_query(terminal_nr) for terminal_nr in sequence_numbers])


def market_code_query(nr: int = None):
    nr_s = "" if nr is None else f"_{nr}"
    return group_query(
        [
            f"?t_mrid{nr_s} cim:Terminal.ConnectivityNode ?con{nr_s}",
            f"?con{nr_s} {CNODE_CONTAINER} ?container{nr_s}",
            f"?container{nr_s} {SUBSTATION} ?substation{nr_s}",
            f"?substation{nr_s} {DELIVERYPOINT} ?m_d_p{nr_s}",
            f"?m_d_p{nr_s} SN:MarketDeliveryPoint.BiddingArea ?barea{nr_s}",
            f"?barea{nr_s} SN:BiddingArea.marketCode ?bidzone{nr_s}",
        ],
        command="OPTIONAL",
    )


def terminal_where_query(
    cim_version: int,
    con: Optional[str],
    node: Optional[str],
    with_sequence_number: bool = False,
    terminal_mrid: str = "?t_mrid",
) -> List[str]:
    query_list = [
        rdf_type_tripler(terminal_mrid, "cim:Terminal"),
        f"{terminal_mrid} cim:Terminal.ConductingEquipment ?mrid",
    ]
    if con:
        query_list.append(f"{terminal_mrid} cim:Terminal.ConnectivityNode ?{con}")
    if node:
        query_list.extend(
            [
                f"{terminal_mrid} cim:{acdc_terminal(cim_version)}.connected 'true'",
                f"{terminal_mrid} cim:Terminal.TopologicalNode ?{node}",
            ]
        )
    if with_sequence_number:
        query_list.append(
            f"{terminal_mrid} cim:{acdc_terminal(cim_version)}.sequenceNumber ?sequenceNumber"
        )
    return query_list


def _temperature_list(temperature: float, xsd: str, curve: str) -> List[str]:
    signed_temperature = f"{negpos(temperature)}_{abs(temperature)}"
    subject = f"?t{signed_temperature}"
    percent = f"?{signed_temperature}_factor"
    temperature_value = f"'{temperature:0.1f}'{xsd}"
    properties = {"Curve": curve, "temperature": temperature_value, "percent": percent}
    return predicate_list(subject, "ALG:TemperatureCurveData", properties)


def temp_correction_factors(
    mrid: str, cim: str, temperatures: Iterable, temperature_mrid: str = "?temp_mrid"
) -> List[str]:
    where_list = [
        rdf_type_tripler(temperature_mrid, "ALG:TemperatureCurveDependentLimit"),
        f"{temperature_mrid} ALG:LimitDependency.Equipment {mrid}",
        f"{temperature_mrid} ALG:TemperatureCurveDependentLimit.TemperatureCurve ?tcur",
    ]
    xsd = _xsd_type(cim, "Temperature")
    for temperature in temperatures:
        where_list.extend(_temperature_list(temperature, xsd, "?tcur"))
    return where_list


def bid_market_code_query() -> List[str]:
    return [
        f"?mrid {EQUIP_CONTAINER} ?eq_container",
        f"?eq_container {SUBSTATION} ?substation",
        f"?substation {DELIVERYPOINT} ?m_d_p",
        "?m_d_p SN:MarketDeliveryPoint.BiddingArea ?barea",
        "?barea SN:BiddingArea.marketCode ?bidzone",
    ]


def to_variables(vars: Iterable[str]) -> List[str]:
    return [f"?{var}" for var in vars]


def combine_statements(*args, group: bool = False, split: str = "\n") -> str:
    """Join *args

    Args:
       group: return enclosed by {...}
       split: join *args by this

    Example:
       >>> import os
       >>> where_list = ['?mrid rdf:type cim:ACLineSegment', '?mrid cim:ACLineSegment.r ?r']
       >>> combine_statements(where_list,group=True, split='\n')
    """
    return "{\n" + split.join(args) + "\n}" if group else split.join(args)


def negpos(val: Union[float, int]) -> str:
    """Convert 'sign' to text"""
    return "minus" if val < 0 else "plus"


def select_statement(variables: Optional[List[str]] = None) -> str:
    """Combine variables in an select statement"""
    vars = "*" if variables is None else " ".join(variables)
    return f"SELECT {vars}"


def group_query(
    x: List[str], command: str = "WHERE", split: str = ".\n", group: bool = True
) -> str:
    """Group Query

    Args:
       x: List of objects to group
       command: to operate on group
       split, group: (see: combine_statements)

    Example:
       >>> import os
       >>> where_list = ['?mrid rdf:type cim:ACLineSegment', '?mrid cim:ACLineSegment.r ?r']
       >>> group_query(where_list, group=True, split= '.\n')
    """
    return command + " " + combine_statements(*x, group=group, split=split)


def unionize(*args: str, group: bool = True):
    if group:
        args = tuple(f"{{\n{arg}\n}}" for arg in args)
    return "\nUNION\n".join(args)


def get_name(mrid: str, name: str, alias: bool = False) -> str:
    param = "aliasName" if alias else "name"
    return f"{mrid} {ID_OBJ}.{param} {name}"


def border_filter(region: Optional[Union[str, List[str]]], area1: str, area2: str) -> List[str]:
    """Border filter where one area is in and the other is out"""

    def _in_first(var1: str, var2: str, regions: Optional[str]) -> List[str]:
        """Return filter for inclusion of first variable and not second"""
        return [f"FILTER (regex({var1}, '{regions}'))", f"FILTER (!regex({var2}, '{regions}'))"]

    regions = "|".join(region) if isinstance(region, list) else region
    return [
        combine_statements(*_in_first(area1, area2, regions)),
        combine_statements(*_in_first(area2, area1, regions)),
    ]
