from typing import Dict, Iterable, List, Literal, Optional, Union

from cimsparql.cim import (
    BIDDINGAREA,
    CNODE_CONTAINER,
    DELIVERYPOINT,
    EQUIP_CONTAINER,
    GEO_REG,
    ID_OBJ,
    MARKETCODE,
    OPERATIONAL_LIMIT_SET,
    SUBSTATION,
    TC_EQUIPMENT,
    TC_NODE,
)
from cimsparql.constants import con_mrid_str, sequence_numbers
from cimsparql.typehints import Region


def graph(url: Optional[str], query: str) -> str:
    return f"graph <{url}> {{{query}}}" if url else query


def base_voltage(mrid: str, var: str) -> str:
    return f"{mrid} cim:ConductingEquipment.BaseVoltage/cim:BaseVoltage.nominalVoltage {var}"


def node_list(
    node: str, query_list: List[str], cim_version: int, mrid: str, connected: str
) -> None:
    query_list.append(
        common_subject(
            mrid,
            [
                f"cim:{acdc_terminal(cim_version)}.connected {connected}",
                f"cim:Terminal.TopologicalNode {node}",
            ],
        )
    )


def terminal_sequence_query(
    cim_version: int, con: Optional[str], nodes: Optional[str], mrid_subject: str
) -> List[str]:
    def _term_seq_nr(
        cim_version: int, con: Optional[str], nodes: Optional[str], mrid_subject: str, nr: int
    ) -> str:
        where_list = [
            rdf_type_tripler("", "cim:Terminal"),
            f"{TC_EQUIPMENT} {mrid_subject}",
            f"cim:{acdc_terminal(15)}.sequenceNumber {nr}",
        ]
        if con:
            where_list.append(f"{TC_NODE} ?{con}_{nr}")
        if nodes:
            node_list(f"?_{nodes}_{nr}", where_list, cim_version, "", f"?connected_{nr}")
        else:
            where_list.append(f"{ID_OBJ}.mRID ?t_mrid_{nr}")

        return common_subject(f"?_t_mrid_{nr}", where_list)

    return [_term_seq_nr(cim_version, con, nodes, mrid_subject, nr) for nr in sequence_numbers]


def operational_limit(
    mrid: str,
    rate: str,
    limit_type: Literal["ActivePowerLimit", "CurrentLimit"] = "ActivePowerLimit",
    limit_set: Literal["Terminal", "Equipment"] = "Equipment",
) -> str:
    return common_subject(
        f"?p_lim{rate}",
        [
            f"{OPERATIONAL_LIMIT_SET}/cim:OperationalLimitSet.{limit_set} {mrid}",
            rdf_type_tripler("", f"cim:{limit_type}"),
            f"{ID_OBJ}.name '{rate}@20'",
            f"cim:{limit_type}.value ?rate{rate}",
        ],
    )


def region_name_predicate(sub_region: bool) -> str:
    return "SN:IdentifiedObject.shortName" if sub_region else f"{GEO_REG}.Region/{ID_OBJ}.name"


def region_query(region: Region, sub_region: bool, container: str) -> List[str]:
    if region is None:
        return []
    try:
        regions_str = region if isinstance(region, str) else "|".join(region)
        filter = [f"FILTER regex(?area, '{regions_str}')"]
    except TypeError:
        filter = []
    return [
        f"?{container} cim:{container}.Region/{region_name_predicate(sub_region)} ?area",
        *filter,
    ]


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


def market_code_query(nr: Optional[int] = None, substation: Optional[str] = None) -> str:
    nr_s = "" if nr is None else f"_{nr}"
    bidzone_predicate = f"{DELIVERYPOINT}/{BIDDINGAREA}/{MARKETCODE}"
    if not substation:
        substation = f"?_t_mrid{nr_s}"
        bidzone_predicate = f"{TC_NODE}/{CNODE_CONTAINER}/{SUBSTATION}/{bidzone_predicate}"
    return f"optional {{{substation} {bidzone_predicate} ?bidzone{nr_s}}}"


def terminal_where_query(
    cim_version: int,
    con: Optional[str],
    node: Optional[str],
    mrid_subject: str,
    with_sequence_number: bool = False,
) -> str:

    query_list = [rdf_type_tripler("", "cim:Terminal"), f"{TC_EQUIPMENT} {mrid_subject}"]
    if con:
        query_list.append(f"{TC_NODE} ?{con}")
    if node:
        query_list.extend(
            [
                f"cim:{acdc_terminal(cim_version)}.connected ?connected",
                f"cim:Terminal.TopologicalNode ?_{node}",
            ]
        )
    if with_sequence_number:
        query_list.append(f"cim:{acdc_terminal(cim_version)}.sequenceNumber ?sequenceNumber")
    return common_subject("?_t_mrid", query_list)


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


def bid_market_code_query(mrid_subject: str) -> List[str]:
    substation = f"{mrid_subject} {EQUIP_CONTAINER}/{SUBSTATION} ?_substation"
    bidzone = f"?_substation {DELIVERYPOINT}/{BIDDINGAREA}/{MARKETCODE} ?bidzone"
    return [substation, f"optional {{{bidzone}}}", f"?_substation {ID_OBJ}.mRID ?station"]


def to_variables(vars: Iterable[str]) -> List[str]:
    return [f"?{var}" for var in vars]


def common_subject(subject: str, predicates_and_objects: List[str]) -> str:
    """Combine list of predicates and objects with common subject

    Example:
    >>> common_subject("?s", ["rdf:type ?type", "cim:ACDCTerminal.connected ?connected"])

    extracts the rdf:type predicate and the cim:ACDCTerminal.connected predicate for all subjects
    where both predicates are present.
    """
    return f"{subject} {';'.join(predicates_and_objects)}"


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


def select_statement(variables: Optional[List[str]] = None, distinct: bool = False) -> str:
    """Combine variables in an select statement"""
    vars = "*" if variables is None else " ".join(variables)
    return f"SELECT {'distinct' if distinct else ''} {vars}"


def groupby(vars: List[str], where_list: List[str], by: str) -> str:
    return f"{combine_statements(select_statement(vars), group_query(where_list))} groupby({by})"


def group_query(
    x: List[str], command: str = "WHERE", split: str = " .\n", group: bool = True
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


def unionize(*args: str, group: bool = True) -> str:
    if group:
        args = tuple(f"{{\n{arg}\n}}" for arg in args)
    return "\nUNION\n".join(args)


def get_name(mrid: str, name: str, alias: bool = False) -> str:
    param = "aliasName" if alias else "name"
    return f"{mrid} {ID_OBJ}.{param} {name}"


def terminal_number(
    subject: str, predicat: str, number: Union[str, int], union: bool = True
) -> str:
    if union and (isinstance(number, int) or not number.startswith("?")):
        return unionize(f"{subject} {predicat} {number}", f"{subject} {predicat} '{number}'")
    return f"{subject} {predicat} {number}"


def border_filter(region: Union[str, List[str]], area1: str, area2: str) -> List[str]:
    """Border filter where one area is in and the other is out"""

    def _in_first(var1: str, var2: str, regions: Optional[str]) -> List[str]:
        """Return filter for inclusion of first variable and not second"""
        return [f"FILTER (regex({var1}, '{regions}'))", f"FILTER (!regex({var2}, '{regions}'))"]

    regions = "|".join(region) if isinstance(region, list) else region
    return [
        combine_statements(*_in_first(area1, area2, regions)),
        combine_statements(*_in_first(area2, area1, regions)),
    ]
