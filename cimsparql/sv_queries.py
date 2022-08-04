from typing import Iterable, Union

from cimsparql.cim import ID_OBJ, TC_EQUIPMENT, TR_END, TR_WINDING, LineTypes
from cimsparql.enums import Power, Voltage
from cimsparql.query_support import (
    acdc_terminal,
    combine_statements,
    common_subject,
    group_query,
    select_statement,
)


def _query_str(var_list: Iterable[Union[Voltage, Power]], rdf_type: str, connection: str) -> str:
    variables = ["?mrid", *[f"?{x}" for x in var_list]]
    where = common_subject(
        "?s",
        [
            f"rdf:type cim:{rdf_type}",
            f"cim:{rdf_type}.{connection} ?mrid",
            *[f"cim:{rdf_type}.{x} ?{x}" for x in var_list],
        ],
    )
    return combine_statements(select_statement(variables), group_query([where]))


def powerflow(power: Iterable[Power] = Power) -> str:
    return _query_str(power, "SvPowerFlow", "Terminal")


def voltage(voltage_vars: Iterable[Voltage] = Voltage) -> str:
    return _query_str(voltage_vars, "SvVoltage", "TopologicalNode")


def branch_flow(cim_version: int, power: Iterable[Power] = Power) -> str:
    mrid = "?_mrid"
    variables = ["?mrid", *[f"(?sv_{p}_1 as ?{p})" for p in power]]
    branch_t = common_subject(
        "?_t_mrid_1",
        [
            "rdf:type cim:Terminal",
            f"{TC_EQUIPMENT} {mrid}",
            f"cim:{acdc_terminal(cim_version)}.sequenceNumber 1",
        ],
    )
    winding_t = f"{mrid} {TR_END}.Terminal ?_t_mrid_1"
    where_list = [
        f"values ?rdf_type {{{' '.join(list(LineTypes) + [TR_WINDING])}}}",
        common_subject(mrid, [f"{ID_OBJ}.mRID ?mrid", "rdf:type ?rdf_type"]),
        f"{{{branch_t}}} union {{{winding_t}}}",
        *[sv_terminal_injection(1, p) for p in power],
    ]
    return combine_statements(select_statement(variables), group_query(where_list))


def tapstep() -> str:
    variables = ["?mrid", "?position"]
    where = common_subject(
        "?t_mrid",
        [
            "rdf:type cim:SvTapStep",
            "cim:SvTapStep.TapChanger ?mrid",
            "cim:SvTapStep.position ?position",
        ],
    )
    return combine_statements(select_statement(variables), group_query([where]))


def sv_terminal_injection(nr: int, power: Power = Power.p) -> str:
    return common_subject(
        f"?sv_t_{nr}",
        [f"cim:SvPowerFlow.Terminal ?_t_mrid_{nr}", f"cim:SvPowerFlow.{power} ?sv_{power}_{nr}"],
    )
