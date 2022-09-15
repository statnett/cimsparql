from typing import Iterable, List, Optional, Union

from cimsparql.enums import Power, SvPowerFlow, SvStatus, SvTapStep, SvVoltage, Voltage
from cimsparql.query_support import (
    combine_statements,
    common_subject,
    group_query,
    select_statement,
)


def _query_str(
    var_list: Iterable[Union[Voltage, Power]],
    rdf_type: Union[SvVoltage, SvPowerFlow],
    connection: str,
) -> str:
    variables = ["?mrid", *[f"?{x}" for x in var_list]]
    where = common_subject(
        "?_s",
        [
            f"rdf:type {rdf_type.pred()}",
            f"{rdf_type[connection]} ?mrid",
            *[f"{rdf_type[x]} ?{x}" for x in var_list],
        ],
    )
    return combine_statements(select_statement(variables), group_query([where]))


def powerflow(power: Iterable[Power] = Power) -> str:
    return _query_str(power, SvPowerFlow, "Terminal")


def voltage(voltage_vars: Iterable[Voltage] = Voltage) -> str:
    return _query_str(voltage_vars, SvVoltage, "TopologicalNode")


def tapstep() -> str:
    variables = ["?mrid", "?position"]
    where = common_subject(
        "?t_mrid",
        [
            f"rdf:type {SvTapStep.pred()}",
            f"{SvTapStep.TapChanger} ?mrid",
            f"{SvTapStep.position} ?position",
        ],
    )
    return combine_statements(select_statement(variables), group_query([where]))


def sv_terminal_injection(nr: Optional[int] = None, power: Power = Power.p) -> str:
    nr_str = "" if nr is None else f"_{nr}"
    return common_subject(
        f"?_sv_t{nr_str}",
        [f"{SvPowerFlow.Terminal} ?_t_mrid{nr_str}", f"{SvPowerFlow[power]} ?{power}{nr_str}"],
    )


def sv_status(connected_status: str) -> List[str]:
    return [
        common_subject(
            "?_sv_status",
            [f"{SvStatus.inService} ?in_service", f"{SvStatus.ConductingEquipment} ?_mrid"],
        ),
        f"bind(coalesce(?in_service, {connected_status}) as ?status)",
    ]
