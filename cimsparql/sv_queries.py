from typing import Iterable

from cimsparql.queries import combine_statements, group_query


def _query_str(var_list: Iterable[str], rdf_type: str, connection: str) -> str:
    select = "SELECT ?mrid " + " ".join([f"?{x}" for x in var_list])
    where = [f"?s rdf:type cim:{rdf_type}", f"?s cim:{rdf_type}.{connection} ?mrid"]
    where += [f"?s cim:{rdf_type}.{x} ?{x}" for x in var_list]
    return combine_statements(select, group_query(where))


def powerflow(power: Iterable[str] = ("p", "q")) -> str:
    return _query_str(power, "SvPowerFlow", "Terminal")


def voltage(voltage_vars: Iterable[str] = ("v", "angle")) -> str:
    return _query_str(voltage_vars, "SvVoltage", "TopologicalNode")


def tapstep() -> str:
    return """
    SELECT ?mrid ?position
    WHERE {
    ?t_mrid rdf:type cim:SvTapStep .
    ?t_mrid cim:SvTapStep.TapChanger ?mrid .
    ?t_mrid cim:SvTapStep.position ?position .
    }
    """
