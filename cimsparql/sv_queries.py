from typing import List, Tuple

from cimsparql.queries import group_query, combine_statements


def _query_str(var_list: List[str], rdf_type: str, connection: str) -> str:
    select = "SELECT ?mrid " + " ".join([f"?{x}" for x in var_list])
    where = [f"?s rdf:type cim:{rdf_type}", f"?s cim:{rdf_type}.{connection} ?mrid"]
    where += [f"?s cim:{rdf_type}.{x} ?{x}" for x in var_list]
    return combine_statements(select, group_query(where))


def powerflow(power: Tuple[str] = ("p", "q")) -> str:
    return _query_str(power, "SvPowerFlow", "Terminal")


def voltage(voltage_vars: Tuple[str] = ("v", "angle")) -> str:
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
