import pandas as pd

from cimsparql.redland import Model, get_table_and_convert


def sv_powerflow(model: Model, cimversion: str) -> pd.DataFrame:
    query = """
    SELECT ?mrid ?p ?q
    WHERE {
    ?p_mrid rdf:type cim:SvPowerFlow .
    ?p_mrid cim:SvPowerFlow.Terminal ?mrid .
    ?p_mrid cim:SvPowerFlow.p ?p .
    ?p_mrid cim:SvPowerFlow.q ?q .
    }
    """
    columns = {"p": float, "q": float}
    return get_table_and_convert(model, cimversion + query, columns).set_index("mrid")


def sv_voltage(model: Model, cimversion: str) -> pd.DataFrame:
    query = """
    SELECT ?mrid ?v ?angle
    WHERE {
    ?s_mrid rdf:type cim:SvVoltage .
    ?s_mrid cim:SvVoltage.TopologicalNode ?mrid .
    ?s_mrid cim:SvVoltage.v ?v .
    ?s_mrid cim:SvVoltage.angle ?angle
    }
    """
    columns = {"v": float, "angle": float}
    return get_table_and_convert(model, cimversion + query, columns).set_index("mrid")


def sv_tapstep(model: Model, cimversion: str) -> pd.DataFrame:
    query = """
    SELECT ?mrid ?position
    WHERE {
    ?t_mrid rdf:type cim:SvTapStep .
    ?t_mrid cim:SvTapStep.TapChanger ?mrid .
    ?t_mrid cim:SvTapStep.position ?position .
    }
    """
    columns = {"position": int}
    return get_table_and_convert(model, cimversion + query, columns).set_index("mrid")
