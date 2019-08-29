def powerflow() -> str:
    return """
    SELECT ?mrid ?p ?q
    WHERE {
    ?p_mrid rdf:type cim:SvPowerFlow .
    ?p_mrid cim:SvPowerFlow.Terminal ?mrid .
    ?p_mrid cim:SvPowerFlow.p ?p .
    ?p_mrid cim:SvPowerFlow.q ?q .
    }
    """


def voltage() -> str:
    return """
    SELECT ?mrid ?v ?angle
    WHERE {
    ?s_mrid rdf:type cim:SvVoltage .
    ?s_mrid cim:SvVoltage.TopologicalNode ?mrid .
    ?s_mrid cim:SvVoltage.v ?v .
    ?s_mrid cim:SvVoltage.angle ?angle
    }
    """


def tapstep() -> str:
    return """
    SELECT ?mrid ?position
    WHERE {
    ?t_mrid rdf:type cim:SvTapStep .
    ?t_mrid cim:SvTapStep.TapChanger ?mrid .
    ?t_mrid cim:SvTapStep.position ?position .
    }
    """
