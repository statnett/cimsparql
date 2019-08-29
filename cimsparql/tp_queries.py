def terminal() -> str:
    return """
    SELECT ?mrid ?connected ?tp_node
    WHERE {{
    ?mrid rdf:type cim:Terminal .
    ?mrid cim:Terminal.TopologicalNode ?tp_node .
    ?mrid cim:Terminal.connected ?connected
    } UNION {
    ?mrid rdf:type cim:Terminal .
    ?mrid cim:Terminal.TopologicalNode ?tp_node .
    ?mrid cim:ACDCTerminal.connected ?connected
    }}
    """


def topological_node() -> str:
    return """
    SELECT ?mrid ?name ?ConnectivityNodeContainer ?BaseVoltage
    WHERE {
    ?mrid rdf:type cim:TopologicalNode .
    ?mrid cim:IdentifiedObject.name ?name .
    ?mrid cim:TopologicalNode.ConnectivityNodeContainer ?ConnectivityNodeContainer .
    ?mrid cim:TopologicalNode.BaseVoltage ?BaseVoltage
    }
    """
