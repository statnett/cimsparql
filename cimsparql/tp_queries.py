from cimsparql.queries import acdc_terminal


def terminal(cim_version: int) -> str:
    select = "SELECT ?mrid ?connected ?tp_node"
    where = [
        "?mrid rdf:type cim:Terminal",
        "?mrid cim:Terminal.TopologicalNode ?tp_node",
        f"?mrid cim:{acdc_terminal(cim_version)}.connected ?connected",
    ]
    return select + "\n WHERE {\n" + " .\n".join(where) + "\n}"


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
