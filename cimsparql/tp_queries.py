from cimsparql.queries import acdc_terminal, combine_statements, group_query


def terminal(cim_version: int) -> str:
    select = "SELECT ?mrid ?connected ?tp_node"
    where = [
        "?mrid rdf:type cim:Terminal",
        "?mrid cim:Terminal.TopologicalNode ?tp_node",
        f"?mrid cim:{acdc_terminal(cim_version)}.connected ?connected",
    ]
    return combine_statements(select, group_query(where))


def topological_node() -> str:
    select = "SELECT ?mrid ?name ?ConnectivityNodeContainer ?BaseVoltage"
    where = [
        "?mrid rdf:type cim:TopologicalNode",
        "?mrid cim:IdentifiedObject.name ?name",
        "?mrid cim:TopologicalNode.ConnectivityNodeContainer ?ConnectivityNodeContainer",
        "?mrid cim:TopologicalNode.BaseVoltage ?base_voltage_mrid",
        "?base_voltage_mrid cim:BaseVoltage.nominalVoltage ?BaseVoltage",
    ]
    return combine_statements(select, group_query(where))
