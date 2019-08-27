import pandas as pd

from cimsparql.redland import Model, get_table_and_convert


def tp_terminal(model: Model, cimversion: str) -> pd.DataFrame:
    query = """
    SELECT ?mrid ?connected ?tp_node
    WHERE {
    ?mrid rdf:type cim:Terminal .
    ?mrid cim:Terminal.TopologicalNode ?tp_node .
    ?mrid cim:Terminal.connected ?connected
    }
    """
    columns = {"connected": bool}
    return get_table_and_convert(model, cimversion + query, columns).set_index("mrid")


def tp_topological_node(model: Model, cimversion: str) -> pd.DataFrame:
    query = """
    SELECT ?mrid ?name ?ConnectivityNodeContainer ?BaseVoltage
    WHERE {
    ?mrid rdf:type cim:TopologicalNode .
    ?mrid cim:IdentifiedObject.name ?name .
    ?mrid cim:TopologicalNode.ConnectivityNodeContainer ?ConnectivityNodeContainer .
    ?mrid cim:TopologicalNode.BaseVoltage ?BaseVoltage
    }
    """
    return get_table_and_convert(model, cimversion + query).set_index("mrid")
