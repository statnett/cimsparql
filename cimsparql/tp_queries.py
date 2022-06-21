from cimsparql.cim import ID_OBJ, TN
from cimsparql.query_support import acdc_terminal, combine_statements, common_subject, group_query


def terminal(cim_version: int) -> str:
    select = "SELECT ?mrid ?connected ?tp_node"
    where = [
        common_subject(
            "?mrid",
            [
                "rdf:type cim:Terminal",
                "cim:Terminal.TopologicalNode ?tp_node",
                f"cim:{acdc_terminal(cim_version)}.connected ?connected",
            ],
        )
    ]
    return combine_statements(select, group_query(where))


def topological_node() -> str:
    select = "SELECT ?mrid ?name ?ConnectivityNodeContainer ?BaseVoltage"
    where = [
        common_subject(
            "?mrid",
            [
                f"rdf:type {TN}",
                f"{ID_OBJ}.name ?name",
                f"{TN}.ConnectivityNodeContainer ?ConnectivityNodeContainer",
                f"{TN}.BaseVoltage ?base_voltage_mrid",
            ],
        ),
        "?base_voltage_mrid cim:BaseVoltage.nominalVoltage ?BaseVoltage",
    ]
    return combine_statements(select, group_query(where))
