import pandas as pd

from typing import List


def disconnected() -> str:
    return """
    SELECT ?mrid
    WHERE {{
    ?mrid ?p cim:Disconnector .
    ?mrid cim:Switch.open ?status .
    FILTER (?status = \"true\")
    } UNION {
    ?mrid ?p cim:Terminal .
    ?mrid cim:ACDCTerminal.connected ?connected .
    FILTER (?connected = \"false\")
    }}
    """


def synchronous_machines() -> str:
    return """
    SELECT ?mrid ?p ?q ?controlEnabled
    WHERE {
    ?mrid rdf:type cim:SynchronousMachine .
    ?mrid cim:RotatingMachine.p ?p .
    ?mrid cim:RotatingMachine.q ?q .
    ?mrid cim:RegulatingCondEq.controlEnabled ?controlEnabled
    }
    """


def _load(rdf_type: str) -> str:
    return " .\n".join(
        [f"?mrid rdf:type {rdf_type}"] + [f"?mrid cim:EnergyConsumer.{s} ?{s}" for s in ["p", "q"]]
    )


def load(rdf_types: List[str]) -> str:
    query = "\nSELECT ?mrid ?p ?q\nWHERE {{\n"
    query += "\n} UNION {\n".join([_load(rdf_type) for rdf_type in rdf_types])
    query += "}}\n"
    return query


def _generating_unit(rdf_type: str) -> str:
    return " .\n".join(
        [f"?mrid rdf:type {rdf_type}", "?mrid cim:GeneratingUnit.normalPF ?normalPF"]
    )


def generating_unit(rdf_types: List[str]) -> pd.DataFrame:
    query = "SELECT ?mrid ?normalPF \n WHERE {{\n"
    query += "\n} UNION {\n".join([_generating_unit(rdf_type) for rdf_type in rdf_types])
    query += "}}\n"
    return query
