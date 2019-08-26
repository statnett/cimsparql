import pandas as pd

from cimsparql.redland import Model
from typing import Dict


def get_table_and_convert(model: Model, query: str, columns: Dict = None) -> pd.DataFrame:
    result = model.get_table(query)
    if len(result) > 0 and columns:
        for column, column_type in columns.items():
            result[column] = result[column].apply(str).astype(column_type)
    return result


def ssh_disconnected(model: Model, cimversion: str) -> pd.DataFrame:
    query = """
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
    return get_table_and_convert(model, cimversion + query)


def ssh_synchronous_machines(model: Model, cimversion: str) -> pd.DataFrame:
    query = """
    SELECT ?mrid ?p ?q ?controlEnabled
    WHERE {
    ?mrid rdf:type cim:SynchronousMachine .
    ?mrid cim:RotatingMachine.p ?p .
    ?mrid cim:RotatingMachine.p ?q .
    ?mrid cim:RegulatingCondEq.controlEnabled ?controlEnabled
    }
    """
    columns = {"p": float, "q": float, "controlEnabled": bool}
    return get_table_and_convert(model, cimversion + query, columns)


def ssh_load(model: Model, cimversion: str, conform: bool = True) -> pd.DataFrame:
    if conform:
        rdf_type = "ConformLoad"
    else:
        rdf_type = "NonConformLoad"

    query = "\nSELECT ?mrid ?p ?q\nWHERE {\n"
    query += " .\n".join(
        [
            f"?mrid rdf:type cim:{rdf_type}",
            "?mrid cim:EnergyConsumer.p ?p",
            "?mrid cim:EnergyConsumer.q ?q",
        ]
    )
    query += "\n}"
    columns = {"p": float, "q": float}
    return get_table_and_convert(model, cimversion + query, columns)


def ssh_combined_load(model: Model, cimversion: str) -> pd.DataFrame:
    query = """
    SELECT ?mrid ?p ?q
    WHERE {{
    ?mrid rdf:type cim:ConformLoad .
    ?mrid cim:EnergyConsumer.p ?p .
    ?mrid cim:EnergyConsumer.q ?q
    } UNION {
    ?mrid rdf:type cim:NonConformLoad .
    ?mrid cim:EnergyConsumer.p ?p .
    ?mrid cim:EnergyConsumer.q ?q
    }}
    """
    columns = {"p": float, "q": float}
    return get_table_and_convert(model, cimversion + query, columns)


def ssh_hydro_generating_unit(model: Model, cimversion: str) -> pd.DataFrame:
    pass


def ssh_thermal_generating_unit(model: Model, cimversion: str) -> pd.DataFrame:
    pass
