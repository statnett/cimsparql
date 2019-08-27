import pandas as pd

from cimsparql.redland import Model, get_table_and_convert
from typing import List


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
    ?mrid cim:RotatingMachine.q ?q .
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


def _generating_unit(rdf_type: str) -> str:
    return " .\n".join(
        [f"?mrid rdf:type {rdf_type}", "?mrid cim:GeneratingUnit.normalPF ?normalPF"]
    )


def ssh_generating_unit(model: Model, cimversion: str, rdf_type: str) -> pd.DataFrame:
    query = "SELECT ?mrid ?normalPF \n WHERE {\n"
    query += _generating_unit(rdf_type)
    query += "\n}"
    columns = {"normalPF": float}
    return get_table_and_convert(model, cimversion + query, columns).set_index("mrid")


def ssh_generating_unit_union(model: Model, cimversion: str, rdf_types: List[str]) -> pd.DataFrame:
    query = "SELECT ?mrid ?normalPF \n WHERE {{\n"
    query += "\n} UNION {\n".join([_generating_unit(rdf_type) for rdf_type in rdf_types])
    query += "}}\n"
    columns = {"normalPF": float}
    return get_table_and_convert(model, cimversion + query, columns).set_index("mrid")


def ssh_hydro_generating_unit(model: Model, cimversion: str) -> pd.DataFrame:
    return ssh_generating_unit(model, cimversion, "cim:HydroGeneratingUnit")


def ssh_thermal_generating_unit(model: Model, cimversion: str) -> pd.DataFrame:
    return ssh_generating_unit(model, cimversion, "cim:ThermalGeneratingUnit")


def ssh_wind_generating_unit(model: Model, cimversion: str) -> pd.DataFrame:
    return ssh_generating_unit(model, cimversion, "cim:WindGeneratingUnit")
