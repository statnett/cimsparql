import networkx as nx
import pandas as pd

from typing import Tuple, Dict, List


def where_query(x: List) -> str:
    return "\nWHERE {\n\t" + " .\n\t".join(x) + "\n}"


def region_query(region: str, container: str) -> List:
    if region is None:
        return []
    else:
        return [
            f"?{container} cim:{container}.Region ?subgeographicalregion",
            "?subgeographicalregion cim:SubGeographicalRegion.Region ?region",
            "?region cim:IdentifiedObject.name ?regionname ",
            f"\tFILTER regex(str(?regionname), '{region}')",
        ]


def connectivity_mrid(
    var: str = "connectivity_mrid", sparql: bool = True, sequence_numbers: List[int] = [1, 2]
) -> [str, List]:
    if sparql:
        return " ".join([f"?{var}_{i}" for i in sequence_numbers])
    else:
        return [f"{var}_{i}" for i in sequence_numbers]


def acdc_terminal(cim_version: int) -> str:
    if cim_version > 15:
        return "ACDCTerminal"
    else:
        return "Terminal"


def terminal_where_query(
    cim_version: int = None, var: str = "connectivity_mrid", with_sequence_number: bool = False
) -> List:
    out = [
        "?terminal_mrid rdf:type cim:Terminal",
        "?terminal_mrid cim:Terminal.ConductingEquipment ?mrid",
        f"?terminal_mrid cim:Terminal.ConnectivityNode ?{var}",
    ]
    if with_sequence_number:
        out += [f"?terminal_mrid cim:{acdc_terminal(cim_version)}.sequenceNumber ?sequenceNumber"]
    return out


def terminal_sequence_query(
    cim_version: int, sequence_numbers: List[int] = [1, 2], var: str = "connectivity_mrid"
) -> List:
    query_list = []
    for i in sequence_numbers:
        mrid = f"?mrid_{i} "
        query_list += [
            mrid + "rdf:type cim:Terminal",
            mrid + f"cim:Terminal.ConductingEquipment ?mrid",
            mrid + f"cim:Terminal.ConnectivityNode ?{var}_{i}",
            mrid + f"cim:{acdc_terminal(cim_version)}.sequenceNumber {i}",
        ]
    return query_list


def load_query(conform: bool = True, region: str = "NO") -> str:
    container = "Substation"
    connectivity_mrid = "connectivity_mrid"
    select_query = f"SELECT ?mrid ?{connectivity_mrid} ?terminal_mrid"

    if conform:
        cim_type = "ConformLoad"
    else:
        cim_type = "NonConformLoad"

    where_list = (
        [
            f"?mrid rdf:type cim:{cim_type}",
            "?mrid cim:Equipment.EquipmentContainer ?container",
            f"?container cim:VoltageLevel.Substation ?{container}",
        ]
        + terminal_where_query(connectivity_mrid)
        + region_query(region, container)
    )
    return select_query + where_query(where_list)


def synchronous_machines_query(region: str = "NO") -> str:
    container = "Substation"
    connectivity_mrid = "connectivity_mrid"

    select_query = f"SELECT ?mrid ?sn ?{connectivity_mrid} ?terminal_mrid"
    where_list = (
        [
            "?mrid rdf:type cim:SynchronousMachine",
            "?mrid cim:RotatingMachine.ratedS ?sn",
            "?mrid cim:SynchronousMachine.type ?machine",
            "?machine rdfs:label 'generator'",
            "?mrid cim:Equipment.EquipmentContainer ?container",
            f"?container cim:VoltageLevel.Substation ?{container}",
        ]
        + terminal_where_query(connectivity_mrid)
        + region_query(region, container)
    )
    return select_query + where_query(where_list)


def transformer_query(region: str = "NO") -> str:
    container = "Substation"
    select_query = "SELECT ?mrid ?c ?x ?endNumber ?sn ?un ?connectivity_mrid ?terminal_mrid"
    where_list = [
        "?mrid rdf:type cim:PowerTransformer",
        "?c cim:PowerTransformerEnd.PowerTransformer ?mrid",
        "?c cim:PowerTransformerEnd.x ?x",
        "?c cim:PowerTransformerEnd.ratedU ?un",
        "?c cim:TransformerEnd.endNumber ?endNumber",
        "?c cim:TransformerEnd.Terminal ?t_mrid",
        "?t_mrid cim:Terminal.ConnectivityNode ?connectivity_mrid",
        f"?mrid cim:Equipment.EquipmentContainer ?{container}",
    ] + region_query(region, container)
    return select_query + where_query(where_list)


def ac_line_query(cim_version: int, region: str = "NO") -> str:
    container = "Line"
    connectivity = "connectivity_mrid"
    select_query = (
        f"SELECT {connectivity_mrid(connectivity)} ?x ?r ?bch ?length ?un ?mrid_1 ?mrid_2"
    )

    where_list = []
    ac_list = [
        "?mrid rdf:type cim:ACLineSegment",
        "?mrid cim:ACLineSegment.x ?x",
        "?mrid cim:ACLineSegment.r ?r",
        "?mrid cim:ACLineSegment.bch ?bch",
        "?mrid cim:Conductor.length ?length",
        "?mrid cim:ConductingEquipment.BaseVoltage ?obase",
        "?obase cim:BaseVoltage.nominalVoltage ?un",
    ]

    if region is not None:
        ac_list += [f"?mrid cim:Equipment.EquipmentContainer ?{container}"]

    where_list += terminal_sequence_query(cim_version=cim_version, var=connectivity)
    where_list += ac_list
    where_list += region_query(region, container)

    return select_query + where_query(where_list)


def connection_query(cim_version: int, rdf_type: str, region: str = "NO") -> str:
    connectivity = "connectivity_mrid"
    select_query = f"SELECT ?mrid {connectivity_mrid(connectivity)} ?mrid_1 ?mrid_2"
    where_list = (
        [
            f"?mrid rdf:type {rdf_type}",
            "?mrid cim:Equipment.EquipmentContainer ?EquipmentContainer",
            "?EquipmentContainer cim:Bay.VoltageLevel ?VoltageLevel",
            "?VoltageLevel cim:VoltageLevel.Substation ?Substation",
        ]
        + region_query(region, "Substation")
        + terminal_sequence_query(cim_version=cim_version, var=connectivity)
    )
    return select_query + where_query(where_list)


def windings_to_tr(windings: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    cols = ["x", "un", "connectivity_mrid"]
    wd = [
        windings[windings["endNumber"] == i][["mrid"] + cols]
        .rename(columns={f"{var}": f"{var}_{i}" for var in cols})
        .set_index(["mrid"])
        for i in range(1, 4)
    ]

    tr = pd.concat(wd, axis=1, sort=False)

    connectivity_mrids = connectivity_mrid(sparql=False)

    two_tr = tr[tr["x_3"].isna()][["x_1", "un_1"] + connectivity_mrids]
    two_tr.reset_index(inplace=True)
    two_tr.set_index(connectivity_mrids, inplace=True)

    three_tr = tr[tr["x_3"].notna()]
    return two_tr.rename(columns={"index": "mrid", "un_1": "un", "x_1": "x"}), three_tr


def reference_nodes(connections: pd.DataFrame) -> Dict:
    g = nx.Graph()
    g.add_edges_from(connections.to_numpy())
    node_dict = dict()
    for group in list(nx.connected_components(g)):
        ref = group.pop()
        node_dict[ref] = ref
        for node in group:
            node_dict[node] = ref
    return node_dict


def members(nodes: pd.DataFrame, branch: pd.DataFrame, columns: List) -> pd.DataFrame:
    return branch.loc[:, columns].isin(nodes.index).to_numpy().transpose()


def connect_nodes(nodes: pd.DataFrame, branch: pd.DataFrame, columns: List) -> pd.DataFrame:
    for indx, column in zip(members(nodes, branch, columns), columns):
        branch.loc[indx, column] = nodes.loc[branch.loc[indx, column].values].values.squeeze()


def branches(
    connectors: pd.DataFrame, lines: pd.DataFrame, windings: pd.DataFrame, columns: List
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    node_dict = reference_nodes(connectors.iloc[:, :2])
    node = pd.DataFrame.from_dict(node_dict, orient="index")

    two_tr, three_tr = windings_to_tr(windings)

    for winding in [two_tr, three_tr]:
        winding.reset_index(inplace=True)

    for br in [lines, two_tr]:
        connect_nodes(node, br, columns[:2])

    connect_nodes(node, three_tr, columns)

    return lines, two_tr, three_tr
