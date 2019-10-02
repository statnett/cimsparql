import copy
import networkx as nx
import numpy as np
import pandas as pd

from typing import Tuple, List, Union

allowed_load_types = ["ConformLoad", "NonConformLoad", "EnergyConsumer"]

con_mrid_str = "connectivity_mrid"
connectivity_columns = [f"{con_mrid_str}_{nr}" for nr in [1, 2]]


def combine_statements(*args, group: bool = False, split: str = "\n"):
    if group:
        return "{\n" + split.join(args) + "\n}"
    else:
        return split.join(args)


def group_query(x: List, command: str = "WHERE", split: str = " .\n", group: bool = True) -> str:
    return command + " " + combine_statements(*x, group=group, split=split)


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
    var: str = con_mrid_str, sparql: bool = True, sequence_numbers: List[int] = [1, 2]
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
    cim_version: int = None, var: str = con_mrid_str, with_sequence_number: bool = False
) -> List:
    out = [
        "?terminal_mrid rdf:type cim:Terminal",
        "?terminal_mrid cim:Terminal.ConductingEquipment ?mrid",
    ]
    if var is not None:
        out += [f"?terminal_mrid cim:Terminal.ConnectivityNode ?{var}"]

    if with_sequence_number:
        out += [f"?terminal_mrid cim:{acdc_terminal(cim_version)}.sequenceNumber ?sequenceNumber"]
    return out


def terminal_sequence_query(
    cim_version: int, sequence_numbers: List[int] = [1, 2], var: str = con_mrid_str
) -> List:
    query_list = []
    for i in sequence_numbers:
        mrid = f"?t_mrid_{i} "
        query_list += [
            mrid + "rdf:type cim:Terminal",
            mrid + f"cim:Terminal.ConductingEquipment ?mrid",
            mrid + f"cim:{acdc_terminal(cim_version)}.sequenceNumber {i}",
        ]
        if var is not None:
            query_list += [mrid + f"cim:Terminal.ConnectivityNode ?{var}_{i}"]

    return query_list


def connectivity_names() -> str:
    select_query = "SELECT ?mrid ?name"
    where_list = ["?mrid rdf:type cim:ConnectivityNode", "?mrid cim:IdentifiedObject.name ?name"]
    return combine_statements(select_query, group_query(where_list))


def bus_data(region: str = "NO") -> str:
    container = "Substation"
    select_query = "SELECT ?mrid ?name"
    where_list = ["?mrid rdf:type cim:TopologicalNode", "?mrid cim:IdentifiedObject.name ?name"]

    if region is not None:
        where_list += [
            "?mrid cim:TopologicalNode.ConnectivityNodeContainer ?container",
            f"?container cim:VoltageLevel.Substation ?{container}",
        ]
        where_list += region_query(region, container)

    return combine_statements(select_query, group_query(where_list))


def load_query(
    load_type: List[str],
    load_vars: List[str] = ["p", "q"],
    region: str = "NO",
    connectivity: str = con_mrid_str,
    cim_version: int = 15,
    with_sequence_number: bool = False,
) -> str:

    if not set(load_type).issubset(allowed_load_types) or len(load_type) == 0:
        raise ValueError(f"load_type should be any combination of {allowed_load_types}")

    container = "Substation"

    select_query = "SELECT ?mrid ?terminal_mrid " + " ".join([f"?{p}" for p in load_vars])

    if connectivity is not None:
        select_query += f" ?{connectivity}"

    cim_types = [f"?mrid rdf:type cim:{cim_type}" for cim_type in load_type]
    where_list = [combine_statements(*cim_types, group=len(cim_types) > 1, split="\n} UNION \n {")]
    where_list += [
        group_query([f"?mrid cim:EnergyConsumer.{p} ?{p}" for p in load_vars], command="OPTIONAL")
    ]
    where_list += terminal_where_query(cim_version, connectivity, with_sequence_number)

    if region is not None:
        where_list += [
            "?mrid cim:Equipment.EquipmentContainer ?container",
            f"?container cim:VoltageLevel.Substation ?{container}",
        ] + region_query(region, container)

    return combine_statements(select_query, group_query(where_list))


def synchronous_machines_query(
    sync_vars: List[str] = ("sn",),
    region: str = "NO",
    connectivity: str = con_mrid_str,
    cim_version: int = 15,
    with_sequence_number: bool = False,
) -> str:
    var_dict = {"sn": "ratedS", "p": "p", "q": "q"}
    select_query = (
        "SELECT ?mrid ?terminal_mrid ?station_group ?market_code ?maxP ?allocationMax "
        "?allocationWeight ?minP ?maxQ ?minQ" + " ".join([f"?{var}" for var in sync_vars])
    )
    if connectivity is not None:
        select_query += f" ?{connectivity}"

    where_list = [
        "?mrid rdf:type cim:SynchronousMachine",
        "?mrid cim:SynchronousMachine.maxQ ?maxQ",
        "?mrid cim:SynchronousMachine.minQ ?minQ",
        "OPTIONAL { ?mrid cim:SynchronousMachine.type ?machine",
        "?machine rdfs:label 'generator' }",
    ]
    where_list += [
        group_query([f"?mrid cim:RotatingMachine.{var_dict[var]} ?{var}"], command="OPTIONAL")
        for var in sync_vars
    ]
    where_list += [
        "OPTIONAL { ?mrid cim:SynchronousMachine.GeneratingUnit ?gu",
        "?gu SN:GeneratingUnit.marketCode ?market_code",
        "?gu cim:GeneratingUnit.maxOperatingP ?maxP",
        "?gu cim:GeneratingUnit.minOperatingP ?minP",
        "?gu SN:GeneratingUnit.groupAllocationMax ?allocationMax",
        "?gu SN:GeneratingUnit.groupAllocationWeight ?allocationWeight",
        "?gu SN:GeneratingUnit.ScheduleResource ?ScheduleResource",
        "?ScheduleResource SN:ScheduleResource.marketCode ?station_group}",
    ]
    where_list += terminal_where_query(cim_version, connectivity, with_sequence_number)

    if region is not None:
        container = "Substation"
        where_list += [
            "?mrid cim:Equipment.EquipmentContainer ?container",
            f"?container cim:VoltageLevel.Substation ?{container}",
        ] + region_query(region, container)

    return combine_statements(select_query, group_query(where_list))


def operational_limit(mrid: str, rate: str, limitset: str = "operationallimitset") -> List[str]:
    return [
        f"?{limitset} cim:OperationalLimitSet.Equipment {mrid}",
        f"?activepowerlimit{rate} cim:OperationalLimit.OperationalLimitSet ?{limitset}",
        f"?activepowerlimit{rate} rdf:type cim:ActivePowerLimit",
        f"?activepowerlimit{rate} cim:IdentifiedObject.name ?limitname{rate}",
        f"?activepowerlimit{rate} cim:ActivePowerLimit.value ?rate{rate}",
        f"filter regex(str(?limitname{rate}), '{rate}')",
    ]


def transformer_query(
    region: str = "NO",
    connectivity: str = con_mrid_str,
    rates: List[str] = ["Normal", "Warning", "Overload"],
) -> str:
    container = "Substation"

    select_query = "SELECT ?name ?mrid ?c ?x ?r ?endNumber ?sn ?un ?t_mrid"

    where_list = [
        "?mrid rdf:type cim:PowerTransformer",
        "?c cim:PowerTransformerEnd.PowerTransformer ?mrid",
        "?c cim:PowerTransformerEnd.x ?x",
        "?c cim:PowerTransformerEnd.r ?r",
        "?c cim:PowerTransformerEnd.ratedU ?un",
        "?c cim:TransformerEnd.endNumber ?endNumber",
        "?c cim:TransformerEnd.Terminal ?t_mrid",
        "?c cim:IdentifiedObject.name ?name",
    ]
    if connectivity is not None:
        select_query += f" ?{connectivity}"
        where_list += [f"?t_mrid cim:Terminal.ConnectivityNode ?{connectivity}"]

    if region is not None:
        where_list += [f"?mrid cim:Equipment.EquipmentContainer ?{container}"]
        where_list += region_query(region, container)

    if rates:
        limitset = "operationallimitset"
        where_rate = [f"?{limitset} cim:OperationalLimitSet.Terminal ?t_mrid"]

        for rate in rates:
            select_query += f" ?rate{rate}"
            where_rate += operational_limit("?mrid", rate, limitset)
        where_list += [group_query(where_rate, command="OPTIONAL")]
    return combine_statements(select_query, group_query(where_list))


def ac_line_query(
    cim_version: int,
    region: str = "NO",
    connectivity: str = con_mrid_str,
    rates: List[str] = ["Normal", "Warning", "Overload"],
) -> str:
    container = "Line"

    select_query = "SELECT ?name ?mrid ?x ?r ?bch ?length ?un ?t_mrid_1 ?t_mrid_2 "

    if connectivity is not None:
        select_query += f"{connectivity_mrid(connectivity)} "

    where_list = terminal_sequence_query(cim_version=cim_version, var=connectivity)
    where_list += [
        "?mrid rdf:type cim:ACLineSegment",
        "?mrid cim:ACLineSegment.x ?x",
        "?mrid cim:ACLineSegment.r ?r",
        "?mrid cim:ACLineSegment.bch ?bch",
        "?mrid cim:Conductor.length ?length",
        "?mrid cim:ConductingEquipment.BaseVoltage ?obase",
        "?obase cim:BaseVoltage.nominalVoltage ?un",
        "?mrid cim:IdentifiedObject.name ?name",
    ]

    if region is not None:
        where_list += [f"?mrid cim:Equipment.EquipmentContainer ?{container}"]
        where_list += region_query(region, container)

    for rate in rates:
        select_query += f"?rate{rate} "
        where_list += operational_limit("?mrid", rate)

    return combine_statements(select_query, group_query(where_list))


def connection_query(
    cim_version: int,
    rdf_types: Union[str, List[str]],
    region: str = "NO",
    connectivity: str = con_mrid_str,
) -> str:

    select_query = "SELECT ?mrid  ?t_mrid_1 ?t_mrid_2"

    if connectivity is not None:
        select_query += f" {connectivity_mrid(connectivity)}"

    if isinstance(rdf_types, str):
        rdf_types = [rdf_types]

    cim_types = [f"?mrid rdf:type {rdf_type}" for rdf_type in rdf_types]
    where_list = [combine_statements(*cim_types, group=len(cim_types) > 1, split="\n} UNION \n {")]

    if region is not None:
        where_list += [
            "?mrid cim:Equipment.EquipmentContainer ?EquipmentContainer",
            "?EquipmentContainer cim:Bay.VoltageLevel ?VoltageLevel",
            "?VoltageLevel cim:VoltageLevel.Substation ?Substation",
        ] + region_query(region, "Substation")

    where_list += terminal_sequence_query(cim_version=cim_version, var=connectivity)

    return combine_statements(select_query, group_query(where_list))


def winding_from_three_tx(three_tx: pd.DataFrame, i: int) -> pd.DataFrame:
    winding = three_tx[[f"x_{i}", f"name_{i}", f"t_mrid_{i}", "mrid"]]
    return winding.rename(columns={f"x_{i}": "x", f"name_{i}": "name", f"t_mrid_{i}": "t_mrid_1"})


def winding_list(three_tx: pd.DataFrame) -> List[pd.DataFrame]:
    return [winding_from_three_tx(three_tx, i) for i in [1, 2, 3]]


def three_tx_to_windings(three_tx: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    three_tx.reset_index(inplace=True)
    three_tx.rename(columns={"index": "mrid"}, inplace=True)
    windings = pd.concat(winding_list(three_tx), ignore_index=True)
    windings["b"] = 1 / windings["x"]
    windings["ckt"] = windings["mrid"]
    windings["t_mrid_2"] = windings["mrid"]
    return windings.loc[:, cols]


def windings_set_end(windings: pd.DataFrame, i: int, cols: List[str]):
    columns = {f"{var}": f"{var}_{i}" for var in cols}
    return windings[windings["endNumber"] == i][["mrid"] + cols].rename(columns=columns)


def windings_to_tx(windings: pd.DataFrame) -> Tuple[pd.DataFrame]:
    possible_columns = [
        "name",
        "x",
        "un",
        "t_mrid",
        con_mrid_str,
        "rateNormal",
        "rateWarning",
        "rateOverload",
    ]
    cols = [col for col in possible_columns if col in windings.columns]
    three_winding_mrid = windings[windings["endNumber"] == 3]["mrid"]
    two_tx = windings[~windings["mrid"].isin(three_winding_mrid)]
    two_tx = two_tx.rename(columns={"mrid": "ckt"})
    three_windings = windings.loc[windings["mrid"].isin(three_winding_mrid), :]
    wd = [windings_set_end(three_windings, i, cols).set_index("mrid") for i in range(1, 4)]
    three_tx = pd.concat(wd, axis=1, sort=False)
    return two_tx, three_tx


class Islands(nx.Graph):
    def __init__(self, connections: pd.DataFrame):
        super().__init__()
        self.add_edges_from(connections.to_numpy())
        self._groups = list(nx.connected_components(self))

    def reference_nodes(self, columns: List[str] = ["mrid", "ref_node"]) -> pd.DataFrame:
        keys = list()
        values = list()
        for group in self.groups():
            ref = list(group)[0]
            keys += list(group)
            values += [ref] * len(group)
        return pd.DataFrame(np.array([keys, values]).transpose(), columns=columns).set_index("mrid")

    def groups(self) -> List:
        return copy.deepcopy(self._groups)
