import copy
import networkx as nx
import numpy as np
import pandas as pd

from typing import Tuple, List, Union, Set

allowed_load_types = ["ConformLoad", "NonConformLoad", "EnergyConsumer"]

con_mrid_str = "connectivity_mrid"
connectivity_columns = [f"{con_mrid_str}_{nr}" for nr in [1, 2]]

ratings = ("Normal", "Warning", "Overload")


def combine_statements(*args, group: bool = False, split: str = "\n"):
    if group:
        return "{\n" + split.join(args) + "\n}"
    else:
        return split.join(args)


def group_query(
    x: List[str], command: str = "WHERE", split: str = " .\n", group: bool = True
) -> str:
    return command + " " + combine_statements(*x, group=group, split=split)


def unionize(*args, group: bool = True):
    if group:
        args = [f"{{\n{arg}\n}}" for arg in args]
    return "\nUNION\n".join(args)


def regions_query() -> str:
    select_query = "SELECT ?mrid ?aliasName  ?name ?shortName ?region ?region_name"
    where_list = [
        "?mrid rdf:type cim:SubGeographicalRegion",
        "?mrid cim:IdentifiedObject.aliasName ?aliasName",
        "?mrid cim:IdentifiedObject.name ?name",
        "?mrid SN:IdentifiedObject.shortName ?shortName",
        "?mrid cim:SubGeographicalRegion.Region ?sub_geographical_region",
        "?sub_geographical_region cim:IdentifiedObject.name ?region",
        "?sub_geographical_region cim:IdentifiedObject.aliasName ?region_name",
    ]
    return combine_statements(select_query, group_query(where_list))


def market_code_query(nr: int = None):
    nr = "" if nr is None else f"_{nr}"
    return group_query(
        [
            f"?t_mrid{nr} cim:Terminal.ConnectivityNode ?con{nr}",
            f"?con{nr} cim:ConnectivityNode.ConnectivityNodeContainer ?container{nr}",
            f"?container{nr} cim:VoltageLevel.Substation ?substation{nr}",
            f"?substation{nr} SN:Substation.MarketDeliveryPoint ?market_delivery_point{nr}",
            f"?market_delivery_point{nr} SN:MarketDeliveryPoint.BiddingArea ?bidding_area{nr}",
            f"?bidding_area{nr} SN:BiddingArea.marketCode ?market{nr}",
        ],
        command="OPTIONAL",
    )


def region_query(region: str, sub_region: bool, container: str) -> List[str]:
    if region is None:
        query = []
    else:
        query = [f"?{container} cim:{container}.Region ?subgeographicalregion"]
        if sub_region:
            query += [f"?subgeographicalregion SN:IdentifiedObject.shortName '{region}'"]
        else:
            query += [
                "?subgeographicalregion cim:SubGeographicalRegion.Region ?region",
                f"?region cim:IdentifiedObject.name '{region}' ",
            ]
    return query


def connectivity_mrid(
    var: str = con_mrid_str, sparql: bool = True, sequence_numbers: Tuple[int] = (1, 2)
) -> [str, List[str]]:
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
) -> List[str]:
    out = [
        "?terminal_mrid rdf:type cim:Terminal",
        "?terminal_mrid cim:Terminal.ConductingEquipment ?mrid",
    ]
    if var is not None:
        out += [f"?terminal_mrid cim:Terminal.ConnectivityNode ?{var}"]

    if with_sequence_number:
        out += [f"?terminal_mrid cim:{acdc_terminal(cim_version)}.sequenceNumber ?sequenceNumber"]
    return out


bid_market_code_query = [
    "?mrid cim:Equipment.EquipmentContainer ?eq_container",
    "?eq_container cim:VoltageLevel.Substation ?substation",
    "?substation SN:Substation.MarketDeliveryPoint ?market_delivery_point",
    "?market_delivery_point SN:MarketDeliveryPoint.BiddingArea ?bidding_area",
    "?bidding_area SN:BiddingArea.marketCode ?bid_market_code",
]


def terminal_sequence_query(
    cim_version: int, sequence_numbers: Tuple[int] = (1, 2), var: str = con_mrid_str
) -> List[str]:
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


def bus_data(region: str = "NO", sub_region: bool = False) -> str:
    container = "Substation"
    select_query = "SELECT ?mrid ?name"
    where_list = ["?mrid rdf:type cim:TopologicalNode", "?mrid cim:IdentifiedObject.name ?name"]

    if region is not None:
        where_list += [
            "?mrid cim:TopologicalNode.ConnectivityNodeContainer ?container",
            f"?container cim:VoltageLevel.Substation ?{container}",
        ]
        where_list += region_query(region, sub_region, container)

    return combine_statements(select_query, group_query(where_list))


def load_query(
    load_type: List[str],
    load_vars: Tuple[str] = ("p", "q"),
    region: str = "NO",
    sub_region: bool = False,
    connectivity: str = con_mrid_str,
    cim_version: int = 15,
    with_sequence_number: bool = False,
    network_analysis: bool = True,
    station_group: bool = False,
) -> str:

    if not set(load_type).issubset(allowed_load_types) or len(load_type) == 0:
        raise ValueError(f"load_type should be any combination of {allowed_load_types}")

    container = "Substation"

    select_query = "SELECT ?mrid ?terminal_mrid ?bid_market_code " + " ".join(
        [f"?{p}" for p in load_vars]
    )

    if connectivity is not None:
        select_query += f" ?{connectivity}"

    cim_types = [f"?mrid rdf:type cim:{cim_type}" for cim_type in load_type]
    where_list = [combine_statements(*cim_types, group=len(cim_types) > 1, split="\n} UNION \n {")]
    where_list += [
        group_query([f"?mrid cim:EnergyConsumer.{p} ?{p}" for p in load_vars], command="OPTIONAL")
    ]
    where_list += terminal_where_query(cim_version, connectivity, with_sequence_number)
    where_list += bid_market_code_query

    if station_group:
        select_query += " ?station_group"
        where_list += [
            group_query(
                [
                    "?mrid cim:NonConformLoad.LoadGroup ?lg",
                    "?lg SN:NonConformLoadGroup.ScheduleResource ?sched_res",
                    "?sched_res SN:ScheduleResource.marketCode ?station_group",
                ],
                command="OPTIONAL",
            )
        ]

    if network_analysis is not None:
        where_list += [f"?mrid SN:Equipment.networkAnalysisEnable {network_analysis}"]

    if region is not None:
        where_list += [
            "?mrid cim:Equipment.EquipmentContainer ?container",
            f"?container cim:VoltageLevel.Substation ?{container}",
        ] + region_query(region, sub_region, container)

    return combine_statements(select_query, group_query(where_list))


def synchronous_machines_query(
    sync_vars: Tuple[str] = ("sn",),
    region: str = "NO",
    sub_region: bool = False,
    connectivity: str = con_mrid_str,
    cim_version: int = 15,
    with_sequence_number: bool = False,
    network_analysis: bool = True,
    u_groups: bool = False,
) -> str:
    var_dict = {"sn": "ratedS", "p": "p", "q": "q"}
    var_dict = {k: var_dict[k] for k in sync_vars}

    select_query = (
        "SELECT ?mrid ?name ?terminal_mrid ?station_group ?market_code ?maxP ?allocationMax "
        "?allocationWeight ?minP ?bid_market_code" + " ".join([f"?{var}" for var in sync_vars])
    )
    if connectivity is not None:
        select_query += f" ?{connectivity}"

    where_list = [
        "?mrid rdf:type cim:SynchronousMachine",
        "?mrid cim:IdentifiedObject.name ?name",
        "?mrid cim:SynchronousMachine.maxQ ?maxQ",
        "?mrid cim:SynchronousMachine.minQ ?minQ",
        group_query(
            ["?mrid cim:SynchronousMachine.type ?machine", "?machine rdfs:label 'generator'"],
            command="OPTIONAL",
        ),
    ]

    if network_analysis is not None:
        where_list += [f"?mrid SN:Equipment.networkAnalysisEnable {network_analysis}"]

    where_list += bid_market_code_query

    where_list += [
        group_query([f"?mrid cim:RotatingMachine.{var_dict[var]} ?{var}"], command="OPTIONAL")
        for var in sync_vars
    ]
    where_list += [
        group_query(
            [
                "?mrid cim:SynchronousMachine.GeneratingUnit ?gu",
                "?gu SN:GeneratingUnit.marketCode ?market_code",
                "?gu cim:GeneratingUnit.maxOperatingP ?maxP",
                "?gu cim:GeneratingUnit.minOperatingP ?minP",
                "?gu SN:GeneratingUnit.groupAllocationMax ?allocationMax",
                "?gu SN:GeneratingUnit.groupAllocationWeight ?allocationWeight",
                "?gu SN:GeneratingUnit.ScheduleResource ?ScheduleResource",
                "?ScheduleResource SN:ScheduleResource.marketCode ?station_group",
                "?ScheduleResource cim:IdentifiedObject.aliasName ?station_group_name",
            ],
            command="OPTIONAL",
        )
    ]
    where_list += terminal_where_query(cim_version, connectivity, with_sequence_number)

    if not u_groups:
        where_list += [
            f"FILTER (!bound(?station_group_name) || (!regex(?station_group_name, 'U-')))"
        ]
    if region is not None:
        container = "Substation"
        where_list += [
            "?mrid cim:Equipment.EquipmentContainer ?container",
            f"?container cim:VoltageLevel.Substation ?{container}",
        ] + region_query(region, sub_region, container)
    return combine_statements(select_query, group_query(where_list))


def operational_limit(mrid: str, rate: str, limitset: str = "operationallimitset") -> List[str]:

    return [
        f"?{limitset} cim:OperationalLimitSet.Equipment {mrid}",
        f"?activepowerlimit{rate} cim:OperationalLimit.OperationalLimitSet ?{limitset}",
        f"?activepowerlimit{rate} rdf:type cim:ActivePowerLimit",
        f"?activepowerlimit{rate} cim:IdentifiedObject.name '{rate}@20'",
        f"?activepowerlimit{rate} cim:ActivePowerLimit.value ?rate{rate}",
    ]


def wind_generating_unit_query(network_analysis: bool = True):

    select_query = (
        "SELECT ?mrid ?station_group ?market_code ?maxP "
        "?allocationMax ?allocationWeight ?minP ?name ?power_plant_mrid"
    )
    where_list = [
        "?mrid rdf:type cim:WindGeneratingUnit",
        "?mrid cim:GeneratingUnit.maxOperatingP ?maxP",
        "?mrid SN:GeneratingUnit.marketCode ?market_code",
        "?mrid cim:GeneratingUnit.minOperatingP ?minP",
        "?mrid cim:IdentifiedObject.name ?name",
        "?mrid SN:WindGeneratingUnit.WindPowerPlant ?power_plant_mrid",
        "?mrid SN:GeneratingUnit.groupAllocationMax ?allocationMax",
        "?mrid SN:GeneratingUnit.groupAllocationWeight ?allocationWeight",
        "?mrid SN:GeneratingUnit.ScheduleResource ?sr",
        "?sr SN:ScheduleResource.marketCode ?station_group",
    ]

    if network_analysis is not None:
        where_list += [f"?mrid SN:Equipment.networkAnalysisEnable {network_analysis}"]

    return combine_statements(select_query, group_query(where_list))


def transformer_query(
    region: str = "NO",
    sub_region: bool = False,
    connectivity: str = con_mrid_str,
    rates: Tuple[str] = ratings,
    network_analysis: bool = True,
    with_market: bool = False,
) -> str:
    container = "Substation"

    select_query = "SELECT ?name ?mrid ?c ?x ?r ?endNumber ?sn ?un ?t_mrid "

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

    if with_market:
        select_query += "?market "
        where_list += [market_code_query()]

    if network_analysis is not None:
        where_list += [f"?mrid SN:Equipment.networkAnalysisEnable {network_analysis}"]

    if connectivity is not None:
        select_query += f" ?{connectivity}"
        where_list += [f"?t_mrid cim:Terminal.ConnectivityNode ?{connectivity}"]

    if region is not None:
        where_list += [f"?mrid cim:Equipment.EquipmentContainer ?{container}"]
        where_list += region_query(region, sub_region, container)

    if rates:
        limitset = "operationallimitset"
        where_rate = [f"?{limitset} cim:OperationalLimitSet.Terminal ?t_mrid"]

        for rate in rates:
            select_query += f" ?rate{rate}"
            where_rate += operational_limit("?mrid", rate, limitset)
        where_list += [group_query(where_rate, command="OPTIONAL")]
    return combine_statements(select_query, group_query(where_list))


def series_compensator_query(
    cim_version: int,
    region: str = "NO",
    sub_region: bool = False,
    connectivity: str = con_mrid_str,
    network_analysis: bool = True,
    with_market: bool = False,
):
    container = "Substation"

    select_query = "Select ?mrid ?x ?un ?name ?t_mrid_1 ?t_mrid_2 "

    if connectivity is not None:
        select_query += f"{connectivity_mrid(connectivity)} "

    where_list = terminal_sequence_query(cim_version=cim_version, var=connectivity)
    if with_market:
        select_query += "?market_1 ?market_2 "
        for terminal_nr in [1, 2]:
            where_list += [market_code_query(terminal_nr)]

    where_list += [
        "?mrid rdf:type cim:SeriesCompensator",
        "?mrid cim:SeriesCompensator.x ?x",
        "?mrid cim:ConductingEquipment.BaseVoltage ?obase",
        "?obase cim:BaseVoltage.nominalVoltage ?un",
        "?mrid cim:IdentifiedObject.name ?name",
    ]
    if network_analysis is not None:
        where_list += [f"?mrid SN:Equipment.networkAnalysisEnable {network_analysis}"]

    if region is not None:
        where_list += [
            "?mrid cim:Equipment.EquipmentContainer ?EquipmentContainer",
            "?EquipmentContainer cim:VoltageLevel.Substation ?Substation",
        ]
        where_list += region_query(region, sub_region, container)

    return combine_statements(select_query, group_query(where_list))


def ac_line_query(
    cim_version: int,
    region: str = "NO",
    sub_region: bool = False,
    connectivity: str = con_mrid_str,
    rates: Tuple[str] = ratings,
    network_analysis: bool = True,
    with_market: bool = True,
) -> str:
    container = "Line"

    select_query = "SELECT ?name ?mrid ?x ?r ?bch ?length ?un ?t_mrid_1 ?t_mrid_2 "

    if connectivity is not None:
        select_query += f"{connectivity_mrid(connectivity)} "

    where_list = terminal_sequence_query(cim_version=cim_version, var=connectivity)
    if with_market:
        select_query += "?market_1 ?market_2 "
        for terminal_nr in [1, 2]:
            where_list += [market_code_query(terminal_nr)]

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

    if network_analysis is not None:
        where_list += [f"?mrid SN:Equipment.networkAnalysisEnable {network_analysis}"]

    if region is not None:
        where_list += [f"?mrid cim:Equipment.EquipmentContainer ?{container}"]
        where_list += region_query(region, sub_region, container)

    if rates:
        where_rate = []
        for rate in rates:
            select_query += f"?rate{rate} "
            where_rate += operational_limit("?mrid", rate)
        where_list += [group_query(where_rate, command="OPTIONAL")]

    return combine_statements(select_query, group_query(where_list))


def connection_query(
    cim_version: int,
    rdf_types: Union[str, List[str]],
    region: str = "NO",
    sub_region: bool = False,
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
        ] + region_query(region, sub_region, "Substation")

    where_list += terminal_sequence_query(cim_version=cim_version, var=connectivity)

    return combine_statements(select_query, group_query(where_list))


def winding_from_three_tx(three_tx: pd.DataFrame, i: int) -> pd.DataFrame:
    columns = [col for col in three_tx.columns if col.endswith(f"_{i}") or col == "mrid"]
    winding = three_tx[columns]
    t_mrid = f"t_mrid_{i}"
    rename_columns = {
        column: "_".join(column.split("_")[:-1])
        for column in columns
        if column not in [t_mrid, "mrid"]
    }
    rename_columns[t_mrid] = "t_mrid_1"
    return winding.rename(columns=rename_columns)


def winding_list(three_tx: pd.DataFrame) -> List[pd.DataFrame]:
    return [winding_from_three_tx(three_tx, i) for i in [1, 2, 3]]


def three_tx_to_windings(three_tx: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    three_tx.reset_index(inplace=True)
    three_tx.rename(columns={"index": "mrid"}, inplace=True)
    windings = pd.concat(winding_list(three_tx), ignore_index=True)
    windings["b"] = 1 / windings["x"]
    windings["ckt"] = windings["t_mrid_1"]
    windings["t_mrid_2"] = windings["mrid"]
    windings["market_1"] = windings["market_2"] = windings["market"]
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
        "market",
        con_mrid_str,
        "rateNormal",
        "rateWarning",
        "rateOverload",
    ]
    cols = [col for col in possible_columns if col in windings.columns]

    # Three winding includes endNumber == 3
    three_winding_mrid = windings[windings["endNumber"] == 3]["mrid"]
    three_windings = windings.loc[windings["mrid"].isin(three_winding_mrid), :]
    wd = [windings_set_end(three_windings, i, cols).set_index("mrid") for i in range(1, 4)]
    three_tx = pd.concat(wd, axis=1, sort=False)

    # While two winding transformers don't. Combine first and second winding.
    two_tx_group = windings[~windings["mrid"].isin(three_winding_mrid)].groupby("endNumber")
    two_tx = two_tx_group.get_group(1).set_index("mrid")
    two_tx_2 = two_tx_group.get_group(2).set_index("mrid")
    for col in ["t_mrid", "market"]:
        two_tx.loc[two_tx_2.index, f"{col}_2"] = two_tx_2[col]
    two_tx.loc[two_tx_2.index, "x"] += two_tx_2["x"]
    two_tx = two_tx.reset_index().rename(
        columns={"mrid": "ckt", "t_mrid": "t_mrid_1", "market": "market_1"}
    )
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

    def groups(self) -> List[Set]:
        return copy.deepcopy(self._groups)
