from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from string import Template
from typing import TYPE_CHECKING

import pytest

import tests.t_utils.common as t_common
import tests.t_utils.custom_models as t_custom
import tests.t_utils.entsoe_models as t_entsoe

if TYPE_CHECKING:
    import pandas as pd

    from cimsparql.data_models import BusDataFrame

swing_bus_query = Template(
    """
PREFIX cim:<${cim}>
select ?top_node ?island
where {
    ?island cim:TopologicalIsland.AngleRefTopoligicalNode ?top_node
}
"""
)


@dataclass
class NodeConsistencyData:
    bus: BusDataFrame
    two_node_dfs: dict[str, pd.DataFrame]
    single_node_dfs: dict[str, pd.DataFrame]
    swing_buses: pd.DataFrame


if TYPE_CHECKING:
    # Alias for a dict with node consistency data. The key should be the name
    # of the model
    from cimsparql.model import Model

    CONSISTENCY_DATA = dict[str, NodeConsistencyData | None]


async def get_node_consistency_test_data(
    model: Model | None,
) -> NodeConsistencyData | None:
    if model is None:
        return None
    loop = asyncio.get_event_loop()
    bus = await loop.run_in_executor(None, model.bus_data)

    # Results that has node_1 and node_2 which should be part of bus
    two_node_names = ["ac_lines", "series_compensators", "transformer_branches"]

    two_node_dfs = await asyncio.gather(
        loop.run_in_executor(None, model.ac_lines),
        loop.run_in_executor(None, model.series_compensators),
        loop.run_in_executor(None, model.transformer_branches),
    )

    # Results that has node which should be part of bus
    single_node_names = ["loads", "exchange", "converters", "branch_node_withdraw"]
    single_node_dfs = await asyncio.gather(
        loop.run_in_executor(None, model.loads),
        loop.run_in_executor(None, model.exchange),
        loop.run_in_executor(None, model.converters),
        loop.run_in_executor(None, model.branch_node_withdraw),
    )
    swing_buses = await loop.run_in_executor(
        None, model.get_table_and_convert, model.template_to_query(swing_bus_query)
    )
    return NodeConsistencyData(
        bus,
        dict(zip(two_node_names, two_node_dfs, strict=True)),
        dict(zip(single_node_names, single_node_dfs, strict=True)),
        swing_buses,
    )


async def collect_node_consistency_data() -> list[NodeConsistencyData]:
    test_models = [t_custom.federated_model(), t_entsoe.micro_t1_nl()]
    if any(m.model is None and m.must_run_in_ci and os.getenv("CI") for m in test_models):
        pytest.fail("Model that must run in CI is None")
    res = await asyncio.gather(*[get_node_consistency_test_data(m.model) for m in test_models])
    return res


@pytest.fixture(scope="session")
def nc_data() -> CONSISTENCY_DATA:
    """Return test data which in a datastructure suitable for consistency checks for node data."""
    res = asyncio.run(collect_node_consistency_data())
    return {"model": res[0]}


def skip_on_missing(data: CONSISTENCY_DATA | NodeConsistencyData | None, model_name: str):
    if not data:
        pytest.skip(f"No data collected for {model_name}")


@pytest.mark.parametrize("model_name", ["model"])
def test_node_consistency(nc_data: CONSISTENCY_DATA, model_name: str):
    data = nc_data[model_name]
    skip_on_missing(data, model_name)
    assert data

    mrids = set[str](data.bus.index)
    for name, df in data.two_node_dfs.items():
        msg = f"Error two node: {name}"
        assert set(df["node_1"]).issubset(mrids), msg
        assert set(df["node_2"]).issubset(mrids), msg

    for name, df in data.single_node_dfs.items():
        msg = f"Error single node: {name}"
        assert set(df["node"]).issubset(mrids), msg


@pytest.mark.parametrize("model_name", ["model"])
def test_swing_bus_consistency(nc_data: CONSISTENCY_DATA, model_name: str):
    data = nc_data[model_name]
    skip_on_missing(data, model_name)
    assert data.bus["is_swing_bus"].sum() == len(data.swing_buses)


@pytest.mark.parametrize("test_model", t_entsoe.micro_models() + t_custom.all_custom_models())
async def test_all_connectivity_nodes_fetched(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    assert test_model.model

    query = Template(
        """
        PREFIX cim:<${cim}>
        PREFIX SN:<${SN}>

        select (count(distinct ?s) as ?count) where  {
            values ?true {True "true"}
            ?s ^cim:Terminal.ConnectivityNode/cim:Terminal.ConductingEquipment/SN:Equipment.networkAnalysisEnable ?true
        }
        """
    )
    count_query = test_model.model.template_to_query(query)
    num_nodes_df = test_model.model.get_table_and_convert(count_query)
    num_connectivity_nodes = int(num_nodes_df["count"].iloc[0])

    # Verify that we get some nodes
    assert num_connectivity_nodes > 0

    df = test_model.model.connectivity_nodes()

    # Verify that we get information for all nodes in the model
    assert len(df) == num_connectivity_nodes
