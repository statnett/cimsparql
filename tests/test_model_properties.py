import asyncio
from dataclasses import dataclass
from typing import Union

import pandas as pd
import pytest
import t_utils.custom_models as t_custom
import t_utils.entsoe_models as t_entsoe

from cimsparql.data_models import BusDataFrame
from cimsparql.model import Model


@dataclass
class NodeConsistencyData:
    bus: BusDataFrame
    two_node_dfs: dict[str, pd.DataFrame]
    single_node_dfs: dict[str, pd.DataFrame]


# Alias for a dict with node consistency data. The key should be the name
# of the model
CONSISTENCY_DATA = dict[str, Union[NodeConsistencyData, None]]


async def get_node_consistency_test_data(
    model: Union[Model, None],
) -> Union[NodeConsistencyData, None]:
    if model is None:
        return None
    bus = await model.bus_data()

    # Results that has node_1 and node_2 which should be part of bus
    two_node_names = [
        "ac_lines",
        "series_compensators",
        "two_winding_transformers",
        "three_winding_transformers",
    ]
    two_node_dfs = await asyncio.gather(
        model.ac_lines(),
        model.series_compensators(),
        model.two_winding_transformers(),
        model.three_winding_transformers(),
    )

    # Results that has node which should be part of bus
    single_node_names = ["loads", "exchange", "converters", "branch_node_withdraw"]
    single_node_dfs = await asyncio.gather(
        model.loads(), model.exchange(), model.converters(), model.branch_node_withdraw()
    )
    return NodeConsistencyData(
        bus, dict(zip(two_node_names, two_node_dfs)), dict(zip(single_node_names, single_node_dfs))
    )


async def collect_node_consistency_data() -> list[NodeConsistencyData]:
    combined_custom = t_custom.combined_model().model
    bg_model = t_entsoe.micro_t1_nl_bg().model
    res = await asyncio.gather(
        get_node_consistency_test_data(combined_custom), get_node_consistency_test_data(bg_model)
    )
    return res


@pytest.fixture(scope="session")
def nc_data() -> CONSISTENCY_DATA:
    """
    Return test data which in a datastructure suitable for consistency checks
    for node data
    """
    res = asyncio.run(collect_node_consistency_data())
    return {"model": res[0], "t_entsoe.micro_t1_nl_bg": res[1]}


@pytest.mark.parametrize("model_name", ["model", "t_entsoe.micro_t1_nl_bg"])
def test_node_consistency(nc_data: CONSISTENCY_DATA, model_name: str):
    data = nc_data[model_name]
    if not data:
        pytest.skip(f"No data collected for {model_name}")

    mrids = set(data.bus.index)
    for name, df in data.two_node_dfs.items():
        msg = f"Error two node: {name}"
        assert set(df["node_1"]).issubset(mrids), msg
        assert set(df["node_2"]).issubset(mrids), msg

    for name, df in data.single_node_dfs.items():
        msg = f"Error single node: {name}"
        assert set(df["node"]).issubset(mrids), msg


@pytest.mark.parametrize("model_name", ["model", "t_entsoe.micro_t1_nl_bg"])
def test_two_or_three_winding(nc_data: CONSISTENCY_DATA, model_name: str):
    """
    Ensure that a transformer is either a two windinng transformer or three winding
    """
    data = nc_data[model_name]
    if not data:
        pytest.skip(f"No data collected for {model_name}")

    two_w = data.two_node_dfs["two_winding_transformers"].index
    three_w = data.two_node_dfs["three_winding_transformers"].index
    assert len(set(two_w).intersection(three_w)) == 0