import contextlib
import logging
from copy import deepcopy
from typing import Any, Dict, Set

import pandas as pd
import pytest
import t_utils.common as t_common
import t_utils.entsoe_models as t_entsoe

from cimsparql.graphdb import GraphDBClient, make_async
from cimsparql.model import SingleClientModel

logger = logging.getLogger()


@contextlib.contextmanager
def tmp_client(model: SingleClientModel, new_client: GraphDBClient):
    orig_client = deepcopy(model.client)
    try:
        model.client = new_client
        yield model
    finally:
        model.client = orig_client


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
async def test_bus_data_micro_t1_nl(test_model: t_common.ModelTest, use_async: bool):
    t_common.check_model(test_model)
    model = test_model.model
    client = model.client
    if use_async:
        client = make_async(client)

    with tmp_client(model, client):
        data = await model.bus_data()

    assert len(data) == 12
    assert data.index.name == "node"


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
def test_cim_version_micro_t1_nl(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    model = test_model.model
    assert model.cim_version == 16


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
def test_repo_not_empty_micro_t1_nl(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    assert not test_model.model.client.empty


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
@pytest.mark.asyncio
async def test_cim_converters_micro_t1_nl(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    converters = await test_model.model.converters()
    assert converters.empty


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
@pytest.mark.asyncio
async def test_full_model_micro_t1_nl(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    data = await test_model.model.full_model()

    profiles = {
        "http://entsoe.eu/CIM/SteadyStateHypothesis/1/1",
        "http://entsoe.eu/CIM/Topology/4/1",
        "http://entsoe.eu/CIM/StateVariables/4/1",
    }
    assert profiles.issubset(data["profile"])

    row = data.query("model == 'urn:uuid:10c3fda3-35b7-47b0-b3c6-919c3e82e974'").iloc[0]
    assert row["profile"] == "http://entsoe.eu/CIM/SteadyStateHypothesis/1/1"
    assert row["time"] == "2017-10-02T09:30:00Z"
    assert row["version"] == "3"


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
@pytest.mark.asyncio
async def test_substation_voltage_level(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    data = await test_model.model.substation_voltage_level()
    assert sorted(data["v"].tolist()) == [225.0, 380.0, 400.0]


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
@pytest.mark.asyncio
async def test_branch_node_withdraw(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    data = await test_model.model.branch_node_withdraw()
    assert data.empty


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
@pytest.mark.asyncio
async def test_loads(test_model: t_common.ModelTest):
    t_common.check_model(test_model)

    data = await test_model.model.loads()
    assert len(data) == 6
    expected_names = {
        "BE-Load_1",
        "NL-Load_3",
        "L-1230804819",
        "BE-Load_2",
        "NL-Load_1",
        "NL-Load_2",
    }
    assert set(data["name"]) == expected_names


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "region,expected_names",
    [
        (".*", {"BE-G1", "NL-G2", "NL-G3", "NL-G1"}),
        ("NL", {"NL-G2", "NL-G3", "NL-G1"}),
        ("BE", {"BE-G1"}),
    ],
)
async def test_sync_machines(test_model: t_common.ModelTest, region: str, expected_names: Set[str]):
    t_common.check_model(test_model)
    data = await test_model.model.synchronous_machines(region)
    assert expected_names == set(data["name"])


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
@pytest.mark.asyncio
async def test_connections(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    data = await test_model.model.connections()
    assert len(data) == 26


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
@pytest.mark.asyncio
async def test_aclines(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    data = await test_model.model.ac_lines(region=".*")
    assert len(data) == 2


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params",
    [
        {
            "region": None,
            "num": 15,
            "end_num_count": [1, 7, 7],
            "expect_names": {
                "NL-TR2_1",
                "NL_TR2_2",
                "NL_TR2_3",
                "BE-TR2_1",
                "BE-TR2_2",
                "BE-TR2_3",
                "BE-TR3_1",
            },
        },
        {
            "region": "NL",
            "num": 6,
            "end_num_count": [3, 3],
            "expect_names": {"NL-TR2_1", "NL_TR2_2", "NL_TR2_3"},
        },
    ],
)
async def test_transformers(test_model: t_common.ModelTest, params: Dict[str, Any]):
    t_common.check_model(test_model)
    data = await test_model.model.transformers(params["region"])
    assert len(data) == params["num"]
    end_num_count = data["end_number"].value_counts()
    assert sorted(list(end_num_count)) == params["end_num_count"]
    assert set(data["name"].unique()) == params["expect_names"]


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params",
    [{"region": ".*", "num": 15, "num_transf": 7}, {"region": "NL", "num": 6, "num_transf": 3}],
)
async def test_transformers_connectivity(test_model: t_common.ModelTest, params: Dict[str, Any]):
    t_common.check_model(test_model)

    data = await test_model.model.transformers(params["region"])
    assert len(data) == params["num"]

    # Groupby mrid of the power transformer. Count gives the number of windings
    assert len(data.groupby(["p_mrid"]).count()) == params["num_transf"]


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
@pytest.mark.asyncio
async def test_two_winding_transformers(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    data = await test_model.model.two_winding_transformers()
    assert len(data) == 6
    expect_names = {"NL-TR2_1", "NL_TR2_2", "NL_TR2_3", "BE-TR2_1", "BE-TR2_2", "BE-TR2_3"}
    assert set(data["name"]) == expect_names
    assert data["r"].dtype == float


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
@pytest.mark.asyncio
async def test_three_winding_transformers(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    data = await test_model.model.three_winding_transformers()
    assert len(data) == 3


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
@pytest.mark.asyncio
async def test_disconnected(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    data = await test_model.model.disconnected()
    assert len(data) == 2

    # For convenience just test parts of mrids exists
    expected_partial_mrids = ["9f984b04", "f04ec73"]
    assert all(data["mrid"].str.contains(x).any() for x in expected_partial_mrids)


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
@pytest.mark.asyncio
async def test_powerflow(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    data = await test_model.model.powerflow()
    assert len(data) == 24


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
@pytest.mark.asyncio
async def test_regions(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    regions = await test_model.model.regions()
    assert set(regions["region"]) == {"BE", "EU", "NL"}


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
@pytest.mark.asyncio
async def test_coordinates(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    model = test_model.model
    crd = await model.coordinates()
    pd.testing.assert_index_equal(crd["epsg"].cat.categories, pd.Index(["4326"], dtype=str))

    cim = model.client.prefixes["cim"]
    categories = {f"{cim}ACLineSegment", f"{cim}Substation"}
    assert crd["rdf_type"].cat.categories.difference(categories).empty

    assert len(crd) == 49
    coordinates = crd.astype({"x": float, "y": float})

    assert ((coordinates["x"] > 4.0) & (coordinates["x"] < 6.0)).all()
    assert ((coordinates["y"] > 50.0) & (coordinates["y"] < 53.0)).all()


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
@pytest.mark.asyncio
async def test_empty_dc_active_flow(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    df = await test_model.model.dc_active_flow()
    assert df.empty


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
@pytest.mark.asyncio
async def test_transformer_windings(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    df = await test_model.model.transformer_windings()

    assert len(df) == 15
