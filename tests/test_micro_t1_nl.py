import contextlib
from copy import deepcopy
from string import Template
from typing import Any

import pandas as pd
import pytest

import tests.t_utils.common as t_common
import tests.t_utils.entsoe_models as t_entsoe
from cimsparql.graphdb import GraphDBClient
from cimsparql.model import SingleClientModel


@contextlib.contextmanager
def tmp_client(model: SingleClientModel, new_client: GraphDBClient):
    orig_client = deepcopy(model.client)
    try:
        model.client = new_client
        yield model
    finally:
        model.client = orig_client


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
def test_bus_data_micro_t1_nl(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    model = test_model.model
    assert model
    data = model.bus_data()

    # 11 topological nodes + 1 dummy node three windnig transformer
    # + 6 dummy nodes for two winding transformer
    assert len(data) == 18
    assert data.index.name == "node"


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
def test_cim_version_micro_t1_nl(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    model = test_model.model
    assert model
    assert model.cim_version == 16


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
def test_repo_not_empty_micro_t1_nl(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    assert test_model.model
    assert not test_model.model.client.empty


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
def test_cim_converters_micro_t1_nl(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    assert test_model.model
    converters = test_model.model.converters()
    assert converters.empty


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
def test_full_model_micro_t1_nl(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    assert test_model.model
    data = test_model.model.full_model()

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
def test_substation_voltage_level(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    assert test_model.model
    data = test_model.model.substation_voltage_level()
    assert sorted(data["v"].tolist()) == [225.0, 380.0, 400.0]


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
def test_branch_node_withdraw(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    assert test_model.model
    data = test_model.model.branch_node_withdraw()
    assert data.empty


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
def test_loads(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    assert test_model.model
    data = test_model.model.loads()
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
@pytest.mark.parametrize(
    "region,expected_names",
    [
        (".*", {"BE-G1", "NL-G2", "NL-G3", "NL-G1"}),
        ("NL", {"NL-G2", "NL-G3", "NL-G1"}),
        ("BE", {"BE-G1"}),
    ],
)
def test_sync_machines(test_model: t_common.ModelTest, region: str, expected_names: set[str]):
    t_common.check_model(test_model)
    assert test_model.model
    data = test_model.model.synchronous_machines(region)
    assert expected_names == set(data["name"])


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
def test_connections(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    assert test_model.model
    data = test_model.model.connections()
    assert len(data) == 26


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
def test_ltc_fixed_angle_equals_zero(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    assert test_model.model
    model = test_model.model
    branches = model.get_table_and_convert(model.transformer_branches_query(), index="mrid")
    angle = model.winding_angle()
    pytest.approx(branches.loc[angle.index, "angle"] == 0.0)


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
def test_aclines(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    assert test_model.model
    data = test_model.model.ac_lines(region=".*")
    assert len(data) == 2


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
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
def test_transformers(test_model: t_common.ModelTest, params: dict[str, Any]):
    t_common.check_model(test_model)
    assert test_model.model
    data = test_model.model.transformers(params["region"])
    assert len(data) == params["num"]
    end_num_count = data["end_number"].value_counts()
    assert sorted(end_num_count) == params["end_num_count"]
    assert set(data["name"].unique()) == params["expect_names"]


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
@pytest.mark.parametrize(
    "params",
    [{"region": ".*", "num": 15, "num_transf": 7}, {"region": "NL", "num": 6, "num_transf": 3}],
)
def test_transformers_connectivity(test_model: t_common.ModelTest, params: dict[str, Any]):
    t_common.check_model(test_model)

    assert test_model.model
    data = test_model.model.transformers(params["region"])
    assert len(data) == params["num"]

    # Groupby mrid of the power transformer. Count gives the number of windings
    assert len(data.groupby(["p_mrid"]).count()) == params["num_transf"]


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
def test_windings(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    assert test_model.model
    data = test_model.model.transformer_branches()
    expect_names = {
        "051cb1e0-2d34-3ac3-487e-a04c30781280": "NL_TR2_2",
        "1464d1bc-4a33-634c-7d93-b550dadf8b06": "NL-TR2_1",
        "bd9f254a-6448-10e5-657b-eb33b827887d": "NL-TR2_1",
        "f8ea6566-e73c-547d-a572-526e45334891": "NL_TR2_3",
        "91ec0b2c-2447-b8da-a141-90c0c35b33d6": "NL_TR2_2",
        "af598929-431f-3ef3-e636-87923c96f17c": "NL_TR2_3",
        "5f68a129-d5d8-4b71-9743-9ca2572ba26b": "BE-TR3_1",
        "49ca3fd4-1b54-4c5b-83fd-4dbd0f9fec9d": "BE-TR2_1",
        "81a18364-0397-48d3-b850-22a0e34b410f": "BE-TR2_2",
        "e1f661c0-971d-4ce5-ad39-0ec427f288ab": "BE-TR3_1",
        "664a19e1-1dc2-48d5-b265-c0630981e61c": "BE-TR2_2",
        "f58281c5-862a-465e-97ec-d809be6e24ab": "BE-TR2_3",
        "1912224a-9e98-41aa-84cf-00875bce7264": "BE-TR2_1",
        "2e21d1ef-2287-434c-a767-1ca807cf2478": "BE-TR3_1",
        "35651e25-a77a-46a1-92f4-443d6acce90e": "BE-TR2_3",
    }
    assert data["name"].to_dict() == expect_names
    assert data["status"].all()

    # On transformer branches, the mrid of node_2 should be a transformer
    transformers = test_model.model.transformers()
    assert set(data["node_2"]).issubset(set(transformers["p_mrid"]))


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
async def test_disconnected(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    assert test_model.model
    data = test_model.model.disconnected()
    assert len(data) == 2

    # For convenience just test parts of mrids exists
    expected_partial_mrids = ["9f984b04", "f04ec73"]
    assert all(data["mrid"].str.contains(x).any() for x in expected_partial_mrids)


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
def test_powerflow(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    assert test_model.model
    data = test_model.model.powerflow()
    assert len(data) == 24


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
def test_regions(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    assert test_model.model
    regions = test_model.model.regions()
    assert set(regions["region"]) == {"BE", "EU", "NL"}


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
def test_coordinates(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    assert test_model.model
    model = test_model.model
    crd = model.coordinates()
    pd.testing.assert_index_equal(crd["epsg"].cat.categories, pd.Index(["4326"], dtype=str))

    cim = model.client.prefixes["cim"]
    categories = {f"{cim}ACLineSegment", f"{cim}Substation"}
    assert crd["rdf_type"].cat.categories.difference(categories).empty

    assert len(crd) == 49
    coordinates = crd.astype({"x": float, "y": float})

    assert ((coordinates["x"] > 4.0) & (coordinates["x"] < 6.0)).all()
    assert ((coordinates["y"] > 50.0) & (coordinates["y"] < 53.0)).all()


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
async def test_not_empty_dc_active_flow(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    assert test_model.model
    df = test_model.model.dc_active_flow()
    assert not df.empty


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
def test_transformer_windings(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    assert test_model.model
    df = test_model.model.transformer_windings()

    assert len(df) == 15


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
def test_switches(test_model: t_common.ModelTest):
    t_common.check_model(test_model)
    assert test_model.model
    df = test_model.model.switches()

    # 26 Breakers from CGMES_v2.4.15-MicroGridTestConfiguration_v2.docx (table 8)
    assert len(df) == 26
    assert (df["equipment_type"] == "Breaker").all()


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
def test_sv_power_deviation(test_model: t_common.ModelTest) -> None:
    t_common.check_model(test_model)
    assert test_model.model

    df = test_model.model.sv_power_deviation()

    client = test_model.model.clients["Sv power deviation"]

    count_query = test_model.model.template_to_query(
        Template("""
        PREFIX cim: <$cim>
        select (count(distinct ?tp_node) as ?num) where {
        ?s cim:Terminal.TopologicalNode ?tp_node
        }
        """)
    )

    # Count number of tp nodes connected to a terminal
    num = int(client.exec_query(count_query).results.bindings[0]["num"].value)
    assert num > 0
    assert len(df) == num


@pytest.mark.parametrize("test_model", t_entsoe.micro_models())
def test_base_voltage(test_model: t_common.ModelTest) -> None:
    t_common.check_model(test_model)
    assert test_model.model

    df = test_model.model.base_voltage()
    assert len(df) == 8
