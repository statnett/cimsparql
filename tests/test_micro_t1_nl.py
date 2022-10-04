import os
from typing import Any, Dict, Optional, Set

import pytest

from cimsparql.model import CimModel


def skip_msg(server: str) -> str:
    return f"Require access to {server}"


def skip(micro_t1_nl: Optional[CimModel], server: str) -> bool:
    if server == "graphdb":
        return not micro_t1_nl
    return not micro_t1_nl and not os.getenv("CI")


MOD_TYPE = Dict[str, Optional[CimModel]]


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_bus_data_micro_t1_nl(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))

    data = model.bus_data()

    assert len(data) == 12
    assert data.index.name == "node"


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_cim_version_micro_t1_nl(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))
    assert model.cim_version == 16


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_repo_not_empty_micro_t1_nl(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))
    assert not model.client.empty


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_cim_converters_micro_t1_nl(micro_t1_nl_models: MOD_TYPE, server: str):
    # TODO: There are no Converters in micro_t1_nl_models
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))
    converters = model.converters()
    assert converters.empty


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_full_model_micro_t1_nl(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))

    data = model.full_model

    assert len(data) == 16
    row = data.query("model == 'urn:uuid:10c3fda3-35b7-47b0-b3c6-919c3e82e974'").iloc[0]
    assert row["profile"] == "http://entsoe.eu/CIM/SteadyStateHypothesis/1/1"
    assert row["time"] == "2017-10-02T09:30:00Z"
    assert row["version"] == "3"


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_substation_voltage_level(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))
    data = model.substation_voltage_level
    assert sorted(data["v"].tolist()) == [225.0, 380.0, 400.0]


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_branch_node_withdraw(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))
    data = model.branch_node_withdraw()
    assert data.empty


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_loads(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))
    data = model.loads()
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


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
@pytest.mark.parametrize(
    "region,expected_names",
    [
        (".*", {"BE-G1", "NL-G2", "NL-G3", "NL-G1"}),
        ("NL", {"NL-G2", "NL-G3", "NL-G1"}),
        ("BE", {"BE-G1"}),
    ],
)
def test_sync_machines(
    micro_t1_nl_models: MOD_TYPE, region: str, expected_names: Set[str], server: str
):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))

    data = model.synchronous_machines(region)

    assert expected_names == set(data["name"])


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_connections(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))
    data = model.connections()

    assert len(data) == 26
    assert data.columns.difference(["t_mrid_1", "t_mrid_2"]).empty


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_aclines(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))

    data = model.ac_lines(region=".*")
    assert len(data) == 2


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
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
def test_transformers(micro_t1_nl_models: MOD_TYPE, params: Dict[str, Any], server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))

    data = model.transformers(params["region"])
    assert len(data) == params["num"]
    end_num_count = data["endNumber"].value_counts()
    assert sorted(list(end_num_count)) == params["end_num_count"]
    assert set(data["name"].unique()) == params["expect_names"]


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
@pytest.mark.parametrize(
    "params",
    [{"region": ".*", "num": 15, "num_transf": 7}, {"region": "NL", "num": 6, "num_transf": 3}],
)
def test_transformers_connectivity(
    micro_t1_nl_models: MOD_TYPE, params: Dict[str, Any], server: str
):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))

    data = model.transformers(params["region"])
    assert len(data) == params["num"]

    # Groupby mrid of the power transformer. Count gives the number of windings
    assert len(data.groupby(["p_mrid"]).count()) == params["num_transf"]


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_two_winding_transformers(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))
    data = model.two_winding_transformers()
    assert len(data) == 6
    expect_names = {"NL-TR2_1", "NL_TR2_2", "NL_TR2_3", "BE-TR2_1", "BE-TR2_2", "BE-TR2_3"}
    assert set(data["name"]) == expect_names
    assert data["r"].dtype == float


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_three_winding_transformers(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))
    data = model.three_winding_transformers()

    assert len(data) == 3


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_disconnected(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))
    data = model.disconnected
    assert len(model.disconnected) == 2

    # For convenience just test parts of mrids exists
    expected_partial_mrids = ["9f984b04", "f04ec73"]
    assert all(data["mrid"].str.contains(x).any() for x in expected_partial_mrids)


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_powerflow(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))

    data = model.powerflow
    assert len(data) == 24


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_regions(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))

    assert set(model.regions["region"]) == {"BE", "EU", "NL"}
