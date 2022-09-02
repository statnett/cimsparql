import os
from typing import Dict, Optional

import pytest

from cimsparql.model import CimModel


def skip_msg(server: str) -> str:
    return f"Require access to {server}"


def skip(micro_t1_nl_adapted: Optional[CimModel], server: str) -> bool:
    if server == "graphdb":
        return not micro_t1_nl_adapted
    return not micro_t1_nl_adapted and not os.getenv("CI")


MOD_TYPE = Dict[str, Optional[CimModel]]


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_bus_data_micro_t1_nl_adapted(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))

    data = model.bus_data()

    assert len(data) == 2
    assert data.index.name == "node"


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_cim_version_micro_t1_nl_adapted(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))
    assert model.cim_version == 16


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_repo_not_empty_micro_t1_nl_adapted(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))
    assert not model.client.empty


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_cim_converters_micro_t1_nl_adapted(micro_t1_nl_models: MOD_TYPE, server: str):
    # TODO: There are no Converters in micro_t1_nl_models
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))
    converters = model.converters()
    assert converters.empty


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_full_model_micro_t1_nl_adapted(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))

    data = model.full_model

    assert len(data) == 1
    uuid = "urn:uuid:10c3fda3-35b7-47b0-b3c6-919c3e82e974"
    assert data.loc[uuid, "profile"] == "http://entsoe.eu/CIM/SteadyStateHypothesis/1/1"
    assert data.loc[uuid, "time"] == "2017-10-02T09:30:00Z"
    assert data.loc[uuid, "version"] == "3"


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_substation_voltage_level(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))
    data = model.substation_voltage_level
    assert all(v == pytest.approx(15.75) for v in data["v"])


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
    assert len(data) == 3
    expected_names = ["NL-Load_3", "NL-Load_2", "NL-Load_1"]
    assert set(data["name"]) == set(expected_names)


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
@pytest.mark.parametrize("region", [".*", "NL"])
def test_sync_machines(micro_t1_nl_models: MOD_TYPE, region: Optional[str], server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))

    data = model.synchronous_machines(region)
    assert len(data) == 3

    names = {"NL-G2", "NL-G3", "NL-G1"}
    assert names == set(data["name"])


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_connections(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))
    data = model.connections()

    # TODO (@davidkleiven): Check this test after cim:Terminal.sequenceNumber has been replaced
    # by cim:ACDCTerminal.sequenceNumber. The dataframe should probably not be empty.
    # This is work in progress on another branch (@leifwa)
    assert len(data) == 11
    assert data.columns.difference(["t_mrid_1", "t_mrid_2"]).empty


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_aclines(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))

    data = model.ac_lines(region=".*", limit=None)

    # TODO (@davidkleiven) relies on cim:Terminal.sequenceNumber <--
    # cim:ACDCTerminal.sequenceNumber. It should not be empty, but all sequenceNumbers are 1.
    # Can not find Un.
    assert data.empty


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
@pytest.mark.parametrize("region", [None, "NL"])
def test_transformers(micro_t1_nl_models: MOD_TYPE, region: Optional[str], server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))

    data = model.transformers(region)
    assert len(data) == 6
    end_num_count = data["endNumber"].value_counts()
    assert list(end_num_count) == [3, 3]

    expect_names = {"NL-TR2_1", "NL_TR2_2", "NL_TR2_3"}
    assert set(data["name"].unique()) == expect_names


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
@pytest.mark.parametrize("region", [".*", "NL"])
def test_transformers_connectivity(
    micro_t1_nl_models: MOD_TYPE, region: Optional[str], server: str
):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))
    # There are 3 transformers with two end-points each. All should be connected to different
    data = model.transformers(region)
    assert len(data) == 6
    assert len(data.groupby(["p_mrid"]).count()) == 3


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_two_winding_transformers(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))
    data = model.two_winding_transformers()
    assert len(data) == 3
    expect_names = {"NL-TR2_1", "NL_TR2_2", "NL_TR2_3"}
    assert set(data["name"]) == expect_names
    assert data["r"].dtype == float


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_three_winding_transformers(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))
    data = model.three_winding_transformers()

    # No three-winding transformers in this model
    assert data.empty


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_disconnected(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))
    data = model.disconnected()
    assert len(data) == 2

    # For convenience just test parts of mrids exists
    expected_partial_mrids = ["9f984b04", "f04ec73"]
    assert all(data["mrid"].str.contains(x).any() for x in expected_partial_mrids)


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_powerflow(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))

    data = model.powerflow()
    assert len(data) == 12


@pytest.mark.parametrize("server", ["rdf4j", "blazegraph", "graphdb"])
def test_regions(micro_t1_nl_models: MOD_TYPE, server: str):
    model = micro_t1_nl_models[server]
    if skip(model, server):
        pytest.skip(skip_msg(server))

    assert list(model.regions["region"]) == ["NL"]
