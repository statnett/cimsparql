import os
from typing import Optional

import pytest

from cimsparql.enums import LoadTypes, SyncVars
from cimsparql.model import CimModel

RDF4J_ACCESS = "Require access to RDF4J"


def skip(micro_t1_nl_adapted: Optional[CimModel]):
    return not micro_t1_nl_adapted and not os.getenv("CI")


def test_bus_data_micro_t1_nl_adapted(micro_t1_nl_adapted: Optional[CimModel]):
    if skip(micro_t1_nl_adapted):
        pytest.skip(RDF4J_ACCESS)

    data = micro_t1_nl_adapted.bus_data(with_market=False)
    assert len(data) == 2
    assert data.index.name == "mrid"


def test_full_model_micro_t1_nl_adapted(micro_t1_nl_adapted: Optional[CimModel]):
    if skip(micro_t1_nl_adapted):
        pytest.skip(RDF4J_ACCESS)

    data = micro_t1_nl_adapted.full_model()
    assert len(data) >= 1
    assert len(data.columns) == 7


def test_loads(micro_t1_nl_adapted: Optional[CimModel]):
    if skip(micro_t1_nl_adapted):
        pytest.skip(RDF4J_ACCESS)
    data = micro_t1_nl_adapted.loads(
        [LoadTypes.EnergyConsumer], network_analysis=False, station_group=False, with_bidzone=False
    )
    assert len(data) == 3
    expected_names = ["NL-Load_3", "NL-Load_2", "NL-Load_1"]
    assert set(data["name"]) == set(expected_names)


@pytest.mark.parametrize("region", [None, "NL"])
def test_sync_machines(micro_t1_nl_adapted: Optional[CimModel], region: Optional[str]):
    if skip(micro_t1_nl_adapted):
        pytest.skip(RDF4J_ACCESS)

    vars = [SyncVars.sn, SyncVars.p, SyncVars.q]
    data = micro_t1_nl_adapted.synchronous_machines(
        vars, with_market=False, station_group=False, network_analysis=False, region=region
    )
    assert len(data) == 3

    names = {"NL-G2", "NL-G3", "NL-G1"}
    assert names == set(data["name"])


def test_connections(micro_t1_nl_adapted: Optional[CimModel]):
    if skip(micro_t1_nl_adapted):
        pytest.skip(RDF4J_ACCESS)
    data = micro_t1_nl_adapted.connections()

    # TODO (@davidkleiven): Check this test after cim:Terminal.sequenceNumber has been replaced
    # by cim:ACDCTerminal.sequenceNumber. The dataframe should probably not be empty.
    # This is work in progress on another branch (@leifwa)
    assert data.empty


def test_aclines(micro_t1_nl_adapted: Optional[CimModel]):
    if skip(micro_t1_nl_adapted):
        pytest.skip(RDF4J_ACCESS)

    data = micro_t1_nl_adapted.ac_lines(network_analysis=False)

    # TODO (@davidkleiven) relies on cim:Terminal.sequenceNumber <-- cim:ACDCTerminal.sequenceNumber
    assert data.empty


@pytest.mark.parametrize("region", [None, "NL"])
def test_transformers(micro_t1_nl_adapted: Optional[CimModel], region: Optional[str]):
    if skip(micro_t1_nl_adapted):
        pytest.skip(RDF4J_ACCESS)

    data = micro_t1_nl_adapted.transformers(network_analysis=False, region=region)
    assert len(data) == 6
    end_num_count = data["endNumber"].value_counts()
    assert list(end_num_count) == [3, 3]

    expect_names = {"NL-TR2_1", "NL_TR2_2", "NL_TR2_3"}
    assert set(data["name"].unique()) == expect_names


@pytest.mark.parametrize("region", [None, "NL"])
def test_transformers_connectivity(micro_t1_nl_adapted: Optional[CimModel], region: Optional[str]):
    if skip(micro_t1_nl_adapted):
        pytest.skip(RDF4J_ACCESS)
    data = micro_t1_nl_adapted.transformers(
        network_analysis=False, connectivity="connectivity", region=region
    )
    assert "connectivity" in data.columns

    # There are 3 transformers with two end-points each. All should be connected to different
    # connectivity nodes
    assert len(data["connectivity"].unique()) == 6


def test_two_winding_transformers(micro_t1_nl_adapted: Optional[CimModel]):
    if skip(micro_t1_nl_adapted):
        pytest.skip(RDF4J_ACCESS)
    data = micro_t1_nl_adapted.two_winding_transformers(network_analysis=False)
    assert len(data) == 3
    expect_names = {"NL-TR2_1", "NL_TR2_2", "NL_TR2_3"}
    assert set(data["name"]) == expect_names


def test_three_winding_transformers(micro_t1_nl_adapted: Optional[CimModel]):
    if skip(micro_t1_nl_adapted):
        pytest.skip(RDF4J_ACCESS)
    data = micro_t1_nl_adapted.three_winding_transformers(network_analysis=False)

    # No three-winding transformers in this model
    assert data.empty


def test_disconnected(micro_t1_nl_adapted: Optional[CimModel]):
    if skip(micro_t1_nl_adapted):
        pytest.skip(RDF4J_ACCESS)
    data = micro_t1_nl_adapted.disconnected()
    assert len(data) == 2

    # For convenience just test parts of mrids exists
    expected_partial_mrids = ["9f984b04", "f04ec73"]
    assert all(data["mrid"].str.contains(x).any() for x in expected_partial_mrids)


def test_ssh_sync_machines(micro_t1_nl_adapted: Optional[CimModel]):
    if skip(micro_t1_nl_adapted):
        pytest.skip(RDF4J_ACCESS)

    data = micro_t1_nl_adapted.ssh_synchronous_machines()
    assert len(data) == 3


def test_ssh_load(micro_t1_nl_adapted: Optional[CimModel]):
    if skip(micro_t1_nl_adapted):
        pytest.skip(RDF4J_ACCESS)

    data = micro_t1_nl_adapted.ssh_load()
    assert len(data) == 3


def test_ssh_generating_unit(micro_t1_nl_adapted: Optional[CimModel]):
    if skip(micro_t1_nl_adapted):
        pytest.skip(RDF4J_ACCESS)

    data = micro_t1_nl_adapted.ssh_generating_unit()
    assert len(data) == 3


def test_terminal(micro_t1_nl_adapted: Optional[CimModel]):
    if skip(micro_t1_nl_adapted):
        pytest.skip(RDF4J_ACCESS)

    data = micro_t1_nl_adapted.terminal()
    assert len(data) == 34


def test_topological_node(micro_t1_nl_adapted: Optional[CimModel]):
    if skip(micro_t1_nl_adapted):
        pytest.skip(RDF4J_ACCESS)

    data = micro_t1_nl_adapted.topological_node()
    assert len(data) == 2

    expect_names = ["NL_TR_BUS2", "N1230822413"]
    assert list(data["name"]) == expect_names


def test_powerflow(micro_t1_nl_adapted: Optional[CimModel]):
    if skip(micro_t1_nl_adapted):
        pytest.skip(RDF4J_ACCESS)

    data = micro_t1_nl_adapted.powerflow()
    assert len(data) == 12


def test_voltage(micro_t1_nl_adapted: Optional[CimModel]):
    if skip(micro_t1_nl_adapted):
        pytest.skip(RDF4J_ACCESS)

    data = micro_t1_nl_adapted.voltage()
    assert len(data) == 10


def test_tapstep(micro_t1_nl_adapted: Optional[CimModel]):
    if skip(micro_t1_nl_adapted):
        pytest.skip(RDF4J_ACCESS)

    data = micro_t1_nl_adapted.tapstep()
    assert len(data) == 3


def test_regions(micro_t1_nl_adapted: Optional[CimModel]):
    if skip(micro_t1_nl_adapted):
        pytest.skip(RDF4J_ACCESS)

    regions = micro_t1_nl_adapted.get_regions(with_sn_short_name=False)
    assert list(regions["region"]) == ["NL"]
