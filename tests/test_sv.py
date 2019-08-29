import pytest

from cimsparql import redland

from conftest import need_cim_sv


@pytest.fixture(scope="module")
def svmodel(root_dir, sv_profile):
    return redland.Model(15, root_dir / "data" / f"{sv_profile}.xml", base_uri="http://example.org")


@need_cim_sv
def test_sv_powerflow(svmodel):
    pflow = svmodel.powerflow(limit=100)
    assert pflow.shape == (100, 2)


@need_cim_sv
def test_sv_voltage(svmodel):
    voltage = svmodel.voltage(limit=100)
    assert voltage.shape == (100, 2)


@need_cim_sv
def test_sv_tapstep(svmodel):
    tapstep = svmodel.tapstep(limit=100)
    assert tapstep.shape == (100, 1)
