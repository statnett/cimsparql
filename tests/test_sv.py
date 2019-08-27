import pytest

from cimsparql import sv_queries as queries
from cimsparql import redland

from conftest import need_cim_sv


@pytest.fixture(scope="module")
def svmodel(root_dir, sv_profile):
    return redland.Model(root_dir / "data" / f"{sv_profile}.xml", base_uri="http://example.org")


@need_cim_sv
def test_sv_powerflow(svmodel, cim15):
    pflow = queries.sv_powerflow(svmodel, str(cim15))
    assert pflow.shape == (15141, 2)


@need_cim_sv
def test_sv_voltage(svmodel, cim15):
    voltage = queries.sv_voltage(svmodel, str(cim15))
    assert voltage.shape == (15141, 2)


@need_cim_sv
def test_sv_tapstep(svmodel, cim15):
    tapstep = queries.sv_tapstep(svmodel, str(cim15))
    assert tapstep.shape == (1126, 1)
