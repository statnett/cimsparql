from conftest import need_local_graphdb_cim


@need_local_graphdb_cim
def test_sv_powerflow(gcli_cim):
    pflow = gcli_cim.powerflow(limit=100)
    assert pflow.shape == (100, 2)


@need_local_graphdb_cim
def test_sv_voltage(gcli_cim):
    voltage = gcli_cim.voltage(limit=100)
    assert voltage.shape == (100, 2)


@need_local_graphdb_cim
def test_sv_tapstep(gcli_cim):
    tapstep = gcli_cim.tapstep(limit=100)
    assert tapstep.shape == (100, 1)
