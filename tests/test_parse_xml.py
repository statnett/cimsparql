from pathlib import Path

import numpy as np
import pytest

from cimsparql.parse_xml import SvTpCimXml

root = Path(__file__).parent.parent


@pytest.fixture(scope="module")
def sv_tp_cim() -> SvTpCimXml:
    paths = [root / "data" / f"{profile}.xml" for profile in ["sv", "tp"]]
    return SvTpCimXml(*paths)


def test_parse_sv_tp_cim_xml_bus_data(sv_tp_cim: SvTpCimXml):
    assert sv_tp_cim.bus_data().shape == (4, 3)


def test_parse_sv_tp_cim_xml_terminal(sv_tp_cim: SvTpCimXml):
    assert sv_tp_cim.terminal().shape == (4, 2)


def test_parse_sv_tp_cim_xml_powerflow(sv_tp_cim: SvTpCimXml):
    powerflow = sv_tp_cim.powerflow()
    assert powerflow.shape == (4, 2)
    assert (powerflow.dtypes == np.float64).all()


def test_parse_sv_tp_cim_xml_voltage(sv_tp_cim: SvTpCimXml):
    voltage = sv_tp_cim.voltage
    assert voltage.shape == (4, 2)
    assert (voltage.dtypes == np.float64).all()


def test_parse_sv_tp_cim_xml_tap_step(sv_tp_cim: SvTpCimXml):
    tap_steps = sv_tp_cim.tap_steps
    assert tap_steps.shape == (4, 1)
    assert (tap_steps.dtypes == np.int).all()
