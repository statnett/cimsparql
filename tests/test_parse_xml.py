import os
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pendulum
import pytest

from cimsparql import parse_xml
from cimsparql.parse_xml import CimXmlStr, SvTpCimXml

root = Path(__file__).parent.parent


@pytest.fixture(scope="module")
def sv_tp_cim() -> SvTpCimXml:
    paths = [root / "data" / f"{profile}.xml" for profile in ["sv", "tp"]]
    return SvTpCimXml(*paths)


@pytest.fixture(scope="module")
def bus_data(sv_tp_cim) -> pd.DataFrame:
    return sv_tp_cim.bus_data()


@pytest.fixture(scope="module")
def tp_cim() -> str:
    with open(root / "data" / "tp.xml", "r") as fid:
        return CimXmlStr(bytes(fid.read(), "utf-8"))


def test_tp_cim(bus_data: pd.DataFrame, tp_cim: CimXmlStr):
    pd.testing.assert_frame_equal(bus_data, tp_cim.parse("TopologicalNode").set_index("mrid"))


def test_str_rep(sv_tp_cim: SvTpCimXml):
    target = "<SvTpCimXml object, {}>".format(", ".join(["sv: sv", "tp: tp"]))
    assert str(sv_tp_cim) == target


def test_parse_sv_tp_cim_xml_bus_data(bus_data: pd.DataFrame):
    assert bus_data.shape == (4, 3)


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


def test_parse_cim_file():
    target_file_type = "sv"
    target_date = pendulum.datetime(2019, 11, 2, 0, 13, 40, tz="Europe/Oslo")

    file_name = "cim_20191102_001340_mis_bymin_rtnet_ems_sv.xml"
    date, file_type = parse_xml.parse_cim_file(Path(file_name).stem)

    assert date == target_date
    assert file_type == target_file_type


def test_find_min():
    dates = [
        pendulum.datetime(2019, 11, 1, 1),
        pendulum.datetime(2019, 11, 1, 2),
        pendulum.datetime(2019, 11, 1, 3),
        pendulum.datetime(2019, 11, 1, 4),
        pendulum.datetime(2019, 11, 1, 5),
        pendulum.datetime(2019, 11, 1, 6),
    ]
    date = pendulum.datetime(2019, 11, 1, 3, 23)

    target_dates = [
        pendulum.datetime(2019, 11, 1, 3),
        pendulum.datetime(2019, 11, 1, 4),
        pendulum.datetime(2019, 11, 1, 5),
        pendulum.datetime(2019, 11, 1, 6),
    ]
    target_date = pendulum.datetime(2019, 11, 1, 3)

    result_date, result_dates = parse_xml.find_min(date, dates)

    assert target_dates == result_dates
    assert target_date == result_date


def get_file_data(path: Path):
    files = [
        "cim_20191102_001340_mis_bymin_rtnet_ems_sv.xml",
        "cim_20191102_001340_mis_bymin_rtnet_ems_tp.xml",
        "cim_20191102_001350_mis_bymin_rtnet_ems_sv.xml",
        "cim_20191102_001350_mis_bymin_rtnet_ems_tp.xml",
    ]
    date_a = pendulum.datetime(2019, 11, 2, 0, 13, 40, tz="Europe/Oslo")
    date_b = pendulum.datetime(2019, 11, 2, 0, 13, 50, tz="Europe/Oslo")

    file_dict = {
        date_a: {"sv_path": path / files[0], "tp_path": path / files[1]},
        date_b: {"sv_path": path / files[2], "tp_path": path / files[3]},
    }

    return file_dict, files


def test_get_files():

    with tempfile.TemporaryDirectory() as tmp_dir:
        target_d, files = get_file_data(Path("tmp") / tmp_dir)
        [open(os.path.join(tmp_dir, f), "w") for f in files]
        file_d = dict(parse_xml.get_files(Path(tmp_dir)))

    assert target_d == file_d


def test_get_sv_tp():
    date_path = Path("191102")
    target = {
        "sv_path": date_path / "cim_20191102_001340_mis_bymin_rtnet_ems_sv.xml",
        "tp_path": date_path / "cim_20191102_001340_mis_bymin_rtnet_ems_tp.xml",
    }
    file_dict, _ = get_file_data(date_path)

    date = pendulum.datetime(2019, 11, 2, 0, 12, 42, tz="Europe/Oslo")
    sv_tp, f_c = parse_xml.get_sv_tp(date, Path(""), file_dict)
    assert sv_tp == target


def test_get_cim_files():

    dates = [
        pendulum.datetime(2019, 11, 2, 0, 13, 45, tz="Europe/Oslo"),
        pendulum.datetime(2019, 11, 2, 0, 13, 55, tz="Europe/Oslo"),
    ]

    with tempfile.TemporaryDirectory() as tmp_dir:
        root_path = Path("tmp") / tmp_dir
        file_path = Path("2019") / "191102"
        target_path = root_path / file_path
        target_d, files = get_file_data(file_path)
        os.makedirs(os.path.join(root_path, file_path))

        [open(target_path / f, "w") for f in files]
        result = parse_xml.get_cim_files(root_path, dates)

    target = {
        dates[0]: {
            "sv_path": target_path / "cim_20191102_001350_mis_bymin_rtnet_ems_sv.xml",
            "tp_path": target_path / "cim_20191102_001350_mis_bymin_rtnet_ems_tp.xml",
        },
        dates[1]: {
            "sv_path": target_path / "cim_20191102_001350_mis_bymin_rtnet_ems_sv.xml",
            "tp_path": target_path / "cim_20191102_001350_mis_bymin_rtnet_ems_tp.xml",
        },
    }
    assert result == target


def test_get_cim_files_no_o_disc():

    dates = [
        pendulum.datetime(2019, 11, 2, 0, 13, 44, tz="Europe/Oslo"),
        pendulum.datetime(2019, 11, 2, 0, 13, 55, tz="Europe/Oslo"),
    ]

    with tempfile.TemporaryDirectory() as tmp_dir:
        root_path = Path("tmp") / tmp_dir

        file_b_path = Path("2019") / "file_b_path"
        file_a_path = Path("file_a_path")

        target_a_path = root_path / file_a_path
        target_b_path = root_path / file_b_path

        target_d, files = get_file_data(Path(""))

        os.makedirs(os.path.join(root_path, target_a_path))
        os.makedirs(os.path.join(root_path, target_b_path))

        [open(target_a_path / f, "w") for f in files[:2]]
        [open(target_b_path / f, "w") for f in files[2:]]

        result = parse_xml.get_cim_files(root_path, dates)

    target = {
        dates[0]: {
            "sv_path": target_a_path / "cim_20191102_001340_mis_bymin_rtnet_ems_sv.xml",
            "tp_path": target_a_path / "cim_20191102_001340_mis_bymin_rtnet_ems_tp.xml",
        },
        dates[1]: {
            "sv_path": target_b_path / "cim_20191102_001350_mis_bymin_rtnet_ems_sv.xml",
            "tp_path": target_b_path / "cim_20191102_001350_mis_bymin_rtnet_ems_tp.xml",
        },
    }
    assert result == target
