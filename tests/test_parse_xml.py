import os
import tempfile
from itertools import product
from pathlib import Path
from typing import List

import pandas as pd
import pendulum
import pytest

from cimsparql import parse_xml
from cimsparql.parse_xml import CimXmlStr, SvTpCimXml

root = Path(__file__).parent.parent


def cim_file(date: pendulum.DateTime, profile: str) -> str:
    return f"cim_{date.format('YYYYMMDD_HHmmss')}_mis_bymin_rtnet_ems_{profile}.xml"


tz = "Europe/Oslo"


@pytest.fixture(scope="module")
def profiles():
    return ["sv", "tp"]


@pytest.fixture(scope="module")
def sv_tp_cim(profiles: List[str]) -> SvTpCimXml:
    paths = [root / "data" / f"{profile}.xml" for profile in profiles]
    return SvTpCimXml(*paths)


@pytest.fixture(scope="module")
def bus_data(sv_tp_cim) -> pd.DataFrame:
    return sv_tp_cim.bus_data


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
    assert sv_tp_cim.terminal.shape == (4, 2)


def test_parse_sv_tp_cim_xml_powerflow(sv_tp_cim: SvTpCimXml):
    assert sv_tp_cim.powerflow.shape == (4, 2)
    assert (sv_tp_cim.powerflow.dtypes == float).all()


def test_parse_sv_tp_cim_xml_voltage(sv_tp_cim: SvTpCimXml):
    assert sv_tp_cim.voltage.shape == (4, 2)
    assert (sv_tp_cim.voltage.dtypes == float).all()


def test_parse_sv_tp_cim_xml_tap_step(sv_tp_cim: SvTpCimXml):
    assert sv_tp_cim.tap_steps.shape == (4, 1)
    assert (sv_tp_cim.tap_steps.dtypes == int).all()


def test_parse_cim_file():
    target_file_type = "sv"
    target_date = pendulum.datetime(2019, 11, 2, 0, 13, 40, tz=tz)

    file_name = cim_file(target_date, "sv")
    date, file_type = parse_xml.parse_cim_file(Path(file_name).stem)

    assert date == target_date
    assert file_type == target_file_type


def test_find_min():
    dates = [pendulum.datetime(2019, 11, 1, hour) for hour in range(1, 7)]
    date = pendulum.datetime(2019, 11, 1, 3, 23)

    target_dates = [pendulum.datetime(2019, 11, 1, hour) for hour in [3, 4, 5, 6]]
    target_date = pendulum.datetime(2019, 11, 1, 3)

    result_date, result_dates = parse_xml.find_min(date, dates)

    assert target_dates == result_dates
    assert target_date == result_date


def get_file_data(path: Path, profiles: List[str]):
    dates = [pendulum.datetime(2019, 11, 2, 0, 13, seconds, tz=tz) for seconds in [40, 50]]
    files = [cim_file(date, profile) for date, profile in product(dates, profiles)]
    file_dict = {
        date: {f"{profile}_path": path / cim_file(date, profile) for profile in profiles}
        for date in dates
    }
    return file_dict, files


def test_get_files(profiles: List[str]):
    with tempfile.TemporaryDirectory() as tmp_dir:
        target_d, files = get_file_data(Path("tmp") / tmp_dir, profiles)
        [open(os.path.join(tmp_dir, file), "w") for file in files]
        file_d = parse_xml.get_files(Path(tmp_dir))

    assert target_d == file_d


def test_get_sv_tp(profiles: List[str]):
    file_date = pendulum.datetime(2019, 11, 2, 0, 13, 40, tz=tz)
    date_path = Path("191102")
    target = {f"{profile}_path": date_path / cim_file(file_date, profile) for profile in profiles}
    file_dict, _ = get_file_data(date_path, profiles)

    date = pendulum.datetime(2019, 11, 2, 0, 12, 42, tz=tz)
    sv_tp, f_c = parse_xml.get_sv_tp(date, Path(""), file_dict)
    assert sv_tp == target


def test_get_cim_files(profiles: List[str]):
    dates = [pendulum.datetime(2019, 11, 2, 0, 13, seconds, tz=tz) for seconds in [45, 55]]

    with tempfile.TemporaryDirectory() as tmp_dir:
        root_path = Path("tmp") / tmp_dir
        file_path = Path("2019") / "191102"
        target_path = root_path / file_path
        _, files = get_file_data(file_path, profiles)
        (root_path / file_path).mkdir(parents=True)
        [(target_path / file).open("w") for file in files]
        result = parse_xml.get_cim_files(root_path, dates)

    target = {
        date: {
            f"{profile}_path": target_path / cim_file(dates[-1].subtract(seconds=5), profile)
            for profile in profiles
        }
        for date in dates
    }

    assert result == target


def test_get_cim_files_no_o_disc(profiles: List[str]):
    dates = [pendulum.datetime(2019, 11, 2, 0, 13, seconds, tz=tz) for seconds in [44, 55]]
    file_dates = [date.subtract(seconds=seconds) for date, seconds in zip(dates, [4, 5])]

    with tempfile.TemporaryDirectory() as tmp_dir:
        root_path = Path("tmp") / tmp_dir

        file_paths = [Path("file_a_path"), Path("2019") / "file_b_path"]

        _, files = get_file_data(Path(""), profiles)

        for file_path, target_files in zip(file_paths, [files[:2], files[2:]]):
            (root_path / file_path).mkdir(parents=True)
            [(root_path / file_path / file).open("w") for file in target_files]

        result = parse_xml.get_cim_files(root_path, dates)

    target = {
        date: {
            f"{profile}_path": root_path / target_path / cim_file(file_date, profile)
            for profile in profiles
        }
        for date, file_date, target_path in zip(dates, file_dates, file_paths)
    }
    assert result == target
