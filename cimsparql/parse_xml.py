import collections
import re
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Tuple, Union

import pandas as pd
import pendulum
from lxml.etree import _Element, _ElementTree, parse


def attrib(node: _Element, key: str, prefix: str) -> str:
    return re.sub("^[#]?_", "", node.attrib[f"{{{prefix}}}{key}"])


class CimXml:
    def __init__(self, fname: Path):
        self.fname = fname

    @property
    def nsmap(self) -> Dict[str, str]:
        return self.root.nsmap

    @property
    def root(self) -> _ElementTree:
        if not hasattr(self, "_root"):
            self._root = parse(self.fname.absolute().as_posix()).getroot()
        return self._root

    def __getstate__(self) -> Path:
        return self.fname

    def __setstate__(self, fname: Path):
        self.fname = fname

    def findall(self, path: str) -> List[_Element]:
        return self.root.findall(path, self.nsmap)

    @staticmethod
    def _sv_power_flow_data_adder(
        node: _Element, nsmap: Dict[str, str]
    ) -> Dict[str, Union[float, str]]:
        return {
            "p": float(node.find("cim:SvPowerFlow.p", nsmap).text),
            "q": float(node.find("cim:SvPowerFlow.q", nsmap).text),
            "mrid": attrib(node.find("cim:SvPowerFlow.Terminal", nsmap), "resource", nsmap["rdf"]),
        }

    @staticmethod
    def _sv_voltage_data_adder(
        node: _Element, nsmap: Dict[str, str]
    ) -> Dict[str, Union[float, str]]:
        rdf = nsmap["rdf"]
        try:
            return {
                "v": float(node.find("cim:SvVoltage.v", nsmap).text),
                "angle": float(node.find("cim:SvVoltage.angle", nsmap).text),
                "mrid": attrib(node.find("cim:SvVoltage.TopologicalNode", nsmap), "resource", rdf),
            }
        except AttributeError:
            pass

    @staticmethod
    def _sv_tap_step_data_adder(node: _Element, nsmap: Dict[str, str]):
        return {
            "position": int(node.find("cim:SvTapStep.position", nsmap).text),
            "mrid": attrib(node.find("cim:SvTapStep.TapChanger", nsmap), "resource", nsmap["rdf"]),
        }

    @staticmethod
    def _topological_node_data_adder(node: _Element, nsmap: Dict[str, str]) -> Dict[str, str]:
        rdf = nsmap["rdf"]
        return {
            "name": node.find("cim:IdentifiedObject.name", nsmap).text,
            "base_voltage": attrib(
                node.find("cim:TopologicalNode.BaseVoltage", nsmap), "resource", rdf
            ),
            "connectivity_node_container": attrib(
                node.find("cim:TopologicalNode.ConnectivityNodeContainer", nsmap), "resource", rdf
            ),
            "mrid": attrib(node, "ID", nsmap["rdf"]),
        }

    @staticmethod
    def _terminal_data_adder(node: _Element, nsmap: Dict[str, str]) -> Dict[str, Union[bool, str]]:
        rdf = nsmap["rdf"]
        return {
            "connected": node.find("cim:Terminal.connected", nsmap).text == "true",
            "tp_node": attrib(node.find("cim:Terminal.TopologicalNode", nsmap), "resource", rdf),
            "mrid": attrib(node, "about", rdf),
        }

    def _adder(self, profile: str) -> Callable:
        try:
            return {
                "SvVoltage": self._sv_voltage_data_adder,
                "SvTapStep": self._sv_tap_step_data_adder,
                "TopologicalNode": self._topological_node_data_adder,
                "Terminal": self._terminal_data_adder,
                "SvPowerFlow": self._sv_power_flow_data_adder,
            }[profile]
        except KeyError:
            raise NotImplementedError(f"Not implememted adder for {profile}")

    def parse(self, profile: str, index: str = "mrid") -> pd.DataFrame:
        data = [self._adder(profile)(node, self.nsmap) for node in self.findall(f"cim:{profile}")]
        return pd.DataFrame([item for item in data if item is not None]).set_index(index)


class SvTpCimXml:
    def __init__(self, sv_path: Path, tp_path: Path):
        self.paths = {"sv": sv_path, "tp": tp_path}
        self._parser = {profile: CimXml(path) for profile, path in self.paths.items()}

    def __str__(self):
        file_desc = ", ".join([f"{profile}: {path.stem}" for profile, path in self.paths.items()])
        return f"<SvTpCimXml object, {file_desc}>"

    @property
    def voltage(self):
        return self._parser["sv"].parse("SvVoltage")

    @property
    def tap_steps(self):
        return self._parser["sv"].parse("SvTapStep")

    def bus_data(self, *args, **kwargs) -> pd.DataFrame:
        return self._parser["tp"].parse("TopologicalNode")

    def terminal(self, *args, **kwargs) -> pd.DataFrame:
        return self._parser["tp"].parse("Terminal")

    def powerflow(self, *args, **kwargs) -> pd.DataFrame:
        return self._parser["sv"].parse("SvPowerFlow")


def parse_cim_file(file_name: str) -> Tuple[pendulum.DateTime, str]:
    """
    Parses a cim file-name to pendulum datetime and cim type.

    Args:
        file_name: cim file-name on format cim_YYYYMMDD_HHMMSS_foo_bar_<sv|tp>.xml

    Returns: date of cim file, and cim type

    """
    splitted = file_name.split("_")
    date = pendulum.parse(" ".join(splitted[1:3]), tz="Europe/Oslo")
    file_type = splitted[-1].split(".")[0]
    return date, file_type


def find_min(
    date: pendulum.DateTime, dates: List[pendulum.DateTime]
) -> Tuple[pendulum.DateTime, List[pendulum.DateTime]]:
    """Finds the closest date to a given date in a list of dates

    Assumes that the list of dates is sorted to reduce the iterations

    Args:
        date: Date that is to be found a closest match to
        dates: A list of dates

    Returns:
        A tuple of the date in dates that is closest to the original date and a list containing the
        unchecked dates

    """
    min_date = None
    dist = None
    for _i, d in enumerate(dates):
        dist_ = date.diff(d).in_seconds()
        if (dist is None) or (dist_ <= dist):
            min_date = d
            dist = dist_
        else:
            break
    return min_date, dates[_i - 1 :]


def get_files(path: Path) -> Dict[pendulum.DateTime, Dict[str, Path]]:
    """Finds all .xml files in given directory and subdirectories

    Returns a dictionary where the values are dicts of sv & tp identificators with each
    corresponding value being a full path to the file.

    Args:
        path: Path to the root directory to search in

    Returns: Dictionary with the resulting file paths for all dates
    """
    file_d = collections.defaultdict(dict)
    for file in path.glob("**/*.xml"):
        date, file_type = parse_cim_file(file.stem)
        file_d[date][f"{file_type}_path"] = path / file
    return file_d


def get_sv_tp(
    dt: pendulum.DateTime,
    root_path: Path = None,
    file_collection: Dict[pendulum.DateTime, Dict[str, Path]] = None,
) -> Tuple[Dict[str, Path], Dict[pendulum.DateTime, Dict[str, Path]]]:
    """For a given DateTime and a path to a directory of sv & tp files or a collection of parsed files
    from :func:`~cimsparql.parse_xml.get_files` finds and returns the sv/tp pair that are closest to
    the given date.

    Args:
        dt: DateTime in question
        root_path: Path to the root directory to search in
        file_collection:

    Returns: A tuple of a dict of the closest sv/tp pair and a collection of dates not yet parsed.

    """
    file_collection = get_files(root_path) if file_collection is None else file_collection
    min_f, rest_dates = find_min(dt, sorted(list(file_collection.keys())))
    sv_tp = file_collection[min_f]
    file_collection = {k: file_collection[k] for k in rest_dates}
    return sv_tp, file_collection


def get_cim_files(
    root_path: Path, date_range: Iterable[pendulum.DateTime]
) -> Dict[pendulum.DateTime, Dict[str, Path]]:
    """For a given directory and a list/range of DateTimes, finds the paths to the files that are
    closest wrt. to the dates in date_range. Assumes date_range is sorted in ascending order

    Args:
        root_path: Path to the root folder of a directory with sv & tp files
        date_range: Iterable of pendulum DateRange

    Returns: dictionary with date as key and another dictionary with path to the sv/tp files

    """
    file_collection = None
    results = {}
    for date in date_range:
        sv_tp, file_collection = get_sv_tp(date, root_path, file_collection)
        results[date] = sv_tp
    return results
