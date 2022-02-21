import collections
import gzip
import re
from functools import cached_property
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union
from zipfile import ZipFile

import pandas as pd
import pendulum
from lxml.etree import _Element, _ElementTree, fromstring, parse


def attrib(node: _Element, key: str, prefix: str) -> str:
    return re.sub("^[#]?_", "", node.attrib[f"{{{prefix}}}{key}"])


class CimXmlBase:
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    @property
    def nsmap(self) -> Dict[str, str]:
        return self.root.nsmap

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
        connected = node.find("cim:Terminal.connected", nsmap)
        topological_node = node.find("cim:Terminal.TopologicalNode", nsmap)
        tp_node = None if topological_node is None else attrib(topological_node, "resource", rdf)
        return {
            "connected": True if connected is None else connected.text == "true",
            "tp_node": tp_node,
            "mrid": attrib(node, "about", rdf),
        }

    @staticmethod
    def _synchrounous_machine_adder(
        node: _Element, nsmap: Dict[str, str]
    ) -> Dict[str, Union[bool, str, float]]:
        control_enabled = node.find("cim:RegulatingCondEq.controlEnabled", nsmap).text == "true"
        mode = attrib(
            node.find("cim:SynchronousMachine.operatingMode", nsmap), "resource", nsmap["rdf"]
        )
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "control_enabled": control_enabled,
            "priority": int(node.find("cim:SynchronousMachine.referencePriority", nsmap).text),
            "mode": mode,
            "p": float(node.find("cim:RotatingMachine.p", nsmap).text),
            "q": float(node.find("cim:RotatingMachine.q", nsmap).text),
        }

    @staticmethod
    def _generating_unit_adder(
        node: _Element, nsmap: Dict[str, str]
    ) -> Dict[str, Union[str, float]]:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "pf": float(node.find("cim:GeneratingUnit.normalPF", nsmap).text),
        }

    @staticmethod
    def _regulating_control_adder(
        node: _Element, nsmap: Dict[str, str]
    ) -> Dict[str, Union[bool, str, float]]:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "discrete": node.find("cim:RegulatingControl.discrete", nsmap).text == "true",
            "enabled": node.find("cim:RegulatingControl.enabled", nsmap).text == "true",
            "deadband": float(node.find("cim:RegulatingControl.targetDeadband", nsmap).text),
            "value": float(node.find("cim:RegulatingControl.targetValue", nsmap).text),
            "multiplier": attrib(
                node.find("cim:RegulatingControl.targetValueUnitMultiplier", nsmap),
                "resource",
                nsmap["rdf"],
            ),
        }

    @staticmethod
    def _tap_changer_adder(
        node: _Element, nsmap: Dict[str, str]
    ) -> Dict[str, Union[str, bool, int]]:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "enabled": node.find("cim:TapChanger.controlEnabled", nsmap).text == "true",
            "step": int(node.find("cim:TapChanger.step", nsmap).text),
        }

    @staticmethod
    def _linear_shunt_compensator_adder(
        node: _Element, nsmap: Dict[str, str]
    ) -> Dict[str, Union[str, bool, int]]:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "enabled": node.find("cim:RegulatingCondEq.controlEnabled", nsmap).text == "true",
            "sections": int(node.find("cim:ShuntCompensator.sections", nsmap).text),
        }

    @staticmethod
    def _load_adder(node: _Element, nsmap: Dict[str, str]) -> Dict[str, Union[str, float]]:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "p": float(node.find("cim:EnergyConsumer.p", nsmap).text),
            "q": float(node.find("cim:EnergyConsumer.q", nsmap).text),
        }

    @staticmethod
    def _static_var_compensator_adder(
        node: _Element, nsmap: Dict[str, str]
    ) -> Dict[str, Union[str, bool, float]]:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "enabled": node.find("cim:RegulatingCondEq.controlEnabled", nsmap).text == "true",
            "sections": float(node.find("cim:StaticVarCompensator.q", nsmap).text),
        }

    @staticmethod
    def _control_area_adder(node: _Element, nsmap: Dict[str, str]) -> Dict[str, Union[str, float]]:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "interchange": float(node.find("cim:ControlArea.netInterchange", nsmap).text),
        }

    @staticmethod
    def _converter_adder(node: _Element, nsmap: Dict[str, str]) -> Dict[str, Union[str, float]]:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "p": float(node.find("cim:ACDCConverter.p", nsmap).text),
            "q": float(node.find("cim:ACDCConverter.q", nsmap).text),
        }

    @staticmethod
    def _terminal_adder(node: _Element, nsmap: Dict[str, str]) -> Dict[str, Union[str, bool]]:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "connected": node.find("cim:ACDCTerminal.connected", nsmap).text == "true",
        }

    @staticmethod
    def _switch_adder(node: _Element, nsmap: Dict[str, str]) -> Dict[str, Union[str, bool]]:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "open": node.find("cim:Switch.open", nsmap).text == "true",
        }

    @staticmethod
    def _power_limit_adder(node: _Element, nsmap: Dict[str, str]) -> Dict[str, Union[str, bool]]:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "limit": float(node.find("cim:ApparentPowerLimit.value", nsmap).text),
        }

    @staticmethod
    def _current_limit_adder(node: _Element, nsmap: Dict[str, str]) -> Dict[str, Union[str, bool]]:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "limit": float(node.find("cim:CurrentLimit.value", nsmap).text),
        }

    @staticmethod
    def _full_model_adder(
        node: _Element, nsmap: Dict[str, str]
    ) -> Dict[str, Union[str, pendulum.DateTime]]:
        return {
            "time": pendulum.parse(node.find("md:Model.scenarioTime", nsmap).text),
            "created": pendulum.parse(node.find("md:Model.created", nsmap).text),
            "description": node.find("md:Model.description", nsmap).text,
            "version": node.find("md:Model.version", nsmap).text,
            "profile": node.find("md:Model.profile", nsmap).text,
            "authority": node.find("md:Model.modelingAuthoritySet", nsmap).text,
        }

    def _adder(self, profile: str) -> Callable:
        try:
            return {
                "ACDCConverterDCTerminal": self._terminal_adder,
                "ApparentPowerLimit": self._power_limit_adder,
                "Breaker": self._switch_adder,
                "ConformLoad": self._load_adder,
                "ControlArea": self._control_area_adder,
                "CsConverter": self._converter_adder,
                "CurrentLimit": self._current_limit_adder,
                "DCTerminal": self._terminal_adder,
                "Disconnector": self._switch_adder,
                "FullModel": self._full_model_adder,
                "HydroGeneratingUnit": self._generating_unit_adder,
                "LinearShuntCompensator": self._linear_shunt_compensator_adder,
                "TapChangerControl": self._regulating_control_adder,
                "NonConformLoad": self._load_adder,
                "PhaseTapChangerLinear": self._tap_changer_adder,
                "RatioTapChanger": self._tap_changer_adder,
                "RegulatingControl": self._regulating_control_adder,
                "StaticVarCompensator": self._static_var_compensator_adder,
                "SvPowerFlow": self._sv_power_flow_data_adder,
                "SvTapStep": self._sv_tap_step_data_adder,
                "SvVoltage": self._sv_voltage_data_adder,
                "SynchronousMachine": self._synchrounous_machine_adder,
                "Terminal": self._terminal_data_adder,
                "ThermalGeneratingUnit": self._generating_unit_adder,
                "TopologicalNode": self._topological_node_data_adder,
                "VsConverter": self._converter_adder,
                "WindGeneratingUnit": self._generating_unit_adder,
            }[profile]
        except KeyError:
            raise NotImplementedError(f"Not implememted adder for {profile}")

    def parse(self, profile: str, ns: Optional[str] = None) -> pd.DataFrame:
        """Parse SVTP xml str

        Args:
            profile: What data to extract. Can be one of:
                     'SvVoltage'|'SvTapStep'|'TopologicalNode'|'Terminal'|'SvPowerFlow'
            for SSH file acceptable values for profile:
                     'ConformLoad'|'PhaseTapChangerLinear'|'RegulatingControl'|
                     'SynchronousMachine'|'ConformLoad'|'TapChangerControl'
        """
        if ns is None:
            ns = "cim"
        ns = "md" if profile.endswith("FullModel") else ns
        data = [self._adder(profile)(node, self.nsmap) for node in self.findall(f"{ns}:{profile}")]
        return pd.DataFrame([item for item in data if item is not None]).drop_duplicates()


class CimXml(CimXmlBase):
    def __init__(self, file_path: Path, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.file_path = file_path

    def _parse(self) -> None:
        if self.file_path.suffix == ".zip":
            with ZipFile(self.file_path) as cimzip:
                with cimzip.open(self.file_path.stem + ".xml") as fid:
                    self._root = parse(fid).getroot()
        else:
            open_function = gzip.open if self.file_path.suffix == ".gz" else open
            with open_function(self.file_path.absolute().as_posix(), "r") as fid:
                self._root = parse(fid).getroot()

    @property
    def root(self) -> _ElementTree:
        if not hasattr(self, "_root"):
            self._parse()
        return self._root

    def __getstate__(self) -> Path:  # pragma: no cover
        return self.file_path

    def __setstate__(self, file_path: Path):  # pragma: no cover
        self.file_path = file_path


class CimXmlStr(CimXmlBase):
    def __init__(self, text: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._text = text

    @property
    def root(self) -> _ElementTree:
        if not hasattr(self, "_root"):
            self._root = fromstring(self._text)
        return self._root


class SvTpCimXml:
    def __init__(self, sv_path: Path, tp_path: Path) -> None:
        self.paths = {"sv": sv_path, "tp": tp_path}
        self._parser = {profile: CimXml(path) for profile, path in self.paths.items()}

    def __str__(self) -> str:
        file_desc = ", ".join([f"{profile}: {path.stem}" for profile, path in self.paths.items()])
        return f"<SvTpCimXml object, {file_desc}>"

    @cached_property
    def voltage(self) -> pd.DataFrame:
        return self._parser["sv"].parse("SvVoltage").set_index("mrid")

    @cached_property
    def tap_steps(self) -> pd.DataFrame:
        return self._parser["sv"].parse("SvTapStep").set_index("mrid")

    @cached_property
    def bus_data(self, *args, **kwargs) -> pd.DataFrame:
        return self._parser["tp"].parse("TopologicalNode").set_index("mrid")

    @cached_property
    def terminal(self, *args, **kwargs) -> pd.DataFrame:
        return self._parser["tp"].parse("Terminal").set_index("mrid")

    @cached_property
    def powerflow(self, *args, **kwargs) -> pd.DataFrame:
        return self._parser["sv"].parse("SvPowerFlow").set_index("mrid")


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
