import collections
import gzip
import re
from abc import ABC, abstractmethod
from functools import cached_property
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union
from zipfile import ZipFile

import pandas as pd
import pendulum
from defusedxml.lxml import RestrictedElement, fromstring, parse
from lxml.etree import _ElementTree

from cimsparql.cim import ID_OBJ, TN

ElementValues = Dict[str, Optional[Union[float, str, bool]]]


def attrib(node: Optional[RestrictedElement], key: str, prefix: str) -> Optional[str]:
    return None if node is None else re.sub("^[#]?_", "", node.attrib[f"{{{prefix}}}{key}"])


class CimXmlBase(ABC):
    _root = None

    @abstractmethod
    def _parse(self):
        pass

    @property
    def nsmap(self) -> Dict[str, str]:
        return self.root.nsmap

    @property
    def root(self) -> _ElementTree:
        if self._root is None:
            self._parse()
        return self._root

    def findall(self, path: str) -> List[RestrictedElement]:
        return self.root.findall(path, self.nsmap)

    @staticmethod
    def _attrib(
        node: RestrictedElement, element: str, nsmap: Dict[str, str], key: str = "resource"
    ) -> Optional[str]:
        return attrib(node.find(element, nsmap), key, nsmap["rdf"])

    @classmethod
    def _element_values(
        cls,
        node: RestrictedElement,
        nsmap: Dict[str, str],
        element: str,
        mrid: str,
        variables: Iterable[str],
    ) -> ElementValues:
        vars = {var: float(node.find(f"{element}.{var}", nsmap).text) for var in variables}
        return {"mrid": cls._attrib(node, f"{element}.{mrid}", nsmap), **vars}

    @classmethod
    def _sv_power_flow_data_adder(
        cls, node: RestrictedElement, nsmap: Dict[str, str]
    ) -> ElementValues:
        return cls._element_values(node, nsmap, "cim:SvPowerFlow", "Terminal", ["p", "q"])

    @classmethod
    def _sv_voltage_data_adder(
        cls, node: RestrictedElement, nsmap: Dict[str, str]
    ) -> ElementValues:
        return cls._element_values(node, nsmap, "cim:SvVoltage", "TopologicalNode", ["v", "angle"])

    @classmethod
    def _sv_injection_adder(cls, node: RestrictedElement, nsmap: Dict[str, str]) -> ElementValues:
        return cls._element_values(
            node, nsmap, "cim:SvInjection", "TopologicalNode", ["pInjection", "qInjection"]
        )

    @staticmethod
    def _sv_status_adder(node: RestrictedElement, nsmap: Dict[str, str]) -> ElementValues:
        return {
            "mrid": attrib(
                node.find("cim:SvStatus.ConductingEquipment", nsmap), "resource", nsmap["rdf"]
            ),
            "in_service": node.find("cim:SvStatus.inService", nsmap).text == "true",
        }

    @staticmethod
    def _sv_tap_step_data_adder(node: RestrictedElement, nsmap: Dict[str, str]) -> ElementValues:
        return {
            "mrid": attrib(node.find("cim:SvTapStep.TapChanger", nsmap), "resource", nsmap["rdf"]),
            "position": int(node.find("cim:SvTapStep.position", nsmap).text),
        }

    @classmethod
    def _topological_node_data_adder(
        cls, node: RestrictedElement, nsmap: Dict[str, str]
    ) -> ElementValues:
        return {
            "name": node.find(f"{ID_OBJ}.name", nsmap).text,
            "base_voltage": cls._attrib(node, f"{TN}.BaseVoltage", nsmap),
            "connectivity_node_container": cls._attrib(
                node, "{TN}.ConnectivityNodeContainer", nsmap
            ),
            "mrid": attrib(node, "ID", nsmap["rdf"]),
        }

    @staticmethod
    def _terminal_data_adder(node: RestrictedElement, nsmap: Dict[str, str]) -> ElementValues:
        rdf = nsmap["rdf"]
        connected = node.find("cim:Terminal.connected", nsmap)
        topological_node = node.find("cim:Terminal.TopologicalNode", nsmap)
        tp_node = None if topological_node is None else attrib(topological_node, "resource", rdf)
        return {
            "mrid": attrib(node, "about", rdf),
            "connected": True if connected is None else connected.text == "true",
            "tp_node": tp_node,
        }

    @staticmethod
    def _synchrounous_machine_adder(
        node: RestrictedElement, nsmap: Dict[str, str]
    ) -> ElementValues:
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

    @classmethod
    def _generating_unit_adder(
        cls, node: RestrictedElement, nsmap: Dict[str, str]
    ) -> ElementValues:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "pf": float(node.find("cim:GeneratingUnit.normalPF", nsmap).text),
        }

    @staticmethod
    def _regulating_control_adder(node: RestrictedElement, nsmap: Dict[str, str]) -> ElementValues:
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
    def _tap_changer_adder(node: RestrictedElement, nsmap: Dict[str, str]) -> ElementValues:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "enabled": node.find("cim:TapChanger.controlEnabled", nsmap).text == "true",
            "step": int(node.find("cim:TapChanger.step", nsmap).text),
        }

    @staticmethod
    def _linear_shunt_compensator_adder(
        node: RestrictedElement, nsmap: Dict[str, str]
    ) -> ElementValues:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "enabled": node.find("cim:RegulatingCondEq.controlEnabled", nsmap).text == "true",
            "sections": int(node.find("cim:ShuntCompensator.sections", nsmap).text),
        }

    @staticmethod
    def _load_adder(node: RestrictedElement, nsmap: Dict[str, str]) -> ElementValues:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "p": float(node.find("cim:EnergyConsumer.p", nsmap).text),
            "q": float(node.find("cim:EnergyConsumer.q", nsmap).text),
        }

    @staticmethod
    def _static_var_compensator_adder(
        node: RestrictedElement, nsmap: Dict[str, str]
    ) -> ElementValues:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "enabled": node.find("cim:RegulatingCondEq.controlEnabled", nsmap).text == "true",
            "sections": float(node.find("cim:StaticVarCompensator.q", nsmap).text),
        }

    @staticmethod
    def _control_area_adder(node: RestrictedElement, nsmap: Dict[str, str]) -> ElementValues:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "interchange": float(node.find("cim:ControlArea.netInterchange", nsmap).text),
        }

    @staticmethod
    def _converter_adder(node: RestrictedElement, nsmap: Dict[str, str]) -> ElementValues:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "p": float(node.find("cim:ACDCConverter.p", nsmap).text),
            "q": float(node.find("cim:ACDCConverter.q", nsmap).text),
        }

    @staticmethod
    def _terminal_adder(node: RestrictedElement, nsmap: Dict[str, str]) -> ElementValues:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "connected": node.find("cim:ACDCTerminal.connected", nsmap).text == "true",
        }

    @staticmethod
    def _switch_adder(node: RestrictedElement, nsmap: Dict[str, str]) -> ElementValues:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "open": node.find("cim:Switch.open", nsmap).text == "true",
        }

    @staticmethod
    def _power_limit_adder(node: RestrictedElement, nsmap: Dict[str, str]) -> ElementValues:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "limit": float(node.find("cim:ApparentPowerLimit.value", nsmap).text),
        }

    @staticmethod
    def _current_limit_adder(node: RestrictedElement, nsmap: Dict[str, str]) -> ElementValues:
        return {
            "mrid": attrib(node, "about", nsmap["rdf"]),
            "limit": float(node.find("cim:CurrentLimit.value", nsmap).text),
        }

    @staticmethod
    def _full_model_adder(node: RestrictedElement, nsmap: Dict[str, str]) -> ElementValues:
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
                "NonConformLoad": self._load_adder,
                "PhaseTapChangerLinear": self._tap_changer_adder,
                "RatioTapChanger": self._tap_changer_adder,
                "RegulatingControl": self._regulating_control_adder,
                "StaticVarCompensator": self._static_var_compensator_adder,
                "SvInjection": self._sv_injection_adder,
                "SvPowerFlow": self._sv_power_flow_data_adder,
                "SvStatus": self._sv_status_adder,
                "SvTapStep": self._sv_tap_step_data_adder,
                "SvVoltage": self._sv_voltage_data_adder,
                "SynchronousMachine": self._synchrounous_machine_adder,
                "TapChangerControl": self._regulating_control_adder,
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
    def __init__(self, file_path: Path) -> None:
        self._file_path = file_path

    def _parse(self) -> None:
        if self._file_path.suffix == ".zip":
            with ZipFile(self._file_path) as cimzip:
                with cimzip.open(self._file_path.stem + ".xml") as fid:
                    self._root = parse(fid).getroot()
        else:
            open_function = gzip.open if self._file_path.suffix == ".gz" else open
            with open_function(self._file_path.absolute().as_posix(), "r") as fid:
                self._root = parse(fid).getroot()


class CimXmlStr(CimXmlBase):
    def __init__(self, text: str) -> None:
        self._text = text

    def _parse(self):
        self._root = fromstring(self._text)


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
    def bus_data(self) -> pd.DataFrame:
        return self._parser["tp"].parse("TopologicalNode").set_index("mrid")

    @cached_property
    def terminal(self) -> pd.DataFrame:
        return self._parser["tp"].parse("Terminal").set_index("mrid")

    @cached_property
    def powerflow(self) -> pd.DataFrame:
        return self._parser["sv"].parse("SvPowerFlow").set_index("mrid")

    @cached_property
    def status(self) -> pd.DataFrame:
        return self._parser["sv"].parse("SvStatus").set_index("mrid")

    @cached_property
    def injection(self) -> pd.DataFrame:
        return self._parser["sv"].parse("SvInjection").set_index("mrid")


class CimXmlProfiles(SvTpCimXml):
    def __init__(self, sv_path: Path, tp_path: Path, ssh_path: Path) -> None:

        self.paths = {"sv": sv_path, "tp": tp_path, "ssh": ssh_path}
        self._parser = {profile: CimXml(path) for profile, path in self.paths.items()}

    @cached_property
    def model(self) -> pd.Series:
        return self._parser["ssh"].parse("FullModel").squeeze("index").rename("FullModel")

    @cached_property
    def generation(self) -> pd.DataFrame:
        return self._parser["ssh"].parse("SynchronousMachine").set_index("mrid")

    def demand(
        self,
        loads: Tuple[str, ...] = ("ConformLoad", "NonConformLoad"),
    ) -> pd.DataFrame:
        return pd.concat([self._parser["ssh"].parse(load) for load in loads]).set_index("mrid")

    @cached_property
    def tap_steps(self) -> pd.Series:
        steps = self._parser["ssh"].parse("RatioTapChanger").set_index("mrid")
        return steps.loc[steps["enabled"], "step"]

    @cached_property
    def converters(
        self, converters: Tuple[str, ...] = ("VsConverter", "CsConverter")
    ) -> pd.DataFrame:
        return pd.concat(
            [self._parser["ssh"].parse(converter) for converter in converters]
        ).set_index("mrid")


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
