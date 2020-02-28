import re
from pathlib import Path
from typing import Callable, Dict, List, Union

import pandas as pd
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

    def parse(
        self,
        path: str,
        data_adder: Callable[[_Element, Dict[str, str]], Dict[str, str]],
        index: str = "mrid",
    ) -> pd.DataFrame:
        data = [data_adder(node, self.nsmap) for node in self.findall(f"cim:{path}")]
        return pd.DataFrame([item for item in data if item is not None]).set_index(index)


class SvTpCimXml:
    def __init__(self, sv_path: Path, tp_path: Path):
        profile_paths = zip(["sv", "tp"], [sv_path, tp_path])
        self._parser = {profile: CimXml(path) for profile, path in profile_paths if path.exists()}

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

    @property
    def voltage(self):
        return self._parser["sv"].parse("SvVoltage", self._sv_voltage_data_adder)

    @property
    def tap_steps(self):
        return self._parser["sv"].parse("SvTapStep", self._sv_tap_step_data_adder)

    def bus_data(self, *args, **kwargs) -> pd.DataFrame:
        return self._parser["tp"].parse("TopologicalNode", self._topological_node_data_adder)

    def terminal(self, *args, **kwargs) -> pd.DataFrame:
        return self._parser["tp"].parse("Terminal", self._terminal_data_adder)

    def powerflow(self, *args, **kwargs) -> pd.DataFrame:
        return self._parser["sv"].parse("SvPowerFlow", self._sv_power_flow_data_adder)
