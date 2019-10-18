from cimsparql.url import Prefix, service
from cimsparql.graphdb import GraphDBClient


prefixes = [
    "rdf",
    "alg",
    "ALG",
    "owl",
    "cim",
    "SN",
    "pti",
    "md",
    "entsoe",
    "entsoe2",
    "wgs",
    "gn",
    "xsd",
    "rdfs",
]


def test_set_cim_version():
    pre = Prefix()
    for nr in range(10):
        pre.prefix_dict = {"cim": f"CIM-schema-cim{nr}"}
        pre.set_cim_version()
        assert pre._cim_version == nr


def test_get_prefix_dict(monkeypatch):
    def init(self, *args, **kwargs):
        self._service = service()

    monkeypatch.setattr(GraphDBClient, "__init__", init)
    gdbc = GraphDBClient()
    gdbc.get_prefix_dict()

    for prefix in ["rdf", "cim", "SN"]:
        assert prefix in gdbc.prefix_dict


def test_header_str_missing_prefix_dict():
    pre = Prefix()
    assert pre.header_str() == ""


def test_header_str():
    pre = Prefix()
    pre.prefix_dict = {"cim": "cim_url", "sn": "sn_url"}
    assert pre.header_str() == "PREFIX cim:<cim_url#>\nPREFIX sn:<sn_url#>"


def test_prefix_ns(monkeypatch):
    def init(self, *args, **kwargs):
        self._service = service()

    monkeypatch.setattr(GraphDBClient, "__init__", init)
    gdbc = GraphDBClient()
    gdbc.get_prefix_dict()
    ns = gdbc.ns()
    assert ns["rdf"] == "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    assert ns["cim"] == "http://iec.ch/TC57/2010/CIM-schema-cim15#"


def test_prefix_inverse(monkeypatch):
    def init(self, *args, **kwargs):
        self._service = service()

    monkeypatch.setattr(GraphDBClient, "__init__", init)
    gdbc = GraphDBClient()
    gdbc.get_prefix_dict()
    cim_inv = gdbc.inverse()
    assert isinstance(cim_inv, dict)
    assert set(cim_inv.values()).difference(prefixes) == set()
