import os

from cimsparql.graphdb import GraphDBClient
from cimsparql.url import Prefix

prefixes = [
    "rdf",
    "alg",
    "fn",
    "sesame",
    "ALG",
    "owl",
    "cim",
    "SN",
    "pti",
    "md",
    "ENTSOE",
    "entsoe2",
    "wgs",
    "gn",
    "xsd",
    "rdfs",
]


def test_set_cim_version():
    pre = Prefix()
    for nr in range(10):
        pre._prefixes = {"cim": f"CIM-schema-cim{nr}"}
        assert pre.cim_version == nr


def test_get_prefixes(graphdb_service, monkeypatch):
    def init(self, *args, **kwargs):
        self.user = os.getenv("GRAPHDB_USER")
        self.passwd = os.getenv("GRAPHDB_USER_PASSWD")
        self.prefixes = graphdb_service

    monkeypatch.setattr(GraphDBClient, "__init__", init)
    gdbc = GraphDBClient()

    for prefix in ["rdf", "cim", "SN"]:
        assert prefix in gdbc.prefixes


def test_header_str_missing_prefixes():
    pre = Prefix()
    assert pre.header_str("") == ""


def test_header_str():
    pre = Prefix()
    pre._prefixes = {"cim": "cim_url", "sn": "sn_url", "ALG": "alg_url"}
    ref_prefix = ["PREFIX cim:<cim_url#>", "PREFIX sn:<sn_url#>"]
    assert set(pre.header_str("cim:Var sn:Var").split("\n")).difference(ref_prefix) == set()


def test_prefix_ns(graphdb_service, monkeypatch):
    def init(self, *args, **kwargs):
        self.user = os.getenv("GRAPHDB_USER")
        self.passwd = os.getenv("GRAPHDB_USER_PASSWD")
        self.prefixes = graphdb_service

    monkeypatch.setattr(GraphDBClient, "__init__", init)
    gdbc = GraphDBClient()
    ns = gdbc.ns
    assert ns["rdf"] == "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    assert ns["cim"] == "http://iec.ch/TC57/2010/CIM-schema-cim15#"


def test_prefix_inverse(graphdb_service, monkeypatch):
    def init(self, *args, **kwargs):
        self.user = os.getenv("GRAPHDB_USER")
        self.passwd = os.getenv("GRAPHDB_USER_PASSWD")
        self.prefixes = graphdb_service

    monkeypatch.setattr(GraphDBClient, "__init__", init)
    gdbc = GraphDBClient()

    cim_inv = gdbc.inverse_ns
    assert isinstance(cim_inv, dict)
    assert set(cim_inv.values()).difference(prefixes) == set()
