import pytest

from cimsparql.url import Prefix, service, prefixes


cim_keys = ["rdf", "alg", "cim", "SN", "pti", "entsoe", "entsoe2", "md"]


@pytest.fixture(scope="module")
def graphdb_prefix():
    prefix = Prefix()
    prefix.get_prefix_dict(service())
    return prefix


def test_set_cim_version():
    pre = Prefix()
    for nr in range(10):
        pre.prefix_dict = {"cim": f"CIM-schema-cim{nr}"}
        pre.set_cim_version()
        assert pre._cim_version == nr


def test_get_prefix_dict(graphdb_prefix):
    for prefix in ["rdf", "cim", "SN"]:
        assert prefix in graphdb_prefix.prefix_dict


def test_header_str_missing_prefix_dict():
    pre = Prefix()
    assert pre.header_str() == ""


def test_header_str():
    pre = Prefix()
    pre.prefix_dict = {"cim": "cim_url", "sn": "sn_url"}
    assert pre.header_str() == "PREFIX cim:<cim_url#>\nPREFIX sn:<sn_url#>"


def test_prefix_ns(graphdb_prefix):
    ns = graphdb_prefix.ns()
    assert ns["rdf"] == "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    assert ns["cim"] == "http://iec.ch/TC57/2010/CIM-schema-cim15#"


def test_prefix_inverse(graphdb_prefix):
    cim_inv = graphdb_prefix.inverse()
    assert isinstance(cim_inv, dict)
    assert set(cim_inv.values()).difference(prefixes) == set()
