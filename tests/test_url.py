from cimsparql.url import Prefix


def test_prefix_input_none(cim15):
    assert set(cim15.prefix_dict.keys()).difference(["rdf", "alg", "cim", "SN"]) == set()


def test_prefix_input():
    prefix_dict = {"cim": 15}
    cim15 = Prefix(15, prefix_dict)
    assert cim15.prefix_dict is prefix_dict


def test_prefix_ns(cim15):
    ns = cim15.ns()
    assert set(ns.keys()).difference(["rdf", "alg", "cim", "SN"]) == set()
    assert ns["rdf"] == "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    assert ns["cim"] == "http://iec.ch/TC57/2010/CIM-schema-cim15#"


def test_prefix_items(cim15):
    assert isinstance(cim15.items(), type(dict().items()))


def test_prefix_inverse(cim15):
    cim_inv = cim15.inverse()
    assert isinstance(cim_inv, dict)
    assert set(cim_inv.values()).difference(["rdf", "alg", "cim", "SN"]) == set()
