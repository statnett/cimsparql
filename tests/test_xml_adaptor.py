from pathlib import Path

import pytest

from cimsparql.adaptions import XmlModelAdaptor, is_uuid


@pytest.fixture
def xml_adaptor():
    folder = Path(__file__).parent / "data/micro"
    return XmlModelAdaptor.from_folder(folder)


def test_namespaces(xml_adaptor: XmlModelAdaptor):
    ns = xml_adaptor.namespaces()
    assert {"xsd", "cim"}.issubset(set(ns.keys()))


def test_add_mrid(xml_adaptor: XmlModelAdaptor):
    query = "select * where {?s cim:IdentifiedObject.mRID ?mrid}"
    assert len(xml_adaptor.graph.query(query, initNs=xml_adaptor.namespaces())) == 0
    xml_adaptor.add_mrid()
    assert len(xml_adaptor.graph.query(query, initNs=xml_adaptor.namespaces())) > 0


def test_tpsvssh_contexts(xml_adaptor: XmlModelAdaptor):
    assert len(xml_adaptor.tpsvssh_contexts()) == 6


@pytest.mark.parametrize(
    "value, result",
    [
        ("17086487-56ba-4979-b8de-064025a6b4da", True),
        ("1708648756ba-4979-b8de-064025a6b4da", False),
    ],
)
def test_is_uuid(value: str, result: bool):
    assert is_uuid(value) is result


def test_add_internal_eq_link(xml_adaptor: XmlModelAdaptor):
    xml_adaptor.add_internal_eq_link("http://eq.com")
    query = f"select * where {{?node <{XmlModelAdaptor.eq_predicate}> ?eq_uri}}"
    assert len(xml_adaptor.graph.query(query)) == 1


def test_add_sv_injection(xml_adaptor: XmlModelAdaptor) -> None:
    query = "select * where {?s a cim:SvInjection}"
    orig_number = len(xml_adaptor.graph.query(query))
    xml_adaptor.add_zero_sv_injection()
    assert len(xml_adaptor.graph.query(query)) > orig_number


def test_add_eic_code(xml_adaptor: XmlModelAdaptor) -> None:
    query = "select * where {?s a cim:Substation}"
    num_substations = len(xml_adaptor.graph.query(query))
    assert num_substations > 0
    xml_adaptor.add_eic_code()
    query = "select * where {?s entsoeSecretariat:IdentifiedObject.energyIdentCodeEIC ?eic}"
    assert len(xml_adaptor.graph.query(query, initNs=xml_adaptor.namespaces())) == num_substations


def test_add_network_analysis_enable(xml_adaptor: XmlModelAdaptor) -> None:
    xml_adaptor.add_network_analysis_enable()
    query = "select * {?terminal cim:Terminal.ConductingEquipment/SN:Equipment.networkAnalysisEnable True}"
    assert len(xml_adaptor.graph.query(query, initNs=xml_adaptor.namespaces())) > 0


def test_add_generating_unit(xml_adaptor: XmlModelAdaptor) -> None:
    xml_adaptor.add_generating_unit()
    query = "select * {?machine cim:SynchronousMachine.GeneratingUnit ?unit}"
    assert len(xml_adaptor.graph.query(query, initNs=xml_adaptor.namespaces())) > 0
